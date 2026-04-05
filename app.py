from flask import Flask, jsonify, render_template, request
from adjust_client import fetch_last_two_days, fetch_last_n_days, fetch_all_apps, fetch_creative_breakdown, fetch_all_apps_with_creatives
from data_processor import compute_day_over_day, get_alerts, compute_cpa, build_network_url, get_low_creative_alerts, detect_country_flag, get_banger_alerts
from datetime import datetime, date, timedelta
from config import APP_CONFIGS

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/alerts")
def api_alerts():
    try:
        df = fetch_last_two_days()
        dod = compute_day_over_day(df)
        alerts = get_alerts(dod)

        if dod.empty:
            return jsonify({"alerts": [], "dod": [], "dates": {}, "error": None})

        date_today = str(dod["date"].iloc[0])
        date_prev = str(dod["date_prev"].iloc[0])

        alerts_list = alerts[[
            "app", "platform", "campaign", "adgroup",
            "result_label", "cost_today", "result_today",
            "cpa_today", "cpa_yesterday", "cpa_change_pct_display"
        ]].to_dict(orient="records")

        # Ajoute le lien ad manager et le drapeau pays pour chaque alerte CPA
        for row in alerts_list:
            row["network_link"]   = build_network_url(row["campaign"], row["adgroup"], row["app"])
            row["country_flag"]   = detect_country_flag(row["adgroup"], row["campaign"])

        dod_list = dod[[
            "app", "platform", "campaign", "adgroup",
            "result_label", "cost_today", "result_today",
            "cpa_today", "cpa_yesterday", "cpa_change_pct"
        ]].copy()
        dod_list["cpa_change_pct"] = (dod_list["cpa_change_pct"] * 100).round(1)
        dod_list = dod_list.to_dict(orient="records")

        # ── Low creative alerts ──────────────────────────────────────────
        today = date.today()
        start_crea = today - timedelta(days=3)
        df_crea = fetch_all_apps_with_creatives(str(start_crea), str(today - timedelta(days=1)))
        low_crea_alerts = get_low_creative_alerts(df_crea)

        low_crea_list = []
        if not low_crea_alerts.empty:
            for _, row in low_crea_alerts.iterrows():
                low_crea_list.append({
                    "app":                  row["app"],
                    "campaign":             row["campaign"],
                    "adgroup":              row["adgroup"],
                    "active_creative_count": int(row["active_creative_count"]),
                    "cost_today":           round(float(row["cost_today"]), 2),
                    "date":                 str(row["date"]),
                    "network_link":         build_network_url(row["campaign"], row["adgroup"], row["app"]),
                    "country_flag":         detect_country_flag(row["adgroup"], row["campaign"]),
                })

        # ── Banger alerts ────────────────────────────────────────────────
        banger_alerts = get_banger_alerts(df_crea)

        return jsonify({
            "alerts": alerts_list,
            "low_creative_alerts": low_crea_list,
            "banger_alerts": banger_alerts,
            "dod": dod_list,
            "dates": {"today": date_today, "prev": date_prev},
            "updated_at": datetime.now().strftime("%H:%M"),
            "error": None,
        })
    except Exception as e:
        return jsonify({"alerts": [], "dod": [], "dates": {}, "error": str(e)}), 500


@app.route("/api/trend")
def api_trend():
    try:
        df = fetch_last_n_days(7)
        df = compute_cpa(df)

        trend = (
            df[df["cost"] > 0]
            .groupby(["app", "day"])
            .apply(lambda g: round(g["cost"].sum() / g["result"].sum(), 2)
                   if g["result"].sum() > 0 else None)
            .reset_index(name="cpa")
            .dropna()
        )
        trend["day"] = trend["day"].astype(str)
        return jsonify(trend.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/creative_breakdown")
def api_creative_breakdown():
    """
    Retourne le CPA par créa pour hier vs avant-hier dans un adgroup donné.
    Identifie les créas dont le CPA a le plus augmenté.
    Params: app, campaign, adgroup
    """
    try:
        app_name = request.args.get("app", "").lower()
        campaign = request.args.get("campaign", "")
        adgroup  = request.args.get("adgroup", "")

        if not all([app_name, campaign, adgroup]):
            return jsonify({"error": "app, campaign, adgroup requis"}), 400

        cfg = APP_CONFIGS.get(app_name)
        if not cfg:
            return jsonify({"error": f"App inconnue: {app_name}"}), 400

        # Fetch 3 jours pour avoir hier + avant-hier avec délai Adjust
        today = date.today()
        start = today - timedelta(days=4)
        yesterday = today - timedelta(days=1)

        df = fetch_creative_breakdown(
            cfg["app_token"], cfg["result_metric"],
            campaign, adgroup,
            str(start), str(yesterday)
        )

        if df.empty:
            return jsonify({"creatives": [], "dates": {}})

        # Trouver les 2 derniers jours avec du spend
        days_with_spend = (
            df[df["cost"] >= 1].groupby("day")["cost"].sum()
            .sort_index(ascending=False)
        )
        if len(days_with_spend) < 2:
            return jsonify({"creatives": [], "dates": {}})

        day_j   = days_with_spend.index[0]
        day_j1  = days_with_spend.index[1]

        # CPA par créa pour chaque jour
        def cpa_by_creative(day):
            sub = df[df["day"] == day].groupby("creative").agg(
                cost=("cost", "sum"), result=("result", "sum")
            ).reset_index()
            sub["cpa"] = sub.apply(
                lambda r: round(r["cost"] / r["result"], 2) if r["result"] > 0 else None,
                axis=1
            )
            return sub

        j_df  = cpa_by_creative(day_j)
        j1_df = cpa_by_creative(day_j1)

        merged = j_df.merge(
            j1_df[["creative", "cpa", "cost", "result"]],
            on="creative", suffixes=("_today", "_yesterday"), how="outer"
        ).fillna({"cost_today": 0, "result_today": 0, "cost_yesterday": 0, "result_yesterday": 0})

        def pct_change(row):
            if row["cpa_yesterday"] and row["cpa_today"]:
                return round((row["cpa_today"] - row["cpa_yesterday"]) / row["cpa_yesterday"] * 100, 1)
            return None

        merged["cpa_change_pct"] = merged.apply(pct_change, axis=1)

        # Nettoyer le nom créa (enlever l'ID entre parenthèses pour l'affichage)
        import re
        def clean_name(name):
            return re.sub(r'\s*\([^)]+\)\s*$', '', str(name)).strip()

        merged["creative_name"] = merged["creative"].apply(clean_name)
        merged["creative_full"] = merged["creative"]

        # Trier par spend aujourd'hui décroissant
        merged = merged.sort_values("cost_today", ascending=False, na_position="last")

        import math

        def safe(v):
            if v is None:
                return None
            try:
                return None if math.isnan(float(v)) else v
            except (TypeError, ValueError):
                return v

        rows_out = []
        for _, row in merged.iterrows():
            rows_out.append({
                "creative_name":    row["creative_name"],
                "creative_full":    row["creative_full"],
                "cost_today":       safe(row.get("cost_today")),
                "result_today":     safe(row.get("result_today")),
                "cpa_today":        safe(row.get("cpa_today")),
                "cost_yesterday":   safe(row.get("cost_yesterday")),
                "result_yesterday": safe(row.get("result_yesterday")),
                "cpa_yesterday":    safe(row.get("cpa_yesterday")),
                "cpa_change_pct":   safe(row.get("cpa_change_pct")),
            })
        result = rows_out

        return jsonify({
            "creatives": result,
            "dates": {"today": str(day_j), "prev": str(day_j1)},
            "result_label": cfg["result_label"],
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/adgroup_trend")
def api_adgroup_trend():
    """
    Retourne le CPA jour par jour sur 7 jours pour un adgroup spécifique.
    Params: app, campaign, adgroup
    """
    try:
        app_name = request.args.get("app", "").lower()
        campaign = request.args.get("campaign", "")
        adgroup  = request.args.get("adgroup", "")

        if not all([app_name, campaign, adgroup]):
            return jsonify({"error": "app, campaign, adgroup requis"}), 400

        cfg = APP_CONFIGS.get(app_name)
        if not cfg:
            return jsonify({"error": f"App inconnue: {app_name}"}), 400

        # Fetch 8 jours pour avoir J-1 comme dernier jour (délai Adjust)
        today = date.today()
        start = today - timedelta(days=8)
        yesterday = today - timedelta(days=1)
        df = fetch_all_apps(str(start), str(yesterday))

        if df.empty:
            return jsonify([])

        # Filtrer sur l'adgroup exact
        df = compute_cpa(df)
        mask = (
            (df["app"].str.lower() == app_name) &
            (df["campaign"] == campaign) &
            (df["adgroup"] == adgroup)
        )
        adgroup_df = df[mask].copy()

        if adgroup_df.empty:
            return jsonify([])

        # Construire la série jour par jour
        adgroup_df = adgroup_df.sort_values("day")
        points = []
        for _, row in adgroup_df.iterrows():
            if row["cost"] > 0:
                points.append({
                    "day": str(row["day"]),
                    "cpa": round(float(row["cpa"]), 2) if row["cpa"] is not None else None,
                    "cost": round(float(row["cost"]), 2),
                    "result": int(row["result"]),
                })

        return jsonify(points)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
