from flask import Flask, jsonify, render_template, request
from adjust_client import fetch_last_two_days, fetch_last_n_days, fetch_all_apps
from data_processor import compute_day_over_day, get_alerts, compute_cpa, build_network_url
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

        # Ajoute le lien vers l'ad manager pour chaque alerte
        for row in alerts_list:
            row["network_link"] = build_network_url(row["campaign"], row["adgroup"], row["app"])

        dod_list = dod[[
            "app", "platform", "campaign", "adgroup",
            "result_label", "cost_today", "result_today",
            "cpa_today", "cpa_yesterday", "cpa_change_pct"
        ]].copy()
        dod_list["cpa_change_pct"] = (dod_list["cpa_change_pct"] * 100).round(1)
        dod_list = dod_list.to_dict(orient="records")

        return jsonify({
            "alerts": alerts_list,
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
