from flask import Flask, jsonify, render_template
from adjust_client import fetch_last_two_days, fetch_last_n_days
from data_processor import compute_day_over_day, get_alerts, compute_cpa
from datetime import datetime

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


if __name__ == "__main__":
    app.run(debug=True, port=5000)
