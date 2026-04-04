import os
from dotenv import load_dotenv

load_dotenv()

ADJUST_API_TOKEN = os.getenv("ADJUST_API_TOKEN")
ADJUST_BASE_URL = "https://dash.adjust.com/control-center/reports-service/report"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Alert threshold: alerte si CPA augmente de plus de X% vs veille
CPA_ALERT_THRESHOLD = 0.25  # 25%

# Minimum spend (USD) pour qu'un adgroup soit éligible à une alerte
MIN_SPEND_FOR_ALERT = 50.0

# Configuration par app
# result_metric = metric Adjust utilisé comme dénominateur du CPA
# note: Stashcook trials non trackés dans Adjust (web), on utilise revenue_events
APP_CONFIGS = {
    "harmony": {
        "campaign_prefix": "HAR_",
        "app_token": "sa4guh9k6by8",
        "result_metric": "trials",
        "result_label": "Trials",
    },
    "stashcook": {
        "campaign_prefix": "STA_",
        "app_token": "fuwsqb7nfh8g",
        "result_metric": "revenue_events",  # Qonversion conversions (trial + subscription)
        "result_label": "Conversions",
    },
    "unchaind": {
        "campaign_prefix": "UNC_",
        "app_token": "kq4ckjjinta8",
        "result_metric": "subscriptions",
        "result_label": "Subscriptions",
    },
}
