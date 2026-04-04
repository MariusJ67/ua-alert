"""
UA-Alert — Main entry point.
Run once manually or via scheduler.
"""
import schedule
import time
from adjust_client import fetch_last_two_days
from data_processor import compute_day_over_day, get_alerts
from slack_notifier import send_alert, send_daily_digest


def run_alert_check():
    print("--- Running CPA alert check ---")
    try:
        df = fetch_last_two_days()
        dod = compute_day_over_day(df)
        alerts = get_alerts(dod)

        if not alerts.empty:
            print(f"⚠️  {len(alerts)} alert(s) found.")
            send_alert(alerts)
        else:
            print("✓ No CPA spikes detected.")

        send_daily_digest(dod)

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Run immediately once (useful for testing)
        run_alert_check()
    else:
        # Schedule: runs every day at 9:00 AM
        print("UA-Alert scheduler started. Checks run daily at 09:00.")
        schedule.every().day.at("09:00").do(run_alert_check)

        # Run once at startup too
        run_alert_check()

        while True:
            schedule.run_pending()
            time.sleep(60)
