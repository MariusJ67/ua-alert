import requests
import pandas as pd
from config import SLACK_WEBHOOK_URL, CPA_ALERT_THRESHOLD


def send_alert(alerts_df: pd.DataFrame) -> None:
    """Send a Slack message for each ad group with a CPA spike."""
    if alerts_df.empty:
        return

    platform_emoji = {"iOS": "🍎", "Android": "🤖"}

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚨 UA-Alert — CPA Spike (+{int(CPA_ALERT_THRESHOLD*100)}% vs veille)",
            },
        },
        {"type": "divider"},
    ]

    for _, row in alerts_df.iterrows():
        change = row["cpa_change_pct_display"]
        severity_emoji = "🔴" if change >= 75 else "🟠" if change >= 50 else "🟡"
        plt_emoji = platform_emoji.get(row.get("platform", ""), "📱")

        blocks.append(
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*App & Plateforme*\n{row['app']} {plt_emoji} {row.get('platform', '')}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Metric*\n{row['result_label']}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Ad Group*\n{row['adgroup']}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Campagne*\n{row['campaign']}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*CPA veille*\n${row['cpa_yesterday']:.2f}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*CPA aujourd'hui*\n{severity_emoji} ${row['cpa_today']:.2f} (+{change}%)",
                    },
                ],
            }
        )
        blocks.append({"type": "divider"})

    payload = {"blocks": blocks}

    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if response.status_code != 200:
        print(f"Slack error {response.status_code}: {response.text}")
    else:
        print(f"✓ Slack alert sent for {len(alerts_df)} ad group(s).")


def send_daily_digest(dod_df: pd.DataFrame) -> None:
    """Send a daily summary of CPA performance per app."""
    if dod_df.empty:
        return

    lines = []
    for app, group in dod_df.groupby("app"):
        avg_change = group["cpa_change_pct"].mean() * 100
        arrow = "↑" if avg_change > 0 else "↓"
        lines.append(f"• *{app}*: CPA moyen {arrow} {abs(avg_change):.1f}%")

    text = "*📊 UA Daily Digest — CPA Day-over-Day*\n" + "\n".join(lines)

    payload = {"text": text}
    requests.post(SLACK_WEBHOOK_URL, json=payload)
    print("✓ Daily digest sent.")
