import requests
import pandas as pd
from datetime import date, timedelta
from config import ADJUST_API_TOKEN, ADJUST_BASE_URL, APP_CONFIGS


def _fetch_app_report(app_token: str, result_metric: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Pull campaign data for a single app from Adjust Reporting API.
    Queries without network dimension so cost + conversions are in the same row.
    """
    headers = {
        "Authorization": f"Bearer {ADJUST_API_TOKEN}",
        "Accept": "application/json",
    }
    params = {
        "date_period": f"{start_date}:{end_date}",
        "dimensions": "campaign,adgroup,day",
        "metrics": f"cost,installs,{result_metric}",
        "utc_offset": "+00:00",
        "currency": "USD",
        "app_token__in": app_token,
    }

    response = requests.get(ADJUST_BASE_URL, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Adjust API error {response.status_code}: {response.text[:300]}")

    rows = response.json().get("rows", [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df.rename(columns={result_metric: "result"}, inplace=True)
    for col in ["cost", "installs", "result"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["day"] = pd.to_datetime(df["day"]).dt.date
    df["result_metric"] = result_metric
    return df


def _detect_platform(campaign_name: str) -> str:
    """Detect iOS or Android from campaign name."""
    name_upper = campaign_name.upper()
    if "_IOS_" in name_upper:
        return "iOS"
    elif "_AND_" in name_upper:
        return "Android"
    return "Unknown"


def _detect_app(campaign_name: str) -> str:
    """Detect app name from campaign prefix."""
    name_upper = campaign_name.upper()
    for app_key, cfg in APP_CONFIGS.items():
        if name_upper.startswith(cfg["campaign_prefix"].upper()):
            return app_key.capitalize()
    return "Unknown"


def fetch_all_apps(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch data for all configured apps and combine into one DataFrame.
    Applies filters:
      - Excludes adgroups with 'test' in name (case-insensitive)
      - Adds platform (iOS/Android) and app name columns
    """
    frames = []
    for app_key, cfg in APP_CONFIGS.items():
        df = _fetch_app_report(cfg["app_token"], cfg["result_metric"], start_date, end_date)
        if not df.empty:
            df["app"] = app_key.capitalize()
            df["result_label"] = cfg["result_label"]
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Add platform detection
    combined["platform"] = combined["campaign"].apply(_detect_platform)

    # Filter out test adgroups
    combined = combined[~combined["adgroup"].str.lower().str.contains("test", na=False)]

    # Filter out adgroups from unknown app (shouldn't happen but safety net)
    combined = combined[combined["app"] != "Unknown"]

    return combined.reset_index(drop=True)


def fetch_last_two_days() -> pd.DataFrame:
    """
    Fetch 3 days of data (J-3 to J-1) to account for Adjust's ~1 day spend reporting delay.
    The data_processor will pick the 2 most recent days with actual spend.
    """
    today = date.today()
    three_days_ago = today - timedelta(days=3)
    yesterday = today - timedelta(days=1)
    return fetch_all_apps(str(three_days_ago), str(yesterday))


def fetch_last_n_days(n: int = 7) -> pd.DataFrame:
    today = date.today()
    start = today - timedelta(days=n)
    yesterday = today - timedelta(days=1)
    return fetch_all_apps(str(start), str(yesterday))


if __name__ == "__main__":
    print("Testing Adjust connection for all apps...")
    df = fetch_last_two_days()
    print(f"✓ Connected. {len(df)} rows fetched.")
    print(f"\nApps found: {df['app'].unique()}")
    print(f"Platforms:  {df['platform'].unique()}")
    print(f"\nSample:")
    print(df[df["cost"] > 50][["app", "platform", "campaign", "adgroup", "cost", "result", "result_metric"]].head(8).to_string())
