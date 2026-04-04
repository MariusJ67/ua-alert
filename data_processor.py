import re
import pandas as pd
from datetime import date, timedelta
from config import CPA_ALERT_THRESHOLD, MIN_SPEND_FOR_ALERT


def _extract_id(name: str):
    """Extract the numeric/hash ID from a name like 'Campaign Name (12345)'."""
    match = re.search(r'\(([a-zA-Z0-9_]+)\)\s*$', name)
    return match.group(1) if match else None


def _detect_network(campaign: str) -> str:
    """Detect ad network from campaign name."""
    c = campaign.upper()
    if any(x in c for x in ["_META_", "_FCB_", "_META ", "META_"]):
        return "meta"
    if any(x in c for x in ["_GOOG_", "_GOOG ", "GOOG_"]):
        return "google"
    if any(x in c for x in ["_TIK_", "_TIKTOK_", "TIK_"]):
        return "tiktok"
    if any(x in c for x in ["_APPLO_", "APPLO_"]):
        return "applovin"
    if any(x in c for x in ["_ASA_", "_OWA_", "ASA_"]):
        return "asa"
    return "unknown"


META_ACCOUNT_IDS = {
    "harmony":   "1440546603639114",  # Lifestyle Web
    "stashcook": "1904582206865362",  # Stashcook Roca
    "unchaind":  "820328104370565",   # Unchaind Roca2
}


def build_network_url(campaign: str, adgroup: str, app: str = "") -> dict:
    """Build a direct link to the ad manager for a given campaign/adgroup."""
    network = _detect_network(campaign)
    adgroup_id = _extract_id(adgroup)
    campaign_id = _extract_id(campaign)

    if network == "meta":
        act = META_ACCOUNT_IDS.get(app.lower(), "")
        act_param = f"act={act}&" if act else ""
        if adgroup_id:
            url = f"https://adsmanager.facebook.com/adsmanager/manage/adsets?{act_param}selected_adset_ids={adgroup_id}"
        elif campaign_id:
            url = f"https://adsmanager.facebook.com/adsmanager/manage/campaigns?{act_param}selected_campaign_ids={campaign_id}"
        else:
            url = f"https://adsmanager.facebook.com/adsmanager/manage/campaigns?{act_param}"
        label = "Meta Ads Manager"
        icon = "🔵"

    elif network == "google":
        if adgroup_id:
            url = f"https://ads.google.com/aw/adgroups?adgroupId={adgroup_id}"
        elif campaign_id:
            url = f"https://ads.google.com/aw/campaigns?campaignId={campaign_id}"
        else:
            url = "https://ads.google.com/"
        label = "Google Ads"
        icon = "🔴"

    elif network == "tiktok":
        if adgroup_id:
            url = f"https://ads.tiktok.com/i18n/perf/adgroup?adgroup_id={adgroup_id}"
        elif campaign_id:
            url = f"https://ads.tiktok.com/i18n/perf/campaign?campaignId={campaign_id}"
        else:
            url = "https://ads.tiktok.com/"
        label = "TikTok Ads"
        icon = "⚫"

    elif network == "applovin":
        url = "https://dash.applovin.com/o/mediation/ad_units/"
        label = "AppLovin"
        icon = "🟠"

    elif network == "asa":
        url = "https://app.searchads.apple.com/"
        label = "Apple Search Ads"
        icon = "⚪"

    else:
        url = None
        label = None
        icon = None

    return {"url": url, "label": label, "icon": icon, "network": network}


def compute_cpa(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'cpa' column = cost / result per row."""
    df = df.copy()
    df["cpa"] = df.apply(
        lambda row: row["cost"] / row["result"] if row["result"] > 0 else None,
        axis=1,
    )
    return df


def filter_active_adgroups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only adgroups that spent more than MIN_SPEND_FOR_ALERT on the most recent day WITH spend.
    Uses the most recent day that has actual spend data (accounts for Adjust's ~1 day delay).
    """
    days_with_spend = (
        df[df["cost"] >= 1].groupby("day")["cost"].sum().sort_index(ascending=False)
    )
    if days_with_spend.empty:
        return df

    most_recent_spend_day = days_with_spend.index[0]
    active = df[df["day"] == most_recent_spend_day].groupby(["app", "campaign", "adgroup"])["cost"].sum()
    active = active[active >= MIN_SPEND_FOR_ALERT].reset_index()[["app", "campaign", "adgroup"]]
    return df.merge(active, on=["app", "campaign", "adgroup"], how="inner")


def compute_day_over_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare CPA for the most recent day with data vs the one before.
    Uses the 2 most recent days that have actual spend (accounts for Adjust's ~1 day delay).
    """
    df = compute_cpa(df)
    df = filter_active_adgroups(df)

    # Find the 2 most recent days that have spend data
    days_with_spend = (
        df[df["cost"] >= 1]
        .groupby("day")["cost"]
        .sum()
        .sort_index(ascending=False)
    )
    if len(days_with_spend) < 2:
        return pd.DataFrame()

    day_j_date = days_with_spend.index[0]   # most recent day with spend
    day_j1_date = days_with_spend.index[1]  # day before that

    key_cols = ["app", "platform", "campaign", "adgroup", "result_label"]

    day_j = df[df["day"] == day_j_date].copy()
    day_j1 = df[df["day"] == day_j1_date].copy()

    merged = day_j.merge(
        day_j1[key_cols + ["cpa", "cost", "result"]],
        on=key_cols,
        suffixes=("_today", "_yesterday"),
        how="inner",
    )

    merged = merged[merged["cpa_today"].notna() & merged["cpa_yesterday"].notna()]

    merged["cpa_change_pct"] = (
        (merged["cpa_today"] - merged["cpa_yesterday"]) / merged["cpa_yesterday"]
    )

    merged["date"] = day_j_date
    merged["date_prev"] = day_j1_date

    return merged.sort_values("cpa_change_pct", ascending=False).reset_index(drop=True)


def get_alerts(df_dod: pd.DataFrame) -> pd.DataFrame:
    """Filter adgroups where CPA increased by more than the alert threshold."""
    alerts = df_dod[
        (df_dod["cpa_change_pct"] > CPA_ALERT_THRESHOLD)
        & (df_dod["cost_today"] >= MIN_SPEND_FOR_ALERT)
    ].copy()

    alerts["cpa_change_pct_display"] = (alerts["cpa_change_pct"] * 100).round(1)
    return alerts


LOW_CREATIVE_THRESHOLD = 5  # alerte si <= N créas actives


def get_low_creative_alerts(df_creatives: pd.DataFrame) -> pd.DataFrame:
    """
    Retourne les adgroups actifs (spend > MIN_SPEND_FOR_ALERT) qui ont
    5 créas actives ou moins sur le dernier jour avec du spend.
    """
    if df_creatives.empty:
        return pd.DataFrame()

    # Exclure les adgroups "text" (text ads, pas de créas visuelles)
    df_creatives = df_creatives[
        ~df_creatives["adgroup"].str.lower().str.contains("text", na=False)
    ]

    # Dernier jour avec du spend
    days_with_spend = (
        df_creatives[df_creatives["cost"] >= 1]
        .groupby("day")["cost"].sum()
        .sort_index(ascending=False)
    )
    if days_with_spend.empty:
        return pd.DataFrame()

    latest_day = days_with_spend.index[0]
    df_day = df_creatives[df_creatives["day"] == latest_day].copy()

    # Adgroups actifs (spend suffisant)
    adgroup_spend = df_day.groupby(["app", "campaign", "adgroup"])["cost"].sum()
    active_adgroups = adgroup_spend[adgroup_spend >= MIN_SPEND_FOR_ALERT].index

    # Compter les créas actives (cost > 0) par adgroup
    active_creatives = (
        df_day[df_day["cost"] > 0]
        .groupby(["app", "campaign", "adgroup"])["creative"]
        .nunique()
        .reset_index(name="active_creative_count")
    )

    # Garder seulement les adgroups actifs
    active_creatives = active_creatives[
        active_creatives.set_index(["app", "campaign", "adgroup"]).index.isin(active_adgroups)
    ].copy()

    # Filtrer ceux avec <= seuil
    alerts = active_creatives[
        active_creatives["active_creative_count"] <= LOW_CREATIVE_THRESHOLD
    ].copy()

    if alerts.empty:
        return pd.DataFrame()

    # Ajouter spend et date
    alerts = alerts.merge(
        adgroup_spend.reset_index(name="cost_today"),
        on=["app", "campaign", "adgroup"],
        how="left",
    )
    alerts["date"] = latest_day

    return alerts.sort_values("active_creative_count").reset_index(drop=True)


if __name__ == "__main__":
    from adjust_client import fetch_last_two_days

    df = fetch_last_two_days()
    print(f"Total rows: {len(df)}")

    dod = compute_day_over_day(df)
    print(f"Active adgroups compared: {len(dod)}")

    alerts = get_alerts(dod)

    print(f"\n=== Day-over-Day CPA (top 10) ===")
    cols = ["app", "platform", "adgroup", "cpa_today", "cpa_yesterday", "cpa_change_pct", "result_label"]
    print(dod[cols].head(10).to_string())

    print(f"\n=== Alerts (+{int(CPA_ALERT_THRESHOLD*100)}% threshold) ===")
    if alerts.empty:
        print("No alerts today.")
    else:
        for _, row in alerts.iterrows():
            print(
                f"  [{row['app']}] [{row['platform']}] {row['adgroup'][:50]:50} "
                f"CPA {row['cpa_yesterday']:.1f} → {row['cpa_today']:.1f} "
                f"(+{row['cpa_change_pct_display']:.1f}%) | {row['result_label']}"
            )
