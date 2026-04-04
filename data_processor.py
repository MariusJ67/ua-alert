import pandas as pd
from datetime import date, timedelta
from config import CPA_ALERT_THRESHOLD, MIN_SPEND_FOR_ALERT


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
