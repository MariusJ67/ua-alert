import streamlit as st
import pandas as pd
from datetime import datetime
from adjust_client import fetch_last_n_days, fetch_last_two_days
from data_processor import compute_day_over_day, get_alerts

st.set_page_config(
    page_title="UA-Alert",
    page_icon="🚨",
    layout="wide",
)

# ── Style ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .alert-card {
        background: #fff3cd;
        border-left: 4px solid #ff6b35;
        padding: 12px 16px;
        border-radius: 4px;
        margin-bottom: 8px;
    }
    .alert-critical { border-left-color: #dc3545; background: #ffd7d7; }
    .alert-high     { border-left-color: #fd7e14; background: #ffe8d0; }
    .alert-medium   { border-left-color: #ffc107; background: #fff3cd; }
    .metric-label { font-size: 12px; color: #666; }
    .metric-value { font-size: 24px; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────────────────────
col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title("🚨 UA-Alert")
    st.caption("Monitoring CPA jour par jour — Harmony · Stashcook · Unchaind")
with col_refresh:
    st.write("")
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()

# ── Data loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)  # cache 30 minutes
def load_data():
    df = fetch_last_two_days()
    dod = compute_day_over_day(df)
    alerts = get_alerts(dod)
    df_7d = fetch_last_n_days(7)
    return df, dod, alerts, df_7d

with st.spinner("Chargement des données Adjust..."):
    df, dod, alerts, df_7d = load_data()

if dod.empty:
    st.warning("Pas de données comparables disponibles. Adjust publie les données avec ~1 jour de délai.")
    st.stop()

# Dates comparées
date_today = dod["date"].iloc[0]
date_prev = dod["date_prev"].iloc[0]
st.caption(f"Comparaison : **{date_today}** vs **{date_prev}**  |  Dernière mise à jour : {datetime.now().strftime('%H:%M')}")

st.divider()

# ── KPI Summary ──────────────────────────────────────────────────────────────
total_alerts = len(alerts)
total_adgroups = len(dod)
avg_cpa_change = dod["cpa_change_pct"].mean() * 100

k1, k2, k3, k4 = st.columns(4)
k1.metric("Alertes actives", total_alerts, delta=None,
          delta_color="inverse" if total_alerts > 0 else "normal")
k2.metric("Ad groups suivis", total_adgroups)
k3.metric("Variation CPA moyenne", f"{avg_cpa_change:+.1f}%",
          delta_color="inverse")
k4.metric("Apps monitorées", dod["app"].nunique())

st.divider()

# ── Filters ──────────────────────────────────────────────────────────────────
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    apps = ["Toutes"] + sorted(dod["app"].unique().tolist())
    selected_app = st.selectbox("App", apps)
with col_f2:
    platforms = ["Toutes"] + sorted(dod["platform"].unique().tolist())
    selected_platform = st.selectbox("Plateforme", platforms)
with col_f3:
    threshold_pct = st.slider("Seuil d'alerte (%)", 10, 100, 25, step=5)

# Apply filters
filtered_dod = dod.copy()
filtered_alerts = alerts.copy()

if selected_app != "Toutes":
    filtered_dod = filtered_dod[filtered_dod["app"] == selected_app]
    filtered_alerts = filtered_alerts[filtered_alerts["app"] == selected_app]
if selected_platform != "Toutes":
    filtered_dod = filtered_dod[filtered_dod["platform"] == selected_platform]
    filtered_alerts = filtered_alerts[filtered_alerts["platform"] == selected_platform]

filtered_alerts = filtered_alerts[filtered_alerts["cpa_change_pct"] > threshold_pct / 100]

# ── Alerts Section ───────────────────────────────────────────────────────────
st.subheader(f"🚨 Alertes CPA (+{threshold_pct}%)")

if filtered_alerts.empty:
    st.success(f"✅ Aucun ad group au-dessus du seuil de +{threshold_pct}%")
else:
    for _, row in filtered_alerts.iterrows():
        change = row["cpa_change_pct"] * 100
        if change >= 75:
            css_class = "alert-critical"
            emoji = "🔴"
        elif change >= 50:
            css_class = "alert-high"
            emoji = "🟠"
        else:
            css_class = "alert-medium"
            emoji = "🟡"

        platform_icon = "🍎" if row["platform"] == "iOS" else "🤖"

        st.markdown(f"""
        <div class="alert-card {css_class}">
            <strong>{emoji} {row['app']} {platform_icon} {row['platform']}</strong>
            &nbsp;&nbsp;|&nbsp;&nbsp; {row['result_label']}
            &nbsp;&nbsp;|&nbsp;&nbsp; <code>{row['adgroup']}</code><br>
            CPA : <strong>${row['cpa_yesterday']:.2f}</strong> → <strong>${row['cpa_today']:.2f}</strong>
            &nbsp; <strong style="color:#dc3545">+{change:.1f}%</strong>
            &nbsp;&nbsp;|&nbsp;&nbsp; Spend : ${row['cost_today']:.0f}
        </div>
        """, unsafe_allow_html=True)

st.divider()

# ── Day-over-Day Table ───────────────────────────────────────────────────────
st.subheader("📊 Tous les ad groups — CPA J vs J-1")

display_cols = {
    "app": "App",
    "platform": "Plateforme",
    "adgroup": "Ad Group",
    "result_label": "Metric",
    "cost_today": "Spend ($)",
    "result_today": "Résultats",
    "cpa_today": "CPA aujourd'hui ($)",
    "cpa_yesterday": "CPA veille ($)",
    "cpa_change_pct": "Variation",
}

table = filtered_dod[list(display_cols.keys())].copy()
table.rename(columns=display_cols, inplace=True)
table["Variation"] = table["Variation"] * 100

def color_variation(val):
    if val >= 50:
        return "background-color: #ffd7d7; color: #dc3545; font-weight: bold"
    elif val >= 25:
        return "background-color: #ffe8d0; color: #fd7e14; font-weight: bold"
    elif val >= 0:
        return "background-color: #fff8e1"
    else:
        return "background-color: #d4edda; color: #28a745"

styled = (
    table.style
    .format({
        "Spend ($)": "${:.0f}",
        "CPA aujourd'hui ($)": "${:.2f}",
        "CPA veille ($)": "${:.2f}",
        "Variation": "{:+.1f}%",
        "Résultats": "{:.0f}",
    })
    .applymap(color_variation, subset=["Variation"])
)

st.dataframe(styled, use_container_width=True, height=400)

st.divider()

# ── 7-day Trend ──────────────────────────────────────────────────────────────
st.subheader("📈 Tendance 7 jours — CPA par app")

if not df_7d.empty:
    from data_processor import compute_cpa
    df_trend = compute_cpa(df_7d)

    # Aggregate CPA by app + day
    trend = (
        df_trend[df_7d["cost"] > 0]
        .groupby(["app", "day"])
        .apply(lambda g: g["cost"].sum() / g["result"].sum() if g["result"].sum() > 0 else None)
        .reset_index(name="cpa")
        .dropna()
    )

    if not trend.empty:
        import plotly.express as px
        fig = px.line(
            trend,
            x="day",
            y="cpa",
            color="app",
            markers=True,
            labels={"day": "Date", "cpa": "CPA ($)", "app": "App"},
            color_discrete_map={"Harmony": "#6c63ff", "Stashcook": "#ff6b6b", "Unchaind": "#4ecdc4"},
        )
        fig.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            yaxis_title="CPA ($)",
            xaxis_title="",
            legend_title="",
            height=320,
        )
        st.plotly_chart(fig, use_container_width=True)
