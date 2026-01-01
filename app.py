import pandas as pd
from pathlib import Path
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")

ROOT = Path(__file__).resolve().parent
marts_spark = ROOT / "data" / "processed" / "marts_spark"
marts_pandas = ROOT / "data" / "processed" / "marts"

# --- Helper: load from spark marts if present, else pandas marts ---
def load_parquet(folder: Path, fallback: Path, name: str) -> pd.DataFrame:
    p = folder / name
    if p.exists():
        return pd.read_parquet(p)
    return pd.read_parquet(fallback / f"{name}.parquet")

agg_hour = load_parquet(marts_spark, marts_pandas, "agg_hour")
agg_dow = load_parquet(marts_spark, marts_pandas, "agg_dow")
agg_zone = load_parquet(marts_spark, marts_pandas, "agg_zone")
pay_unk = load_parquet(marts_spark, marts_pandas, "pay_unknown_by_hour")

# --- Sidebar controls ---
st.sidebar.title("Controls")
metric = st.sidebar.selectbox(
    "Metric",
    ["trips", "avg_fare", "avg_distance", "avg_total", "avg_tip", "unknown_payment_rate"],
    index=0
)

top_n = st.sidebar.slider("Top zones (N)", min_value=5, max_value=50, value=15, step=5)

st.title("NYC Yellow Taxi – Dashboard (Marts-based)")

# --- Layout: 2 columns ---
col1, col2 = st.columns(2)

# --- Chart 1: Trips by hour ---
with col1:
    st.subheader("By Hour")
    if metric in ["trips", "avg_fare", "avg_distance"]:
        df = agg_hour.copy()
        fig = px.line(df, x="pickup_hour", y=metric, markers=True, title=f"{metric} by hour")
        fig.update_xaxes(dtick=1)
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Select trips / avg_fare / avg_distance for the hourly chart.")

# --- Chart 2: Trips by day of week ---
with col2:
    st.subheader("By Day of Week")
    if metric in ["trips", "avg_fare", "avg_distance"]:
        df = agg_dow.copy()
        dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        df["pickup_dow"] = pd.Categorical(df["pickup_dow"], categories=dow_order, ordered=True)
        df = df.sort_values("pickup_dow")
        fig = px.bar(df, x="pickup_dow", y=metric, title=f"{metric} by day of week")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Select trips / avg_fare / avg_distance for the DOW chart.")

st.divider()

# --- Chart 3: Top pickup zones ---
st.subheader(f"Top {top_n} Pickup Zones")
zone_df = agg_zone.head(top_n).copy()

if metric in ["trips", "avg_total", "avg_tip", "avg_distance"]:
    fig = px.bar(
        zone_df[::-1],
        x=metric,
        y="PU_Zone",
        color="PU_Borough",
        title=f"Top zones by {metric}"
    )
    st.plotly_chart(fig, width="stretch")
else:
    st.info("Select trips / avg_total / avg_tip / avg_distance for zone chart.")

# --- Chart 4: Data quality transparency ---
st.subheader("Data Quality: Unknown Payment Rate by Hour")
fig = px.line(pay_unk, x="pickup_hour", y="unknown_payment_rate", markers=True,
              title="Unknown payment type rate by hour")
fig.update_xaxes(dtick=1)
st.plotly_chart(fig, width="stretch")

# --- Optional: show raw tables ---
with st.expander("Show tables"):
    st.write("agg_hour", agg_hour.head(24))
    st.write("agg_dow", agg_dow)
    st.write("agg_zone (top rows)", agg_zone.head(25))
    st.write("pay_unknown_by_hour", pay_unk.head(24))
