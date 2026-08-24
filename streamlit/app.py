"""
Streamlit dashboard for the GSOD 2025 analytics pipeline.

Primary data path: live scan of the `gsod_weather` HBase table via the
Thrift gateway (happybase). If HBase is unreachable, falls back to the
pre-computed CSVs that analytics.py writes to ./results, so the dashboard
stays demoable even before the full pipeline has been run.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px

HBASE_THRIFT_HOST = os.environ.get("HBASE_THRIFT_HOST", "hbase-thrift")
HBASE_THRIFT_PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))
RESULTS_DIR = "/app/results"

st.set_page_config(page_title="GSOD 2025 Weather Analytics", layout="wide")

NUMERIC_FIELDS = {
    "latitude", "longitude", "elevation", "temp", "dewp", "slp", "stp",
    "visib", "wdsp", "mxspd", "gust", "max", "min", "prcp", "sndp", "year",
}
BOOL_FIELDS = {"fog", "rain", "snow", "hail", "thunder", "tornado"}


def _parse(field, raw):
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    if text == "":
        return None
    if field in NUMERIC_FIELDS:
        try:
            return float(text)
        except ValueError:
            return None
    if field in BOOL_FIELDS:
        return text == "1"
    return text


@st.cache_data(ttl=300, show_spinner="Scanning HBase for live GSOD data...")
def load_from_hbase(row_limit=200_000):
    import happybase
    connection = happybase.Connection(host=HBASE_THRIFT_HOST, port=HBASE_THRIFT_PORT, timeout=5000)
    connection.open()
    table = connection.table("gsod_weather")

    records = []
    for i, (row_key, data) in enumerate(table.scan()):
        if i >= row_limit:
            break
        record = {"row_key": row_key.decode("utf-8")}
        for col, value in data.items():
            col_name = col.decode("utf-8").split(":", 1)[1]
            record[col_name] = _parse(col_name, value)
        records.append(record)
    connection.close()

    if not records:
        raise ValueError("gsod_weather table is empty -- run etl.py first.")
    return pd.DataFrame(records)


@st.cache_data(ttl=300)
def load_from_local_results():
    """Fallback: read the pre-computed CSVs written by analytics.py."""
    files = {
        "overview_missing": "01_dataset_overview_missing_values.csv",
        "overview_summary": "01_dataset_overview_summary.csv",
        "stations": "02_station_analysis.csv",
        "geographic": "03_geographic_analysis.csv",
        "monthly": "04_monthly_analysis.csv",
        "events": "05_weather_event_analysis.csv",
        "quality": "06_data_quality.csv",
    }
    out = {}
    for key, fname in files.items():
        path = os.path.join(RESULTS_DIR, fname)
        if os.path.exists(path):
            out[key] = pd.read_csv(path)
    return out


def compute_all(df):
    """Mirrors spark/analytics.py, but in pandas, for the live-HBase path."""
    results = {}

    n_rows, n_cols = len(df), len(df.columns)
    dup_rows = n_rows - df.drop_duplicates(subset=["row_key"]).shape[0]
    missing = df.isna().sum().reset_index()
    missing.columns = ["column", "missing_count"]
    missing["missing_pct"] = (missing["missing_count"] / max(n_rows, 1) * 100).round(2)
    results["overview_missing"] = missing
    results["overview_summary"] = pd.DataFrame(
        [{"total_rows": n_rows, "total_columns": n_cols, "duplicate_rows": dup_rows}]
    )

    results["stations"] = (
        df.groupby(["station", "name"], dropna=False)
        .agg(
            num_readings=("row_key", "count"),
            avg_temp_f=("temp", "mean"),
            max_temp_f=("temp", "max"),
            min_temp_f=("temp", "min"),
            total_prcp_in=("prcp", "sum"),
        )
        .round(2)
        .reset_index()
        .sort_values("avg_temp_f", ascending=False)
    )

    def lat_band(lat):
        if pd.isna(lat):
            return "Unknown"
        if lat >= 60:
            return "Arctic (>=60N)"
        if lat >= 30:
            return "Northern Temperate (30-60N)"
        if lat >= 0:
            return "Northern Tropics (0-30N)"
        if lat >= -30:
            return "Southern Tropics (0-30S)"
        if lat >= -60:
            return "Southern Temperate (30-60S)"
        return "Antarctic (<-60S)"

    df = df.copy()
    df["lat_band"] = df["latitude"].apply(lat_band)
    results["geographic"] = (
        df.groupby("lat_band")
        .agg(
            num_stations=("station", "nunique"),
            avg_temp_f=("temp", "mean"),
            avg_prcp_in=("prcp", "mean"),
        )
        .round(2)
        .reset_index()
    )

    monthly = (
        df.groupby("month")
        .agg(
            avg_temp_f=("temp", "mean"),
            avg_prcp_in=("prcp", "mean"),
            avg_windspeed_kt=("wdsp", "mean"),
            num_readings=("row_key", "count"),
        )
        .round(2)
        .reset_index()
        .sort_values("month")
    )
    monthly["temp_change_mom"] = monthly["avg_temp_f"].diff().round(2)
    results["monthly"] = monthly

    events = pd.DataFrame([{
        "fog_days": int(df["fog"].sum(skipna=True)),
        "rain_days": int(df["rain"].sum(skipna=True)),
        "snow_days": int(df["snow"].sum(skipna=True)),
        "hail_days": int(df["hail"].sum(skipna=True)),
        "thunder_days": int(df["thunder"].sum(skipna=True)),
        "tornado_days": int(df["tornado"].sum(skipna=True)),
    }]).T.reset_index()
    events.columns = ["event_type", "day_count"]
    results["events"] = events

    quality = missing.copy()
    quality["duplicate_rows"] = dup_rows
    results["quality"] = quality

    return results


# ---------------------------------------------------------------------------
# Load data: try HBase first, fall back to local pre-computed CSVs
# ---------------------------------------------------------------------------
data_source = None
results = None

try:
    raw_df = load_from_hbase()
    results = compute_all(raw_df)
    data_source = "live"
except Exception as exc:
    fallback = load_from_local_results()
    if fallback:
        results = fallback
        data_source = "fallback"
    else:
        st.error(
            "Could not reach HBase and no pre-computed results were found under "
            f"./results.\n\nHBase error: {exc}\n\n"
            "Run the ETL + analytics Spark jobs first, or check that the "
            "hbase-thrift container is up."
        )
        st.stop()

st.title("🌦️ GSOD 2025 Weather Analytics")
if data_source == "live":
    st.success("Connected to HBase — showing live data.", icon="✅")
else:
    st.warning(
        "HBase is unreachable — showing the last pre-computed results from "
        "spark/analytics.py.", icon="⚠️"
    )

tab_overview, tab_stations, tab_geo, tab_monthly, tab_events, tab_quality = st.tabs(
    ["Overview", "Stations", "Geographic", "Monthly Trends", "Weather Events", "Data Quality"]
)

with tab_overview:
    st.subheader("Dataset Overview")
    summary = results.get("overview_summary")
    if summary is not None and not summary.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Rows", f"{int(summary['total_rows'][0]):,}")
        c2.metric("Total Columns", int(summary["total_columns"][0]))
        c3.metric("Duplicate Rows", int(summary["duplicate_rows"][0]))
    st.markdown("**Missing values by column**")
    st.dataframe(results.get("overview_missing"), use_container_width=True)

with tab_stations:
    st.subheader("Station Analysis")
    stations = results.get("stations")
    if stations is not None and not stations.empty:
        st.dataframe(stations, use_container_width=True, height=400)
        top_n = st.slider("Show top/bottom N stations by average temperature", 5, 30, 10)
        hottest = stations.head(top_n)
        coldest = stations.tail(top_n)
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(hottest, x="avg_temp_f", y="name", orientation="h",
                         title="Hottest Stations (avg temp, °F)")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.bar(coldest, x="avg_temp_f", y="name", orientation="h",
                         title="Coldest Stations (avg temp, °F)")
            st.plotly_chart(fig, use_container_width=True)

with tab_geo:
    st.subheader("Geographic Analysis (by latitude band)")
    geo = results.get("geographic")
    if geo is not None and not geo.empty:
        st.dataframe(geo, use_container_width=True)
        fig = px.bar(geo, x="lat_band", y="avg_temp_f", title="Average Temperature by Latitude Band")
        st.plotly_chart(fig, use_container_width=True)

with tab_monthly:
    st.subheader("Monthly Analysis")
    monthly = results.get("monthly")
    if monthly is not None and not monthly.empty:
        st.dataframe(monthly, use_container_width=True)
        fig = px.line(monthly, x="month", y="avg_temp_f", markers=True,
                      title="Average Temperature by Month (2025)")
        st.plotly_chart(fig, use_container_width=True)
        fig2 = px.bar(monthly, x="month", y="avg_prcp_in", title="Average Precipitation by Month")
        st.plotly_chart(fig2, use_container_width=True)

with tab_events:
    st.subheader("Weather Event Frequency")
    events = results.get("events")
    if events is not None and not events.empty:
        fig = px.bar(events, x="event_type", y="day_count",
                     title="Total Station-Days With Each Weather Event (FRSHTT)")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(events, use_container_width=True)

with tab_quality:
    st.subheader("Data Quality")
    quality = results.get("quality")
    if quality is not None and not quality.empty:
        st.dataframe(quality, use_container_width=True)
        fig = px.bar(quality, x="column", y="missing_pct", title="Missing Value % by Column")
        st.plotly_chart(fig, use_container_width=True)
