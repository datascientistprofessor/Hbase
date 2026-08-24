"""
analytics.py
------------
Reads the cleaned GSOD data back out of HBase and computes the analyses the
dashboard needs. Results are written as small CSVs under
/opt/spark-apps/results (mounted to ./results on the host), which the
Streamlit app also uses as an offline fallback if HBase is unreachable.

Plain-pandas version -- see etl.py for why Spark was dropped.

By default this scans a capped number of rows (DEFAULT_SCAN_LIMIT) rather
than the full table. A full-table scan of ~2.5M rows can hold a
long-lived HBase scanner open long enough to trip ZooKeeper session
timeouts under constrained memory (observed in practice on a 4-6GB
Docker Desktop budget shared across HDFS+ZooKeeper+HBase). The project's
goal is demonstrating HBase read/write capability, not full-dataset
analytics, so a bounded sample is sufficient -- pass 0 to scan
everything if you have the memory headroom for it.

Run with:
  docker exec spark-master python3 /opt/spark-apps/analytics.py
  docker exec spark-master python3 /opt/spark-apps/analytics.py 100000   # custom limit
  docker exec spark-master python3 /opt/spark-apps/analytics.py 0        # full table, no limit
"""

import os
import sys

import numpy as np
import pandas as pd

from hbase_reader import scan_gsod_table

RESULTS_DIR = "/opt/spark-apps/results"
DEFAULT_SCAN_LIMIT = 50000


def save(pdf, name):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{name}.csv")
    pdf.to_csv(path, index=False)
    print(f"  wrote {path} ({len(pdf)} rows)")


def main():
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
    else:
        limit = DEFAULT_SCAN_LIMIT
    limit = None if limit == 0 else limit

    if limit is None:
        print("Scanning gsod_weather from HBase (full table, no limit) ...")
    else:
        print(f"Scanning gsod_weather from HBase (limit={limit}) ...")
    records = scan_gsod_table("gsod_weather", limit=limit)
    print(f"Pulled {len(records)} rows from HBase.")

    if not records:
        print("No data in HBase yet -- run etl.py first.")
        return

    df = pd.DataFrame(records)

    # 1. Dataset overview -------------------------------------------------
    print("1. Dataset overview")
    n_rows = len(df)
    n_cols = len(df.columns)
    dup_rows = n_rows - len(df.drop_duplicates(subset=["row_key"]))

    missing_counts = df.isnull().sum().to_frame("missing_count")
    missing_counts["missing_pct"] = (missing_counts["missing_count"] / n_rows * 100).round(2)
    missing_counts = missing_counts.reset_index().rename(columns={"index": "column"})
    save(missing_counts, "01_dataset_overview_missing_values")

    overview = pd.DataFrame([{
        "total_rows": n_rows,
        "total_columns": n_cols,
        "duplicate_rows": dup_rows,
    }])
    save(overview, "01_dataset_overview_summary")

    # 2. Station analysis ---------------------------------------------------
    print("2. Station analysis")
    station_stats = (
        df.groupby(["station", "name"])
        .agg(
            num_readings=("station", "count"),
            avg_temp_f=("temp", "mean"),
            max_temp_f=("temp", "max"),
            min_temp_f=("temp", "min"),
            total_prcp_in=("prcp", "sum"),
        )
        .round(2)
        .reset_index()
        .sort_values("avg_temp_f", ascending=False)
    )
    save(station_stats, "02_station_analysis")

    # 3. Geographic analysis -------------------------------------------------
    print("3. Geographic analysis")
    lat_bins = [-90, -60, -30, 0, 30, 60, 90]
    lat_labels = [
        "Antarctic (<-60S)",
        "Southern Temperate (30-60S)",
        "Southern Tropics (0-30S)",
        "Northern Tropics (0-30N)",
        "Northern Temperate (30-60N)",
        "Arctic (>=60N)",
    ]
    df["lat_band"] = pd.cut(
        df["latitude"], bins=lat_bins, labels=lat_labels, right=False
    )
    geo_stats = (
        df.groupby("lat_band", observed=True)
        .agg(
            num_stations=("station", "nunique"),
            avg_temp_f=("temp", "mean"),
            avg_prcp_in=("prcp", "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("lat_band")
    )
    save(geo_stats, "03_geographic_analysis")

    # 4. Monthly analysis -----------------------------------------------------
    print("4. Monthly analysis")
    monthly = (
        df.groupby("month")
        .agg(
            avg_temp_f=("temp", "mean"),
            avg_prcp_in=("prcp", "mean"),
            avg_windspeed_kt=("wdsp", "mean"),
            num_readings=("month", "count"),
        )
        .round(2)
        .reset_index()
        .sort_values("month")
    )
    monthly["temp_change_mom"] = monthly["avg_temp_f"].diff().round(2)
    save(monthly, "04_monthly_analysis")

    # 5. Weather event analysis ------------------------------------------------
    print("5. Weather event analysis")
    event_cols = ["fog", "rain", "snow", "hail", "thunder", "tornado"]
    for c in event_cols:
        df[c] = df[c].astype(bool)
    events_long = pd.DataFrame({
        "event_type": [f"{c}_days" for c in event_cols],
        "day_count": [int(df[c].sum()) for c in event_cols],
    })
    save(events_long, "05_weather_event_analysis")

    # 6. Data quality ------------------------------------------------------
    print("6. Data quality")
    dq = missing_counts.copy()
    dq["duplicate_rows"] = dup_rows
    save(dq, "06_data_quality")

    print("Analytics complete. Results written under:", RESULTS_DIR)


if __name__ == "__main__":
    main()