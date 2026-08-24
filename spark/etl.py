"""
etl.py
------
Reads the raw per-station GSOD 2025 CSV files (one file per weather station,
all sharing the same NOAA schema), cleans them, derives a few extra columns,
and writes the result into HBase via hbase_writer.py.

This is a plain-pandas pipeline, not Spark -- the project's focus is HBase,
and at ~2.5M rows / 1443 files, pandas handles the load comfortably on a
single process without the JVM/executor/shuffle overhead that was causing
Spark's OOM kills under Docker Desktop's memory limit.

Run with:
  docker exec spark-master python3 /opt/spark-apps/etl.py

Optional CLI arg: path glob for the raw CSVs, e.g.:
  docker exec spark-master python3 /opt/spark-apps/etl.py "/data/gsod/raw/2025/*.csv"
"""

import sys
import glob

import numpy as np
import pandas as pd

from hbase_writer import write_dataframe_to_hbase

DEFAULT_RAW_PATH_GLOB = "/data/raw/*.csv"

# NOAA GSOD "missing value" sentinels per column family
MISSING_9999_9 = 9999.9   # TEMP, DEWP, SLP, STP, MAX, MIN
MISSING_999_9 = 999.9     # VISIB, WDSP, MXSPD, GUST, SNDP
MISSING_99_99 = 99.99     # PRCP

RAW_DTYPES = {
    "STATION": "string",
    "DATE": "string",
    "LATITUDE": "float64",
    "LONGITUDE": "float64",
    "ELEVATION": "float64",
    "NAME": "string",
    "TEMP": "float64",
    "TEMP_ATTRIBUTES": "string",
    "DEWP": "float64",
    "DEWP_ATTRIBUTES": "string",
    "SLP": "float64",
    "SLP_ATTRIBUTES": "string",
    "STP": "float64",
    "STP_ATTRIBUTES": "string",
    "VISIB": "float64",
    "VISIB_ATTRIBUTES": "string",
    "WDSP": "float64",
    "WDSP_ATTRIBUTES": "string",
    "MXSPD": "float64",
    "GUST": "float64",
    "MAX": "float64",
    "MAX_ATTRIBUTES": "string",
    "MIN": "float64",
    "MIN_ATTRIBUTES": "string",
    "PRCP": "float64",
    "PRCP_ATTRIBUTES": "string",
    "SNDP": "float64",
    "FRSHTT": "string",
}

USE_COLS = list(RAW_DTYPES.keys())


def null_out_sentinels(df):
    """Replace NOAA's missing-value sentinels with real NaNs."""
    for col in ["TEMP", "DEWP", "SLP", "STP", "MAX", "MIN"]:
        df.loc[np.isclose(df[col], MISSING_9999_9, atol=0.01), col] = np.nan
    for col in ["VISIB", "WDSP", "MXSPD", "GUST", "SNDP"]:
        df.loc[np.isclose(df[col], MISSING_999_9, atol=0.01), col] = np.nan
    df.loc[np.isclose(df["PRCP"], MISSING_99_99, atol=0.01), "PRCP"] = np.nan
    return df


def derive_weather_flags(df):
    """FRSHTT is a 6-digit string: Fog, Rain, Snow, Hail, Thunder, Tornado (1/0)."""
    frshtt = df["FRSHTT"].fillna("000000").str.zfill(6)
    df["FOG"] = frshtt.str[0] == "1"
    df["RAIN"] = frshtt.str[1] == "1"
    df["SNOW"] = frshtt.str[2] == "1"
    df["HAIL"] = frshtt.str[3] == "1"
    df["THUNDER"] = frshtt.str[4] == "1"
    df["TORNADO"] = frshtt.str[5] == "1"
    return df


def load_raw_csvs(path_glob):
    files = sorted(glob.glob(path_glob))
    if not files:
        raise FileNotFoundError(f"No CSV files matched: {path_glob}")
    print(f"Found {len(files)} station CSV files.")

    frames = []
    for i, f in enumerate(files, start=1):
        frames.append(pd.read_csv(f, usecols=USE_COLS, dtype=RAW_DTYPES))
        if i % 200 == 0 or i == len(files):
            print(f"  read {i}/{len(files)} files...")

    return pd.concat(frames, ignore_index=True)


def main():
    raw_path_glob = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RAW_PATH_GLOB

    print(f"Reading raw GSOD files from {raw_path_glob} ...")
    raw = load_raw_csvs(raw_path_glob)
    total_rows = len(raw)
    print(f"Loaded {total_rows} raw rows across all station files.")

    # --- Data quality snapshot (pre-clean) -------------------------------
    dup_count = total_rows - len(raw.drop_duplicates(subset=["STATION", "DATE"]))
    print(f"Duplicate STATION/DATE rows found: {dup_count}")

    df = raw.drop_duplicates(subset=["STATION", "DATE"]).copy()
    df = df[df["STATION"].notna() & df["DATE"].notna()]

    # --- Clean ---------------------------------------------------------
    df = null_out_sentinels(df)
    df = derive_weather_flags(df)

    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y-%m-%d")
    df["YEAR"] = df["DATE"].dt.year
    df["MONTH"] = df["DATE"].dt.strftime("%Y-%m")
    df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")

    # Row key used in HBase: STATION#DATE
    df["ROW_KEY"] = df["STATION"].astype(str) + "#" + df["DATE"].astype(str)

    cleaned_count = len(df)
    print(f"Cleaned dataset row count: {cleaned_count}")

    print("Sample of cleaned data:")
    print(df[[
        "ROW_KEY", "NAME", "LATITUDE", "LONGITUDE", "TEMP", "PRCP",
        "RAIN", "SNOW", "MONTH"
    ]].head(5).to_string(index=False))

    # --- Write to HBase --------------------------------------------------
    print("Writing cleaned rows to HBase table 'gsod_weather' ...")
    write_dataframe_to_hbase(df, table_name="gsod_weather")

    print("ETL complete.")


if __name__ == "__main__":
    main()