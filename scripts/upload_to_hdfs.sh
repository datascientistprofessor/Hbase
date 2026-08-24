#!/bin/bash
# Uploads the raw per-station GSOD 2025 CSV files (mounted at /data/raw on the
# namenode container) into HDFS at /gsod/raw/2025/.
#
# Run from the host with:
#   docker exec -it namenode bash /scripts/upload_to_hdfs.sh

set -e

SRC_DIR="/data/raw"
DST_DIR="/gsod/raw/2025"

FILE_COUNT=$(find "$SRC_DIR" -maxdepth 1 -name "*.csv" | wc -l)
echo "Found $FILE_COUNT station CSV files in $SRC_DIR"

if [ "$FILE_COUNT" -eq 0 ]; then
  echo "No CSV files found in $SRC_DIR. Copy your GSOD station files there first"
  echo "(see data/README.md), then re-run this script."
  exit 1
fi

echo "Creating HDFS directory $DST_DIR ..."
hdfs dfs -mkdir -p "$DST_DIR"

echo "Uploading station files to HDFS (this can take a while for thousands of files) ..."
hdfs dfs -put -f "$SRC_DIR"/*.csv "$DST_DIR"/

echo "Done. Listing a sample of what landed in HDFS:"
hdfs dfs -ls "$DST_DIR" | head -n 10

COUNT=$(hdfs dfs -ls "$DST_DIR" | grep -c ".csv$" || true)
echo "Total files now in HDFS: $COUNT"
