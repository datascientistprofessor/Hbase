#!/bin/bash
# Copies a small sample of already-uploaded station files (server-side, no
# re-upload needed) into /gsod/raw/2025_sample/, so the ETL job can be
# tested quickly instead of against the full dataset.
#
# Run with:
#   docker exec -it namenode bash /scripts/make_sample.sh

set -e

SRC_DIR="/gsod/raw/2025"
DST_DIR="/gsod/raw/2025_sample"
N=50

echo "Creating $DST_DIR ..."
hdfs dfs -mkdir -p "$DST_DIR"

echo "Listing first $N files from $SRC_DIR ..."
hdfs dfs -ls "$SRC_DIR" | grep '\.csv$' | head -n "$N" | awk '{print $8}' > /tmp/sample_files.txt

echo "Copying $(wc -l < /tmp/sample_files.txt) files into $DST_DIR ..."
while read -r f; do
  hdfs dfs -cp "$f" "$DST_DIR/"
done < /tmp/sample_files.txt

echo "Done. Contents of $DST_DIR:"
hdfs dfs -count "$DST_DIR"