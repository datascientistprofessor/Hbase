"""
hbase_writer.py
---------------
Writes a cleaned GSOD Spark DataFrame into HBase over the Thrift gateway
(happybase), one row per STATION/DATE, using batched puts per partition.

Row key:   STATION#DATE  (e.g. "72530094846#2025-07-04")
Families:
  info    -> station, name, latitude, longitude, elevation
  weather -> temp, dewp, slp, stp, visib, wdsp, mxspd, gust, max, min,
             prcp, sndp, fog, rain, snow, hail, thunder, tornado, month, year
"""

import happybase

HBASE_THRIFT_HOST = "hbase-thrift"
HBASE_THRIFT_PORT = 9090


def _fmt(value):
    """HBase stores bytes; render None as empty string, everything else as str."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _write_partition(rows):
    connection = happybase.Connection(host=HBASE_THRIFT_HOST, port=HBASE_THRIFT_PORT)
    table = connection.table("gsod_weather")

    with table.batch(batch_size=500) as batch:
        for row in rows:
            row_key = row["ROW_KEY"]
            if row_key is None:
                continue

            batch.put(row_key.encode("utf-8"), {
                b"info:station": _fmt(row["STATION"]).encode("utf-8"),
                b"info:name": _fmt(row["NAME"]).encode("utf-8"),
                b"info:latitude": _fmt(row["LATITUDE"]).encode("utf-8"),
                b"info:longitude": _fmt(row["LONGITUDE"]).encode("utf-8"),
                b"info:elevation": _fmt(row["ELEVATION"]).encode("utf-8"),

                b"weather:date": _fmt(row["DATE"]).encode("utf-8"),
                b"weather:year": _fmt(row["YEAR"]).encode("utf-8"),
                b"weather:month": _fmt(row["MONTH"]).encode("utf-8"),
                b"weather:temp": _fmt(row["TEMP"]).encode("utf-8"),
                b"weather:dewp": _fmt(row["DEWP"]).encode("utf-8"),
                b"weather:slp": _fmt(row["SLP"]).encode("utf-8"),
                b"weather:stp": _fmt(row["STP"]).encode("utf-8"),
                b"weather:visib": _fmt(row["VISIB"]).encode("utf-8"),
                b"weather:wdsp": _fmt(row["WDSP"]).encode("utf-8"),
                b"weather:mxspd": _fmt(row["MXSPD"]).encode("utf-8"),
                b"weather:gust": _fmt(row["GUST"]).encode("utf-8"),
                b"weather:max": _fmt(row["MAX"]).encode("utf-8"),
                b"weather:min": _fmt(row["MIN"]).encode("utf-8"),
                b"weather:prcp": _fmt(row["PRCP"]).encode("utf-8"),
                b"weather:sndp": _fmt(row["SNDP"]).encode("utf-8"),
                b"weather:fog": _fmt(row["FOG"]).encode("utf-8"),
                b"weather:rain": _fmt(row["RAIN"]).encode("utf-8"),
                b"weather:snow": _fmt(row["SNOW"]).encode("utf-8"),
                b"weather:hail": _fmt(row["HAIL"]).encode("utf-8"),
                b"weather:thunder": _fmt(row["THUNDER"]).encode("utf-8"),
                b"weather:tornado": _fmt(row["TORNADO"]).encode("utf-8"),
            })

    connection.close()


def write_dataframe_to_hbase(df, table_name="gsod_weather", chunk_size=50000):
    """Writes every row of a pandas DataFrame to HBase, in chunks.

    Single-process pandas pipeline -- no Spark executors/partitions
    involved, so this just opens one Thrift connection and streams rows
    through table.batch() (already batched internally at batch_size=500
    in _write_partition). Chunking here just keeps memory bounded when
    converting to dicts, not for parallelism.
    """
    columns = [
        "ROW_KEY", "STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION",
        "DATE", "YEAR", "MONTH", "TEMP", "DEWP", "SLP", "STP", "VISIB",
        "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP",
        "FOG", "RAIN", "SNOW", "HAIL", "THUNDER", "TORNADO",
    ]
    subset = df[columns]
    n = len(subset)
    for start in range(0, n, chunk_size):
        chunk = subset.iloc[start:start + chunk_size]
        _write_partition(chunk.to_dict(orient="records"))
        print(f"  wrote rows {start}-{min(start + chunk_size, n)} of {n} to HBase")