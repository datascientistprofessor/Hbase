"""
hbase_reader.py
---------------
Reads all rows out of the gsod_weather HBase table over the Thrift gateway
and returns them as a list of plain dicts, for use by analytics.py and (as
a fallback path) by the Streamlit app.
"""

import happybase

HBASE_THRIFT_HOST = "hbase-thrift"
HBASE_THRIFT_PORT = 9090

NUMERIC_FIELDS = {
    "latitude", "longitude", "elevation", "temp", "dewp", "slp", "stp",
    "visib", "wdsp", "mxspd", "gust", "max", "min", "prcp", "sndp", "year",
}
BOOL_FIELDS = {"fog", "rain", "snow", "hail", "thunder", "tornado"}


def _parse_value(field, raw):
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


def scan_gsod_table(table_name="gsod_weather", limit=None, batch_size=1000):
    """Returns a list of plain dicts, one per HBase row.

    batch_size controls how many rows Thrift fetches per round-trip.
    Without it, happybase may hold a server-side scanner open too long
    for a 2.5M-row table, which can exceed HBase's default scanner lease
    timeout and drop the connection (TTransportException / broken pipe).
    Smaller, steady batches keep each round-trip fast.
    """
    connection = happybase.Connection(
        host=HBASE_THRIFT_HOST, port=HBASE_THRIFT_PORT, timeout=60000
    )
    table = connection.table(table_name)

    records = []
    for i, (row_key, data) in enumerate(table.scan(batch_size=batch_size)):
        if limit is not None and i >= limit:
            break
        record = {"row_key": row_key.decode("utf-8")}
        for col, value in data.items():
            col_name = col.decode("utf-8").split(":", 1)[1]
            record[col_name] = _parse_value(col_name, value)
        records.append(record)
        if (i + 1) % 200000 == 0:
            print(f"  scanned {i + 1} rows so far...")

    connection.close()
    return records