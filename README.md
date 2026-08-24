# GSOD Weather Analytics — HDFS, HBase & Streamlit

A fully Dockerized Big Data pipeline that ingests NOAA's Global Surface
Summary of the Day (GSOD) — 2025 dataset, cleans and enriches it with
**pandas**, persists it in HBase, and serves interactive analytics through a
Streamlit dashboard.

> **Note on processing engine:** the ETL and analytics jobs originally ran
> on PySpark. At this project's scale (~2.5M rows across ~1,443 station
> files), Spark's JVM/executor/shuffle overhead caused repeated
> out-of-memory kills under a constrained Docker Desktop memory budget.
> Since the project's focus is demonstrating **HBase** as the storage
> layer — not distributed processing — `etl.py` and `analytics.py` were
> rewritten in plain pandas. They run single-process, read CSVs directly
> from the local `/data` mount, and write to HBase via the same
> `hbase_writer.py`/`hbase_reader.py` Thrift-based modules as before. See
> [Section 12](#12-future-improvements) for notes on reintroducing Spark.



## Dataset

GSOD 2025 ships as one CSV per weather station (thousands of files), each
with one row per day the station reported:


| Column              | Definition                                                        |
| ------------------- | ----------------------------------------------------------------- |
| STATION             | USAF+WBAN station identifier.                                     |
| DATE                | Observation date (YYYY-MM-DD).                                    |
| LATITUDE/LONGITUDE  | Station coordinates.                                              |
| ELEVATION           | Station elevation, metres.                                        |
| NAME                | Station name.                                                     |
| TEMP / DEWP         | Mean temperature / dew point, °F.                                 |
| SLP / STP           | Mean sea-level / station pressure, millibars.                     |
| VISIB               | Mean visibility, miles.                                           |
| WDSP / MXSPD / GUST | Mean / max / peak wind speed, knots.                              |
| MAX / MIN           | Max / min temperature for the day, °F.                            |
| PRCP / SNDP         | Precipitation / snow depth, inches.                               |
| FRSHTT              | 6-digit flag: Fog, Rain, Snow, Hail, Thunder, Tornado (1/0 each). |


**Missing-value note:** NOAA encodes missing readings with sentinel
values (`9999.9`, `999.9`, `99.99` depending on the column) rather than
blanks. `etl.py` converts these to real nulls during cleaning.

Source: NOAA GSOD access page — download the per-station CSVs for 2025
into `data/raw/` (see `data/README.md`).

## Project Layout

```
GSODAnalytics/
  config/            hadoop.env, hbase.env  (shared container config)
  data/raw/          put your GSOD 2025 station CSVs here
  scripts/           upload_to_hdfs.sh, create_table.hbase
  notebooks/         ad-hoc exploration notebook
  results/           CSVs written by analytics.py (also the Streamlit fallback)
  spark/             Dockerfile, etl.py, analytics.py, hbase_writer.py, hbase_reader.py
  streamlit/         Dockerfile, app.py
  docker-compose.yml
```

> The `spark/` folder name is legacy — it still holds `etl.py` and
> `analytics.py`, which now run as plain Python rather than Spark jobs.



## 4. Docker Setup



### Services


| Service            | Image                               | Purpose                                                                      |
| ------------------ | ----------------------------------- | ---------------------------------------------------------------------------- |
| namenode           | bde2020/hadoop-namenode             | HDFS NameNode                                                                |
| datanode           | bde2020/hadoop-datanode             | HDFS DataNode                                                                |
| zookeeper          | zookeeper:3.7                       | Coordination service for HBase                                               |
| hbase-master       | bde2020/hbase-master                | HBase Master                                                                 |
| hbase-regionserver | bde2020/hbase-regionserver          | HBase RegionServer                                                           |
| hbase-thrift       | bde2020/hbase-master (reused image) | Dedicated Thrift API gateway on port 9090 (used by happybase)                |
| spark-master       | custom build on apache/spark:3.5.1  | Runs `etl.py`/`analytics.py` (plain Python, not Spark jobs — see note above) |
| streamlit          | custom (streamlit/Dockerfile)       | Interactive dashboard                                                        |


All containers join a single bridge network, `gsod-net`.

### Build

```
cd GSODAnalytics
docker-compose build
docker-compose up -d
docker-compose ps
```

Web UIs once running:


| UI                  | URL                                              |
| ------------------- | ------------------------------------------------ |
| HDFS NameNode       | [http://localhost:9870](http://localhost:9870)   |
| HDFS DataNode       | [http://localhost:9864](http://localhost:9864)   |
| HBase Master        | [http://localhost:16010](http://localhost:16010) |
| HBase RegionServer  | [http://localhost:16030](http://localhost:16030) |
| Streamlit Dashboard | [http://localhost:8501](http://localhost:8501)   |




## 5. Run Instructions (End-to-End Pipeline)  


### Step 1 — Upload the raw CSVs into HDFS *(optional)*

`etl.py` now reads CSVs directly from the local `/data` mount, so this
step is **no longer required** for the pipeline to run. It's kept here
in case you want HDFS populated for its own sake (e.g. to browse via the
NameNode UI, or as groundwork for reintroducing Spark later — see
[Section 12](#12-future-improvements)).

```
docker exec -it namenode bash /scripts/upload_to_hdfs.sh
```



### Step 2 — Confirm HBase is healthy, then create the table

```
echo "status 'simple'" | docker exec -i hbase-master hbase shell -n
"list" | docker exec -i hbase-thrift hbase shell -n
docker exec -it hbase-master hbase shell /scripts/create_table.hbase
```

> The `"list" | docker exec -i ...` form works in both PowerShell and
> bash. The bash-only `<<<` here-string syntax will fail on Windows —
> use the pipe form above regardless of shell.



### Step 3 — Run the ETL job (clean + write to HBase)

```
docker exec spark-master python3 /opt/spark-apps/etl.py
```

Reads every CSV under `/data/gsod/raw/2025/*.csv` by default. To point at
a different path:

```
docker exec spark-master python3 /opt/spark-apps/etl.py "/data/some/other/path/*.csv"
```



### Step 4 — Run the Analytics job

```
docker exec spark-master python3 /opt/spark-apps/analytics.py
```

By default this scans a **capped 50,000 rows** rather than the full
table (see [Section 11](#11-known-issues--troubleshooting) for why). To
scan more or fewer rows, or the full table:

```
docker exec spark-master python3 /opt/spark-apps/analytics.py 500000   # custom limit
docker exec spark-master python3 /opt/spark-apps/analytics.py 0        # full table, no limit
```

A full-table scan (2.5M+ rows) is not required to demonstrate HBase
read/write capability — the project's goal — and is only worth
attempting if Docker Desktop has enough memory headroom (see
[Section 11](#11-known-issues--troubleshooting)).

### Step 5 — Open the dashboard

Visit [http://localhost:8501](http://localhost:8501)

The Streamlit app reads live from HBase via happybase
(`hbase-thrift:9090`), and falls back to the CSVs `analytics.py` wrote
under `results/` if HBase is unreachable.

## 6. HDFS Commands Reference

```
hdfs dfs -mkdir -p /gsod/raw/2025
hdfs dfs -put -f data/raw/*.csv /gsod/raw/2025/
hdfs dfs -ls /gsod/raw/2025
hdfs dfs -cat /gsod/raw/2025/01001099999.csv | head
hdfs dfsadmin -report
```



## 7. HBase Commands Reference

```
hbase shell

status 'simple'
list
describe 'gsod_weather'
scan 'gsod_weather', {'LIMIT' => 5}
count 'gsod_weather'
get 'gsod_weather', '72530094846#2025-07-04'

docker logs -f hbase-regionserver
docker logs -f hbase-thrift
open http://localhost:16030
```



## 8. ETL / Analytics Commands Reference

```
docker exec spark-master python3 /opt/spark-apps/etl.py
docker exec spark-master python3 /opt/spark-apps/analytics.py
docker exec spark-master python3 /opt/spark-apps/analytics.py 500000   # custom scan limit
docker exec spark-master python3 /opt/spark-apps/analytics.py 0        # full table scan
```



## 9. Required Analyses Covered

- **Dataset Overview** — rows, columns, missing values, duplicate rows
- **Station Analysis** — per-station average/max/min temperature, total precipitation
- **Geographic Analysis** — average temp/precip by latitude band (Arctic → Antarctic)
- **Monthly Analysis** — average temp/precip/wind per month across 2025, month-over-month change
- **Weather Event Analysis** — Fog/Rain/Snow/Hail/Thunder/Tornado day counts, derived from FRSHTT
- **Data Quality** — missing values, duplicate STATION/DATE rows



## 10. HBase Table Schema

```
Table: gsod_weather
Row Key: STATION#DATE

Column Families:
  info
    info:station
    info:name
    info:latitude
    info:longitude
    info:elevation

  weather
    weather:date
    weather:year
    weather:month
    weather:temp
    weather:dewp
    weather:slp
    weather:stp
    weather:visib
    weather:wdsp
    weather:mxspd
    weather:gust
    weather:max
    weather:min
    weather:prcp
    weather:sndp
    weather:fog
    weather:rain
    weather:snow
    weather:hail
    weather:thunder
    weather:tornado
```



## 11. Known Issues & Troubleshooting

**Docker Desktop memory limit.** All nine containers (HDFS, ZooKeeper,
HBase ×3, plus `spark-master`/`streamlit`) share whatever memory limit
is set in Docker Desktop → Settings → Resources → Advanced. The default
(often 4 GB) is tight for this stack under real load — symptoms include
containers being OOM-killed, `docker` commands hanging or returning
blank output, and `hbase-thrift` losing its ZooKeeper session
(`SessionExpiredException` in `docker logs hbase-thrift`) during a
long-running full-table scan. Raise the memory limit as high as your
system can spare (check Task Manager → Performance → Memory for what's
actually free first — Docker will fail to start if you request more
than is available), then Apply and let Docker Desktop restart.

**If** `docker` **commands start hanging or returning blank/garbled
output** (e.g. `docker stats` showing all dashes), the Docker Desktop
engine itself is likely wedged, not just a container:

```
# Quit Docker Desktop via the tray icon first, then:
wsl --shutdown
# Relaunch Docker Desktop, wait for "Engine running", then:
docker-compose up -d
docker-compose ps
```

**If only ZooKeeper/HBase containers seem unhealthy** after a session
timeout (e.g. `analytics.py` hangs with no output on a subsequent run),
try a targeted restart before a full Docker Desktop reset:

```
docker restart zookeeper hbase-master hbase-regionserver hbase-thrift
```

Give it 30-60 seconds to settle before retrying.

## 12. Future Improvements

- Reintroduce PySpark for the ETL/analytics stages if the dataset grows
beyond what a single pandas process handles comfortably, with proper
memory budgeting (raise Docker Desktop's VM memory limit, tune
`spark.sql.shuffle.partitions`, and pre-partition input files) rather
than the ad-hoc per-station file layout that caused OOM kills here.
- Replace the happybase/Thrift bridge with the native Spark-HBase
connector for higher write throughput, if/when Spark is reintroduced.
- Add Apache Airflow to orchestrate upload → ETL → analytics on a schedule.
- Add a real-time ingestion path (Kafka → Spark Structured Streaming → HBase)
for live NOAA feeds.
- Add unit tests for `etl.py` cleaning/validation logic (pytest).
- Add a station map view (folium/pydeck) once lat/lon coverage is verified.
- Add user authentication and role-based access to the Streamlit dashboard.



## 13. Tech Stack

- **Storage:** Apache Hadoop HDFS (optional/legacy — see Section 5, Step 1)
- **Processing:** Python (pandas)
- **NoSQL Store:** Apache HBase (Master + RegionServer + dedicated Thrift gateway)
- **Dashboard:** Streamlit + Plotly
- **Orchestration:** Docker Compose

