FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    happybase==1.2.0 \
    thrift==0.16.0 \
    "pandas<2.1"

WORKDIR /opt/spark-apps

COPY . /opt/spark-apps

ENV PYTHONUNBUFFERED=1
