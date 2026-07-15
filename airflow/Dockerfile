# Gunakan versi Airflow 2.10.5 untuk kompatibilitas
FROM apache/airflow:2.10.5-python3.11

USER root

# Install OpenJDK 17 (opsional, kalau memang butuh)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set JAVA_HOME (opsional, jika Spark/Hive memang butuh)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install airflow dan provider yang KOMPATIBEL dengan Airflow 2.10.5
RUN pip install --no-cache-dir \
    "pyspark==3.5.4" \
    "apache-airflow[statsd]==2.10.5" \
    "apache-airflow-providers-apache-hive==9.0.0" \
    "apache-airflow-providers-apache-spark==5.0.0"
RUN pip install faker
