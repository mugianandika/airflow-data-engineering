# Use Bitnami Spark 3.5.6 legacy image as the base
FROM bitnamilegacy/spark:3.5.6

# Switch to root to install packages and modify system files
USER root

# Install Python 3.11 and related tools, create convenient symlinks,
# then clean up apt cache to reduce image size
RUN apt-get update \
    && apt-get install -y python3.11 python3.11-venv python3.11-dev \
    && ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Ensure PySpark uses the installed Python interpreter
ENV PYSPARK_PYTHON=/usr/bin/python3.11

# Make sure /usr/bin is in PATH (usually already is, but declared explicitly)
ENV PATH="/usr/bin:${PATH}"

# Add a 'spark' user entry to /etc/passwd and create the home directory
# This mirrors a system user entry for UID/GID 1001 used in the base image
RUN echo "spark:x:1001:1001::/home/spark:/bin/bash" >> /etc/passwd \
    && mkdir -p /home/spark \
    && chown -R 1001:1001 /home/spark

# Set HOME for the spark user environment
ENV HOME=/home/spark

# Prepare an Ivy cache directory for Spark JARs and ensure proper ownership
RUN mkdir -p /opt/bitnami/spark/ivy \
    && chown -R 1001:1001 /opt/bitnami/spark/ivy

# Configure Spark to use the created Ivy directory for resolving jars
RUN echo "spark.jars.ivy /opt/bitnami/spark/ivy" \
    >> /opt/bitnami/spark/conf/spark-defaults.conf

# Switch to the non-root spark user (UID 1001) for running the container
USER 1001
