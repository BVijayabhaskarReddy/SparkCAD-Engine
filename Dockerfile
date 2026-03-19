# 1. Use Miniconda as the base
FROM continuumio/miniconda3:latest

# 2. Install Java 21 and System Dependencies
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    libgl1 \
    libglu1-mesa \
    libxext6 \
    libxt6 \
    && apt-get clean

# 3. Install pythonocc-core AND downgrade Python to 3.12
# pythonocc-core is not yet compatible with Python 3.13
RUN conda install -y \
    -c conda-forge \
    python=3.12 \
    pythonocc-core=7.8.1

# 4. Set Environment Variables for Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 5. Download and Install Spark
RUN curl -sL "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | tar -xz -C /opt/ && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME

# 6. Install Python utility libraries with pip
RUN pip install --no-cache-dir \
    pyspark==$SPARK_VERSION \
    delta-spark \
    dbt-spark[session] \
    boto3 \
    pandas

WORKDIR /app