FROM apache/spark:3.5.0-python3

USER root

RUN groupadd -g 1000 sparkuser && \
    useradd -u 1000 -g sparkuser -m sparkuser && \
    mkdir -p /home/sparkuser/.ivy2 && \
    chown -R sparkuser:sparkuser /home/sparkuser /opt/spark

RUN mkdir -p /opt/spark/jars && \
    curl -o /opt/spark/jars/postgresql-42.5.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.4.jar && \
    curl -o /opt/spark/jars/spark-cassandra-connector_2.12-3.4.0.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.4.0/spark-cassandra-connector_2.12-3.4.0.jar && \
    curl -o /opt/spark/jars/neo4j-connector-apache-spark_2.12-5.0.0.jar \
    https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_2.12/5.0.0/neo4j-connector-apache-spark_2.12-5.0.0.jar && \
    curl -o /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar && \
    curl -o /opt/spark/jars/spark-redis_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/com/redislabs/spark-redis_2.12/3.1.0/spark-redis_2.12-3.1.0.jar && \
    curl -o /opt/spark/jars/clickhouse-jdbc-0.4.6.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6.jar

ENV HADOOP_USER_NAME=sparkuser \
    USER=sparkuser \
    HOME=/home/sparkuser \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        libnss3 \
        libnss3-tools && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=sparkuser:sparkuser spark-apps /opt/spark/apps
COPY --chown=sparkuser:sparkuser requirements.txt /app/requirements.txt
COPY --chown=sparkuser:sparkuser .env /opt/spark

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    pip install --no-cache-dir \
        psycopg2-binary \
        clickhouse-connect \
        pymongo \
        redis

USER sparkuser

WORKDIR /opt/spark/apps

CMD ["/opt/spark/bin/spark-submit", \
    "--conf", "spark.jars.ivy=/home/sparkuser/.ivy2", \
    "--conf", "spark.hadoop.security.authentication=simple", \
    "--conf", "spark.executorEnv.HADOOP_USER_NAME=sparkuser", \
    "/opt/spark/apps/snowflake-transform.py"]