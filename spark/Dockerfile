# spark/Dockerfile
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Tools + python deps
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-pip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a real user (uid=1001) so Hadoop/Spark can resolve username
RUN groupadd -g 1001 spark || true \
 && useradd -m -u 1001 -g 1001 -s /bin/bash spark || true

ENV HOME=/home/spark
ENV USER=spark
ENV LOGNAME=spark
ENV HADOOP_USER_NAME=spark
ENV SPARK_USER=spark

# Install python requirements (for pyspark job code)
WORKDIR /opt/spark-apps
COPY requirements.txt /opt/spark-apps/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark-apps/requirements.txt

# Put required connector jars INTO Spark's jars folder so no Ivy download is needed
# (Spark 3.5.3 / Scala 2.12)
ENV SPARK_HOME=/opt/spark

RUN curl -fL -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && curl -fL -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && curl -fL -o ${SPARK_HOME}/jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
 && curl -fL -o ${SPARK_HOME}/jars/commons-pool2-2.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
 && curl -fL -o ${SPARK_HOME}/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar

# Copy spark apps
COPY spark/streaming.py /opt/spark-apps/streaming.py
COPY spark/batch_job.py /opt/spark-apps/batch_job.py

RUN chown -R 1001:1001 /opt/spark-apps ${SPARK_HOME}/jars

USER 1001
WORKDIR /opt/spark-apps
