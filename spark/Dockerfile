FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Create directories with proper permissions
# Combine mkdir, chmod, chown, and curl commands to reduce layers
RUN mkdir -p /opt/spark-jars /tmp/spark_checkpoints /tmp/spark-events /opt/spark/.ivy2 /opt/spark/work-dir && \
    chmod -R 777 /tmp/spark_checkpoints /tmp/spark-events /opt/spark/.ivy2 && \
    chown -R spark:spark /opt/spark-jars /opt/spark/.ivy2 /opt/spark/work-dir && \
    chmod -R 775 /opt/spark/work-dir && \
    curl -o /opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -o /opt/spark-jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -o /opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -o /opt/spark-jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -o /opt/spark-jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar

# Bake in the Spark applications
COPY spark/ /opt/spark-apps/

USER spark