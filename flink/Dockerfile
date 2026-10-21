# Official Flink image + the Kafka SQL connector (version must match Flink: 3.3.0-1.20 <-> 1.20)
FROM flink:1.20.1-scala_2.12-java17

ADD --chown=flink:flink \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
    /opt/flink/lib/

# Pre-create so the named volume inherits flink ownership
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
