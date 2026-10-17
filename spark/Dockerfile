FROM apache/spark:3.5.5-python3
USER root
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.8.1/iceberg-spark-runtime-3.5_2.12-1.8.1.jar /opt/spark/jars/iceberg-spark-runtime.jar
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar /opt/spark/jars/iceberg-aws-bundle.jar
RUN chmod 644 /opt/spark/jars/iceberg-*.jar
