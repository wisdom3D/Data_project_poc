FROM apache/spark:3.4.1

USER root

# Hadoop AWS + S3
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Iceberg Runtime
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.0/iceberg-spark-runtime-3.4_2.12-1.3.0.jar /opt/spark/jars/

# Postgres JDBC 
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

USER spark
