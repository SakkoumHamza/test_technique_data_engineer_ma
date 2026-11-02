FROM apache/airflow:2.10.5-python3.11

USER root
# Resolve JAVA_HOME dynamically for both amd64 and arm64 images.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY --chown=airflow:root requirements.txt .
USER airflow
# Keep the Airflow version fixed when installing extra dependencies.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r requirements.txt \
    && SPARK_JARS="$(python -c 'import os,pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fsSL -o "$SPARK_JARS/hadoop-aws-3.3.4.jar" https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o "$SPARK_JARS/aws-java-sdk-bundle-1.12.262.jar" https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
COPY --chown=airflow:root pipeline ./pipeline
COPY --chown=airflow:root dags ./dags
COPY --chown=airflow:root tests ./tests

COPY --chown=airflow:root scripts ./scripts
