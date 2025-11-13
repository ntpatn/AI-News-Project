FROM apache/airflow:2.10.3
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk wget \
 && mkdir -p /opt/spark_drivers \
 && wget -O /opt/spark_drivers/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
COPY requirements_airflow.txt /requirements_airflow.txt
RUN pip install --no-cache-dir -r /requirements_airflow.txt
