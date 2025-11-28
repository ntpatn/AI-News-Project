FROM apache/airflow:2.10.3

USER root

# ติดตั้ง Java และ PostgreSQL client
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    postgresql-client \
 && mkdir -p /opt/spark_drivers \
 && wget -O /opt/spark_drivers/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir torch==2.4.0+cu121 torchvision==0.19.0+cu121 torchaudio==2.4.0+cu121 \
    --index-url https://download.pytorch.org/whl/cu121
# ติดตั้ง Python packages รวม dbt
COPY requirements_airflow.txt /requirements_airflow.txt
RUN pip install --no-cache-dir -r /requirements_airflow.txt \
 && pip install --no-cache-dir \
    dbt-core==1.9.0 \
    dbt-postgres==1.9.0