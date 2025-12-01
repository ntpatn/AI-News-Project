FROM apache/airflow:2.10.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    wget \
    postgresql-client \
    build-essential \
    git \
    curl \
 && mkdir -p /opt/spark_drivers \
 && wget -q -O /opt/spark_drivers/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir torch==2.4.0+cu121 torchvision==0.19.0+cu121 torchaudio==2.4.0+cu121 \
    --index-url https://download.pytorch.org/whl/cu121

COPY requirements_airflow.txt /requirements_airflow.txt

RUN pip install --no-cache-dir -r /requirements_airflow.txt \
    && pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-postgres==1.9.1
    
WORKDIR /opt/airflow