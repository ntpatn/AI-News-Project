FROM apache/airflow:2.10.3
USER airflow
COPY requirements_airflow.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
