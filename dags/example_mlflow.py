from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import os
import random 
import mlflow


def log_to_mlflow():
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    with mlflow.start_run(run_name="airflow_demo"):
        mlflow.log_param("owner", "airflow")
        metric = random.random()
        mlflow.log_metric("accuracy", metric)
        print("logged accuracy =", metric)


with DAG(
    dag_id="mlflow_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mlflow"],
):
    PythonOperator(
        task_id="log_metric",
        python_callable=log_to_mlflow,
    )

# def log_to_mlflow(exp_name="exp_minio"):
#     # Airflow ถูกตั้ง env MLFLOW_TRACKING_URI= http://mlflow:5050
#     tracking = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5050")
#     mlflow.set_tracking_uri(tracking)

#     # เลือกปลายทาง artifact ต่อ experiment:
#     #   - exp_minio  -> ใช้ s3://mlflow (MinIO)
#     #   - exp_gcs    -> ใช้ gs://mlflow (fake-GCS)
#     mlflow.set_experiment(exp_name)

#     with mlflow.start_run(run_name=f"airflow_demo_{exp_name}"):
#         mlflow.log_param("owner", "airflow")
#         acc = random.random()
#         mlflow.log_metric("accuracy", acc)
#         print("logged accuracy =", acc, "to", tracking, "exp:", exp_name)


# with DAG(
#     dag_id="mlflow_demo",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["mlflow"],
# ):
#     to_minio = PythonOperator(
#         task_id="log_minio",
#         python_callable=lambda: log_to_mlflow("exp_minio"),
#     )
#     to_gcs = PythonOperator(
#         task_id="log_gcs",
#         python_callable=lambda: log_to_mlflow("exp_gcs"),
#     )

#     to_minio >> to_gcs
