import os
import requests
import json

base = os.environ["AIRFLOW_BASE_URL"].rstrip("/")
auth = (os.environ["AIRFLOW_API_USER"], os.environ["AIRFLOW_API_PASSWORD"])
dag_id = os.environ["DAG_ID"]

r = requests.post(
    f"{base}/api/v1/dags/{dag_id}/dagRuns",
    auth=auth,
    headers={"Content-Type": "application/json"},
    data=json.dumps({"conf": {"from": "project-b"}}),
    timeout=30,
)
r.raise_for_status()
print("Triggered:", r.json().get("dag_run_id"))
