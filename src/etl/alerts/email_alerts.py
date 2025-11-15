import json
import logging
from airflow.models import Variable
from airflow.utils.email import send_email

log = logging.getLogger(__name__)


def get_alert_recipients() -> list:
    """Load recipients from Airflow Variables (dynamic, no rebuild)."""
    raw = Variable.get("alert_emails", default_var="")

    # case 1 → JSON list
    if raw.strip().startswith("["):
        try:
            return json.loads(raw)
        except Exception:
            pass

    # case 2 → comma-separated string
    return [e.strip() for e in raw.split(",") if e.strip()]


def build_dq_failure_body(context):
    ti = context.get("task_instance")
    task = context.get("task")
    dag = context.get("dag")
    exception = context.get("exception")

    # pull XCom results
    dq_results = None
    try:
        dq_results = ti.xcom_pull(task_ids=task.task_id, key="dq_results")
    except Exception:
        pass

    html = []
    html.append("<h2>🚨 DQ Validation Failed</h2>")
    html.append(f"<b>DAG:</b> {dag.dag_id}<br>")
    html.append(f"<b>Task:</b> {task.task_id}<br>")
    html.append(f"<b>Run ID:</b> {context.get('run_id')}<br>")
    html.append(f"<b>Exception:</b><pre>{exception}</pre><br>")
    html.append(f"<b>Log URL:</b> <a href='{ti.log_url}'>{ti.log_url}</a><br><br>")

    if dq_results:
        html.append("<h3>DQ Summary</h3>")
        html.append(f"Passed: {dq_results.get('passed')} / {dq_results.get('total')}<br>")
        html.append(f"Failed: {dq_results.get('failed')}<br>")
        html.append(f"Success Rate: {dq_results.get('success_rate')}%<br><br>")

        html.append("<h4>Failures:</h4>")
        for f in dq_results.get("failures", [])[:10]:
            html.append(f"❌ [{f['index']}] {f['expectation']} ({f['column']}): {f.get('error') or f.get('result')}<br>")
    else:
        html.append("<i>No GE summary attached.</i>")

    return "".join(html)


def send_email_on_failure(context):
    recipients = get_alert_recipients()
    if not recipients:
        log.warning("No alert recipients configured. Email not sent.")
        return

    dag = context.get("dag")
    task = context.get("task")

    subject = f"[DQ FAILED] {dag.dag_id}::{task.task_id}"
    body = build_dq_failure_body(context)

    try:
        send_email(to=recipients, subject=subject, html_content=body)
        log.info("Sent DQ failure email to: %s", recipients)
    except Exception as e:
        log.exception("Failed to send alert email: %s", e)
