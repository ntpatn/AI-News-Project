from airflow import DAG
from airflow.decorators import task
from airflow.utils.session import create_session
from airflow.models.xcom import XCom
from airflow.models.log import Log
from datetime import datetime, timedelta, timezone
import subprocess
import os
import glob
import shutil
import psutil


@task()
def cleanup_system():
    print("🧹 Soft Cleanup: /tmp, XCom, and logs older than 6 months...")

    # 1️⃣ ล้าง /tmp
    for f in glob.glob("/tmp/*"):
        try:
            if os.path.isfile(f) or os.path.islink(f):
                os.remove(f)
            elif os.path.isdir(f):
                shutil.rmtree(f, ignore_errors=True)
        except Exception as e:
            print(f"[WARN] Can't remove {f}: {e}")
    print("   • /tmp cleaned")

    # 2️⃣ ลบ log file เก่ากว่า 6 เดือน
    log_dir = os.getenv("AIRFLOW_HOME", "/opt/airflow") + "/logs"
    subprocess.run(f"find {log_dir} -type f -mtime +180 -delete", shell=True)
    print("   • Old log files cleaned (>180 days)")

    # 3️⃣ ล้าง XCom
    with create_session() as session:
        deleted_xcom = session.query(XCom).delete()
        session.commit()
    print(f"   • Deleted {deleted_xcom} XCom records")

    # 4️⃣ ลบ log entries เก่ากว่า 6 เดือน (fix timezone)
    with create_session() as session:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=180)
        deleted_logs = session.query(Log).filter(Log.dttm < cutoff_date).delete()
        session.commit()
    print(f"   • Deleted {deleted_logs} old log entries from DB")

    # 5️⃣ Memory check
    mem_mb = psutil.Process(os.getpid()).memory_info().rss / 1024**2
    print(f"   • Memory usage after cleanup: {mem_mb:.2f} MB")

    print("✅ Soft system cleanup completed.")


with DAG(
    dag_id="system_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["maintenance", "system"],
) as dag:
    cleanup_system()
