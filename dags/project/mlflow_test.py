import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator


def train_and_log():
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, classification_report

    from src.build.pipeline.pipeline import pipeline_from_steps_dict
    from src.ml.search.run import run_search
    from src.ml.train.train import train_with_search
    from src.build.matric_logging.mlflow_utils import (
        set_tracking,
        start_run,
        log_metrics,
        log_model,
    )
    from src.build.matric_logging.mlflow_composites import (
        log_search_summary,
        log_classification_report,
    )
    from src.registry import feature, model
    from src.registry import load_plugins

    set_tracking(uri=os.environ["MLFLOW_TRACKING_URI"], experiment="news-experiment")
    X = [
        "economy grows strongly in Q4",
        "stock market drops due to inflation fears",
        "new AI breakthrough announced worldwide",
        "heavy rain expected across northern region",
        "government reveals new policy on education",
        "major company reports unexpected profit",
        "scientists discover new exoplanet",
        "political tensions rise before election",
        "health experts warn about new flu variant",
        "tech companies unveil futuristic devices",
        "severe flood hits coastal towns",
        "central bank prepares interest rate changes",
        "researchers develop advanced cancer therapy",
        "sports team wins championship after a decade",
        "new renewable energy plant opens in japan",
        "wildfire continues spreading in forest region",
        "international summit discusses climate issues",
        "cybersecurity breach impacts major corporation",
        "astronomers detect mysterious radio signals",
        "agriculture output increases due to new tech",
    ]

    y = [1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0, 1]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.4, random_state=42, stratify=y
    )

    # ==== PLUGINS ====
    load_plugins("src.ml", "features")
    load_plugins("src.ml", "model")

    steps = {
        "1_fe": feature.create("tfidf", max_features=500),
        "2_clf": model.create("logreg", C=1.0, max_iter=500),
    }
    pipeline = pipeline_from_steps_dict(steps)

    param_space = {"2_clf__C": [0.1, 1.0, 3.0], "2_clf__max_iter": [200, 500]}

    load_plugins("src.ml", "search")

    with start_run(run_name="tfidf_logreg_grid"):
        search_est = run_search(
            kind="search",
            alias="grid",
            pipeline=pipeline,
            X=X_train,
            y=y_train,
            space=param_space,
        )

        log_search_summary(search_est)

        final_model = train_with_search(
            pipe=pipeline,
            x_train=X_train,
            y_train=y_train,
            search=search_est,
        )

        preds = final_model.predict(X_val)
        acc = accuracy_score(y_val, preds)
        report = classification_report(y_val, preds, output_dict=True)

        log_metrics({"val_accuracy": acc})
        log_classification_report(report, prefix="val_")
        log_model(final_model, artifact_path="model")


with DAG(
    dag_id="news_ml_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mlflow", "training", "ml"],
):
    run_training = PythonOperator(
        task_id="run_train_pipeline",
        python_callable=train_and_log,
    )
