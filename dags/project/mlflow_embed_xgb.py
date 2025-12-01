import os
from datetime import datetime
from airflow.models import DAG
from src.function.utils.debug.debug_memory import debug_memory_and_files_pandas
from src.function.utils.debug.debug_finally import finalize_task
from airflow.decorators import task
import logging
from airflow.exceptions import AirflowException
from src.etl.bronze.transform.data_format_strategy import (
    CsvToTempFormatter,
    DataFormatter,
)
import pandas as pd

log = logging.getLogger(__name__)
con = os.environ.get("Connection_String2")


@task
def extract_gold_features():
    from src.etl.bronze.extractors.data_structure_extract_strategy import DataExtractor

    try:
        debug_memory_and_files_pandas("extract_gold:start")

        if not con:
            raise AirflowException("Connection_String2 is not set.")

        db_config = {
            "type": "db",
            "conn_str": con,
            "table": "news_features",
            "schema": "gold",
        }

        with DataExtractor.get_extractor(cfg=db_config) as Extractor:
            df = Extractor.extractor()

        df = df[
            [
                "sf_id",
                "embedding",
                "category_encode",
            ]
        ]

        csv_path = DataFormatter(
            CsvToTempFormatter(prefix="gold_features_extract_")
        ).formatting(df)

        log.info("Extract Gold → %s rows → %s", len(df), csv_path)
        return csv_path

    except Exception as e:
        log.exception(f"Failed to extract gold features: {e}")
        raise AirflowException("Force extract_gold_features to fail")

    finally:
        finalize_task(
            "extract_gold:end", obj=(df), debug_func=debug_memory_and_files_pandas
        )


@task
def train_pipeline(csv_path):
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, classification_report

    from src.build.pipeline.pipeline import pipeline_from_steps_dict
    from src.ml.search.run import run_search
    from src.ml.train.train import train_with_search

    # MLflow utils ของนาย
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
    import json

    set_tracking(uri=os.environ["MLFLOW_TRACKING_URI"], experiment="news-experiment")
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
    # 1) rare category → general
    y = df["category_encode"]

    # X = embedding vector (list of floats)
    X = df["embedding"].apply(json.loads).tolist()

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.4, random_state=42, stratify=y
    )

    load_plugins("src.ml", "features")
    load_plugins("src.ml", "model")
    load_plugins("src.ml", "search")

    steps = {
        # "1_embed": feature.create(
        #     "embed",
        #     model_name="sentence-transformers/all-MiniLM-L6-v2",
        #     batch_size=32,
        #     show_progress_bar=False,
        # ),
        "1_xgb": model.create("xgb"),
    }

    pipeline = pipeline_from_steps_dict(steps)

    param_space = {
        "1_xgb__max_depth": [4, 6, 8],
        "1_xgb__learning_rate": [0.03, 0.05, 0.1],
        "1_xgb__n_estimators": [200, 300],
    }

    with start_run(run_name="embed_xgb_training"):
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

        # log accuracy / report
        log_metrics({"val_accuracy": acc})
        log_classification_report(report, prefix="val_")

        # log model final state
        log_model(final_model, artifact_path="model")


with DAG(
    dag_id="news_ml_pipeline_embed_xgb",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mlflow", "training", "ml", "embed", "xgboost"],
):
    extract = extract_gold_features()

    train = train_pipeline(extract)
    extract >> train
