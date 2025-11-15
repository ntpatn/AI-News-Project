import pandas as pd
import os
import json
import logging
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.exceptions import AirflowException, AirflowSkipException
from src.function.utils.debug.debug_memory import debug_memory_and_files_pandas
from src.function.utils.debug.debug_finally import finalize_task
from src.etl.bronze.transform.data_format_strategy import (
    FromJsonToDataFrameFormatter,
    JsonToTempFormatter,
    CsvToTempFormatter,
    DataFormatter,
)
from src.etl.alerts.email_alerts import send_email_on_failure
from airflow.operators.python import get_current_context
from datetime import timedelta

log = logging.getLogger(__name__)
currentsapi_api_key = os.environ.get("CURRENTS_API_KEY")
if not currentsapi_api_key:
    raise AirflowException("CURRENTS_API_KEY environment variable is not set.")
LANGUAGE = "en"

default_args = {
    "owner": "ai_news_pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": send_email_on_failure,
}


@task(
    retries=3,
)
def extract_get_currentsapi_bronze_pipeline():
    data = None
    from src.etl.bronze.extractors.data_structure_extract_strategy import (
        DataExtractor,
    )

    debug_memory_and_files_pandas("extract:start")
    try:
        config = {
            "type": "api_url",
            "path": "https://api.currentsapi.services/v1/latest-news",
            "params": {"language": LANGUAGE, "apiKey": currentsapi_api_key},
            "max_retry": 5,
            "base_delay": 5,
            "timeout": 30,
        }
        data = DataExtractor.get_extractor(config).extractor()
        json_path = DataFormatter(
            JsonToTempFormatter(prefix="currentsapi_raw_")
        ).formatting(data)
        log.info("Extracted %s and wrote to %s", "currentsapi", json_path)
        return json_path
    except Exception as e:
        log.exception(f"Failed to extract data from CurrentsAPI: {e}")
        raise AirflowException(
            "Force extract_get_currentsapi_bronze_pipeline task to fail"
        )
    finally:
        finalize_task(
            "extract:end", obj=(data), debug_func=debug_memory_and_files_pandas
        )


@task()
def transform_get_currentsapi_bronze_pipeline(json_path: str) -> str:
    from src.etl.bronze.transform.data_metadata_strategy import MetadataAppender

    df = None
    try:
        debug_memory_and_files_pandas("transform:start")

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        ctx = get_current_context()
        df = DataFormatter(
            FromJsonToDataFrameFormatter(array_keys=["news"])
        ).formatting(data)

        metadata = {
            "createdate": pd.Timestamp.now(tz="UTC").to_pydatetime(),
            "usercreate": "system",
            "updatedate": pd.NaT,
            "userupdate": pd.NaT,
            "activedata": True,
            "batch_id": ctx["run_id"],
            "source_system": "currentsapi",
            "layer": "bronze",
            "language": "en",
        }
        df = MetadataAppender(metadata).meta(df)

        csv_path = DataFormatter(
            CsvToTempFormatter(prefix="currentsapi_bronze_")
        ).formatting(df)
        log.info(
            "Transformed JSON -> DataFrame(shape=%s) -> wrote CSV %s",
            df.shape,
            csv_path,
        )
        return csv_path

    except Exception as e:
        log.error(f"Failed to transform CurrentsAPI data: {e}")
        raise AirflowException(
            "Force transform_get_currentsapi_bronze_pipeline task to fail"
        )

    finally:
        finalize_task(
            "transform:end", obj=(df), debug_func=debug_memory_and_files_pandas
        )


@task()
def transform_add_sf_running_currentsapi_bronze_pipeline(csv_path: str) -> str:
    df = None
    debug_memory_and_files_pandas("add_sf:start")
    try:
        from src.function.index.sf_generator import SnowflakeGenerator
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("postgres_localhost_5433")
        dsn = f"dbname={conn.schema} user={conn.login} password={conn.password} host={conn.host} port={conn.port}"
        sf = SnowflakeGenerator(dsn=dsn, source_name="bronze.currentsapi", version_no=1)

        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
        if df.empty:
            log.info("No rows found in bronze CSV %s", csv_path)
            raise AirflowSkipException("No data to add sf_id for")

        df["sf_id"] = sf.bulk_generate_fast(len(df))

        out_csv_path = DataFormatter(
            CsvToTempFormatter(prefix="currentsapi_bronze_sf_")
        ).formatting(df)
        log.info("Added sf_id to DF -> wrote %s (%d rows)", out_csv_path, len(df))
        return out_csv_path

    except AirflowSkipException:
        raise
    except Exception as e:
        log.exception(f"Failed to transform add id_sf in currentsapi data: {e}")
        raise AirflowException(
            "Force transform_add_sf_running_currentsapi_bronze_pipeline task to fail"
        )

    finally:
        finalize_task("add_sf:end", obj=(df), debug_func=debug_memory_and_files_pandas)


@task()
def transform_currentsapi_silver_pipeline(csv_path_with_sf: str) -> str:
    df = None
    debug_memory_and_files_pandas("silver:start")
    try:
        from src.etl.bronze.transform.data_silver_cleaning_strategy import (
            SilverCleaningCurrentsapi,
        )

        df = pd.read_csv(csv_path_with_sf, sep=";", encoding="utf-8-sig")
        if df.empty:
            log.info("No rows for silver cleaning in %s", csv_path_with_sf)
            raise AirflowSkipException("No data for silver cleaning")

        df = SilverCleaningCurrentsapi().silverCleaning(df)
        df["layer"] = "silver"

        out_csv_path = DataFormatter(
            CsvToTempFormatter(prefix="currentsapi_silver_")
        ).formatting(df)
        log.info("Silver cleaning done -> wrote %s (rows=%d)", out_csv_path, len(df))
        return out_csv_path

    except AirflowSkipException:
        raise

    except Exception as e:
        log.error(f"Failed to transform CurrentsApi data for Silver Pipeline: {e}")
        raise AirflowException(
            "Force transform_currentsapi_silver_pipeline task to fail"
        )

    finally:
        finalize_task("silver:end", obj=(df), debug_func=debug_memory_and_files_pandas)


@task()
def load_get_currentsapi_bronze_pipeline(
    csv_path: str, schema: str, table: str, conflict_columns: list
) -> str:
    loader = None
    debug_memory_and_files_pandas("load:start")
    try:
        from src.etl.bronze.loader.data_structure_loader_strategy import (
            PostgresUpsertLoader,
        )

        loader = PostgresUpsertLoader(
            dsn="postgres_localhost_5433",
            schema=schema,
            table=table,
            conflict_columns=conflict_columns,
            delimiter=";",
        )

        with open(csv_path, "r", encoding="utf-8-sig") as f:
            csv_content = f.read()

        loader.loader(csv_content)
        log.info("Data loaded to %s.%s successfully.", schema, table)
        return "ok"

    except AirflowSkipException:
        raise

    except Exception as e:
        log.error(f"Failed to load CurrentsAPI data to database: {e}")
        raise AirflowException(
            "Force load_get_currentsapi_bronze_pipeline task to fail"
        )

    finally:
        finalize_task(
            "load:end", obj=(loader), debug_func=debug_memory_and_files_pandas
        )


@task()
def validate_currentsapi_bronze_pipeline_with_ge_core(csv_path: str) -> str:
    import src.etl.validators.gx_core.validation_utils as gx_utils
    from src.etl.validators.gx_core.expectations.bronze_currentsapi import (
        bronze_expectations_currentsapi,
    )

    df = None
    debug_memory_and_files_pandas("validate_bronze:start")
    try:
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")

        # สร้าง batch แบบ auto
        batch = gx_utils.setup_gx_context_auto(df, name="bronze_currentsapi")

        # โหลด expectation set
        expectations = bronze_expectations_currentsapi()

        # run validation
        results = gx_utils.run_validations(batch, expectations)

        ctx = get_current_context()
        ctx["ti"].xcom_push(key="dq_results", value=results)

        gx_utils.log_validation_summary(csv_path, df, results)
        gx_utils.raise_validation_error(results)

        return csv_path
    except AirflowSkipException:
        raise
    except Exception as e:
        log.error(f"Failed to validation CurrentsAPI data to database: {e}")
        raise AirflowException(
            "Force validate_currentsapi_bronze_pipeline_with_ge_core task to fail"
        )
    finally:
        finalize_task(
            "validate_bronze:end",
            obj=(df, locals().get("batch"), locals().get("expectations")),
            debug_func=debug_memory_and_files_pandas,
        )


@task()
def validate_currentsapi_silver_pipeline_with_ge_core(csv_path: str) -> str:
    import pandas as pd
    import src.etl.validators.gx_core.validation_utils as gx_utils
    from src.etl.validators.gx_core.expectations.silver_currentsapi import (
        silver_expectations_currentsapi,
    )

    df = None
    debug_memory_and_files_pandas("validate_silver:start")
    try:
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")

        batch = gx_utils.setup_gx_context_auto(df, name="silver_currentsapi")
        expectations = silver_expectations_currentsapi()

        results = gx_utils.run_validations(batch, expectations)
        ctx = get_current_context()
        ctx["ti"].xcom_push(key="dq_results", value=results)
        gx_utils.log_validation_summary(csv_path, df, results)
        gx_utils.raise_validation_error(results)

        return csv_path

    except AirflowSkipException:
        raise

    except Exception as e:
        log.error(f"Failed to validate Silver CurrentsAPI data: {e}")
        raise AirflowException(
            "Force validate_currentsapi_silver_pipeline_with_ge_core task to fail"
        )

    finally:
        finalize_task(
            "validate_silver:end",
            obj=(df, locals().get("batch"), locals().get("expectations")),
            debug_func=debug_memory_and_files_pandas,
        )


with DAG(
    dag_id="pipeline_bronze_currentsapi",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["bronze,silver", "currentsapi"],
) as dag:
    with TaskGroup("pipeline_bronze_currentsapi") as etl_bronze_group:
        data_extract = extract_get_currentsapi_bronze_pipeline()
        data_transform = transform_get_currentsapi_bronze_pipeline(data_extract)
        data_add_sf = transform_add_sf_running_currentsapi_bronze_pipeline(
            data_transform
        )
        data_validate = validate_currentsapi_bronze_pipeline_with_ge_core(data_add_sf)
        data_bronze_load = load_get_currentsapi_bronze_pipeline(
            data_validate, "bronze", "source_news_articles_currentsapi", ["id", "url"]
        )
        (
            data_extract
            >> data_transform
            >> data_add_sf
            >> data_validate
            >> data_bronze_load
        )

    with TaskGroup("pipeline_silver_clean_currentsapi") as tl_silver_group:
        data_silver_transform = transform_currentsapi_silver_pipeline(data_validate)

        data_silver_validate = validate_currentsapi_silver_pipeline_with_ge_core(
            data_silver_transform
        )

        data_silver_load = load_get_currentsapi_bronze_pipeline(
            data_silver_validate,
            "silver",
            "clean_news_articles_currentsapi",
            ["id", "url"],
        )

        data_silver_transform >> data_silver_validate >> data_silver_load

    etl_bronze_group >> tl_silver_group
