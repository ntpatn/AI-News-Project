from airflow import DAG
from datetime import datetime
import logging
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException, AirflowSkipException
from src.function.utils.debug.debug_memory import debug_memory_and_files_pandas
from src.function.utils.debug.debug_finally import finalize_task
from src.etl.bronze.transform.data_format_strategy import (
    CsvToTempFormatter,
    DataFormatter,
)
import pandas as pd
import os
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)
con = os.environ.get("Connection_String2")


@task.bash
def dbt_run():
    return "cd /opt/airflow/dbt && dbt run --select silver_dbt_combined"


@task.bash
def dbt_test():
    return "cd /opt/airflow/dbt && dbt test --select silver_dbt_combined"


@task
def extract_data():
    from src.etl.bronze.extractors.data_structure_extract_strategy import DataExtractor

    try:
        debug_memory_and_files_pandas("extract:start")

        if not con:
            raise AirflowException(
                "Connection_String2 environment variable is not set."
            )
        db_config = {
            "type": "db",
            "conn_str": con,
            "table": "silver_dbt_combined",
            "schema": "silver",
        }
        with DataExtractor.get_extractor(cfg=db_config) as Extractor:
            df = Extractor.extractor()
        csv_path = DataFormatter(
            CsvToTempFormatter(prefix="silver_dbt_combined_extract_")
        ).formatting(df)
        log.info(
            "Transformed Database -> DataFrame(shape=%s) -> wrote CSV %s",
            df.shape,
            csv_path,
        )
        return csv_path

    except Exception as e:
        log.exception(f"Failed to extract data from silver_dbt_combined: {e}")
        raise AirflowException("Force extract_data task to fail")
    finally:
        finalize_task("extract:end", obj=(df), debug_func=debug_memory_and_files_pandas)


@task()
def transform_data(csv_path: str) -> str:
    from src.function.index.sf_generator import SnowflakeGenerator

    df = None
    debug_memory_and_files_pandas("transform:start")
    try:
        df = pd.read_csv(
            csv_path,
            sep=";",
            encoding="utf-8-sig",
            dtype={"sf_id": "object"},
        )
        if df.empty:
            log.info("No rows for silver cleaning in %s", csv_path)
            raise AirflowSkipException("No data for silver cleaning")
        sf = SnowflakeGenerator(dsn=con, source_name="silver.dbt.combined")
        df = sf.assign_ids_from_dataframe(df, ts_col="createdate", sf_col="sf_id")
        if df["sf_id"].duplicated().any():
            raise AirflowException("Duplicate sf_id found after assignment.")
        df["category"] = (
            df["category"]
            .str.replace("'", "", regex=False)
            .str.replace(", ", ",")
            .str.strip()
        )
        out_csv_path = DataFormatter(
            CsvToTempFormatter(prefix="silver_dbt_combined_transform_")
        ).formatting(df)
        log.info("Silver transform done -> wrote %s (rows=%d)", out_csv_path, len(df))
        return out_csv_path

    except AirflowSkipException:
        raise

    except Exception as e:
        log.exception(f"Failed to transform data from silver_dbt_combined: {e}")
        raise AirflowException(
            "Force transform_currentsapi_silver_pipeline task to fail"
        )

    finally:
        finalize_task(
            "transform:end", obj=(df), debug_func=debug_memory_and_files_pandas
        )


@task()
def validate_silver_pipeline_with_ge_core(csv_path: str) -> str:
    import src.etl.validators.gx_core.validation_utils as gx_utils
    from src.etl.validators.gx_core.expectations.dbt_validate_silver import (
        silver_expectations_dbt_combined,
    )

    df = None
    debug_memory_and_files_pandas("validate_silver:start")
    try:
        df = pd.read_csv(
            csv_path,
            sep=";",
            encoding="utf-8-sig",
            dtype={"sf_id": "object"},
        )

        batch = gx_utils.setup_gx_context_auto(df, name="silver_currentsapi")
        expectations = silver_expectations_dbt_combined()

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


@task()
def load_pipeline(
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
        raise AirflowException("Force load_get_currentsapi_pipeline task to fail")

    finally:
        finalize_task(
            "load:end", obj=(loader), debug_func=debug_memory_and_files_pandas
        )


with DAG(
    dag_id="pipeline_silver_dbt_combined",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    with TaskGroup("Test") as test:
        combine_data_base_with_dbt = dbt_run()

        test_data_base_with_dbt = dbt_test()

        extract = extract_data()

        transform = transform_data(extract)
        validate = validate_silver_pipeline_with_ge_core(transform)
        load = load_pipeline(validate, "silver", "silver_combined", ["url"])
        (
            combine_data_base_with_dbt
            >> test_data_base_with_dbt
            >> extract
            >> transform
            >> validate
            >> load
        )

    test
