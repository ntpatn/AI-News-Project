"""Test extract task"""

import pytest
import os
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
import pandas as pd


class TestExtractCurrentsAPI:
    """Test extract functionality"""

    @patch.dict(os.environ, {"CURRENTS_API_KEY": "test_key", "DEBUG_MODE": "false"})
    @patch("src.function.utils.debug.debug_memory.debug_memory_and_files_pandas")
    @patch("src.function.utils.debug.debug_finally.finalize_task")
    @patch("src.etl.bronze.transform.data_format_strategy.DataFormatter")
    @patch(
        "src.etl.bronze.extractors.data_structure_extract_strategy.DataExtractor.get_extractor"
    )
    def test_extract_success(
        self, mock_get_extractor, mock_formatter, mock_finalize, mock_debug
    ):
        """Extract should return JSON path on success"""

        # Mock API response
        mock_extractor_instance = MagicMock()
        mock_extractor_instance.extractor.return_value = {
            "status": "ok",
            "news": [{"id": "1", "title": "Test Article"}],
        }
        mock_get_extractor.return_value = mock_extractor_instance

        # Mock formatter
        mock_formatter_instance = MagicMock()
        mock_formatter_instance.formatting.return_value = "/tmp/test_extract.json"
        mock_formatter.return_value = mock_formatter_instance

        # Import task
        from dags.bronze.pipeline_bronze_currenapi import (
            extract_get_currentsapi_bronze_pipeline,
        )

        # Get the actual Python function (unwrap from @task decorator)
        task_obj = extract_get_currentsapi_bronze_pipeline

        # Execute the underlying Python function
        if hasattr(task_obj, "function"):
            # Airflow 2.x
            result = task_obj.function()
        elif hasattr(task_obj, "python_callable"):
            # Airflow older version
            result = task_obj.python_callable()
        else:
            # Fallback: call directly
            result = task_obj()

        assert result == "/tmp/test_extract.json"
        mock_extractor_instance.extractor.assert_called_once()

    @patch.dict(os.environ, {"CURRENTS_API_KEY": "test_key", "DEBUG_MODE": "false"})
    @patch("src.function.utils.debug.debug_memory.debug_memory_and_files_pandas")
    @patch("src.function.utils.debug.debug_finally.finalize_task")
    @patch(
        "src.etl.bronze.extractors.data_structure_extract_strategy.DataExtractor.get_extractor"
    )
    def test_extract_api_failure(self, mock_get_extractor, mock_finalize, mock_debug):
        """Extract should fail gracefully on API error"""

        # Mock API error
        mock_get_extractor.side_effect = Exception("API connection failed")

        # Import task
        from dags.bronze.pipeline_bronze_currenapi import (
            extract_get_currentsapi_bronze_pipeline,
        )

        # Execute and expect exception
        with pytest.raises(AirflowException):
            if hasattr(extract_get_currentsapi_bronze_pipeline, "function"):
                extract_get_currentsapi_bronze_pipeline.function()
            else:
                extract_get_currentsapi_bronze_pipeline()
