from typing import Any, List, Union
import pandas as pd
from src.etl.transform.base_transform import BaseFormatting
from typing import Optional


class FromJsonToDataFrameFormatter(BaseFormatting):
    def __init__(self, array_keys: Optional[List[str]] = None):
        self.array_keys: List[str] = list(array_keys) if array_keys is not None else []

    def formatting(self, obj: Any) -> pd.DataFrame:
        if not isinstance(obj, dict):
            raise TypeError("FromJsonToDataFrameFormatter expects a dict-like object")
        payload_key = next(
            (k for k in self.array_keys if k in obj and isinstance(obj.get(k), list)),
            None,
        )
        try:
            data = obj[payload_key] if payload_key is not None else obj
            df = pd.json_normalize(data)
            return df
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError("Failed to convert JSON to DataFrame") from exc


class FromDataFrameToCsvFormatter(BaseFormatting):
    def __init__(
        self,
        index: bool = False,
        encoding: str = "utf-8-sig",
        sep: str = ",",
        columns: Optional[list[str]] = None,
    ) -> None:
        """
        Convert DataFrame → CSV string (in-memory only, no local file).

        Args:
            index (bool): Include DataFrame index in CSV output.
            encoding (str): Encoding used for CSV text.
            sep (str): Field separator.
            columns (Optional[list[str]]): List of columns to include.
        """
        self.index = index
        self.encoding = encoding
        self.sep = sep
        self.columns = columns

    def formatting(self, obj: Any) -> str:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError("FromDataFrameToCsvFormatter expects a pandas DataFrame")

        return obj.to_csv(
            index=self.index,
            sep=self.sep,
            columns=self.columns,
            encoding=self.encoding,
        )


class DataFormatter:
    def __init__(self, strategies: Union[List[BaseFormatting], BaseFormatting]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseFormatting], BaseFormatting]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def formatting(self, obj: Any) -> pd.DataFrame:
        for strategy in self.strategies:
            obj = strategy.formatting(obj)
        return obj
