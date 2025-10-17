from typing import Any
import pandas as pd
from src.etl.bronze.transform.base_transform import BaseMeta


class MetadataAppender(BaseMeta):
    def __init__(self, metadata: dict):
        self.metadata = metadata

    def meta(self, obj: Any) -> pd.DataFrame:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError("MetadataAppender expects a pandas DataFrame")

        for key, value in self.metadata.items():
            if key not in obj.columns:
                obj[key] = value
            else:
                obj[key] = obj[key].fillna(value)
        return obj
