from typing import List, Union
import pandas as pd
from .base_transform import BaseTransform


class AstypeTransform(BaseTransform):
    def __init__(self, dtype_map=None):
        if dtype_map is None:
            self.dtype_map = {}
        else:
            self.dtype_map = dict(dtype_map)

    def transform(self, df: pd.DataFrame):
        if not self.dtype_map:
            print("No dtype mapping specified, nothing to change.")
            return df
        for column, dtype in self.dtype_map.items():
            if column in df.columns:
                print(f"Changing column '{column}' to {dtype} ...")
                df[column] = df[column].astype(dtype)
            else:
                print(f"Column '{column}' not found in DataFrame. Skipping.")
        return df


class RenameColumnsTransform(BaseTransform):
    def __init__(self, rename_map=None):
        if rename_map is None:
            self.rename_map = {}
        else:
            self.rename_map = rename_map

    def transform(self, df: pd.DataFrame):
        if not self.rename_map:
            print("No columns to rename.")
            return df
        print(f"Renaming columns: {self.rename_map}")
        return df.rename(columns=self.rename_map)


class ReplaceValuesTransform(BaseTransform):
    def __init__(self, column=None, replace_map=None):
        self.column = column
        self.replace_map = replace_map if replace_map is not None else {}

    def transform(self, df: pd.DataFrame):
        if self.column is None or self.column not in df.columns:
            print(
                f"Column '{self.column}' not found in DataFrame. Skipping replacement."
            )
            return df
        if not self.replace_map:
            print(f"No replacement map provided for column '{self.column}'.")
            return df
        print(f"Replacing values in column '{self.column}' with {self.replace_map} ...")
        df[self.column] = df[self.column].replace(self.replace_map)
        return df


class DataTransform:
    def __init__(self, strategies: Union[List[BaseTransform], BaseTransform]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseTransform], BaseTransform]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def transform(self, df: pd.DataFrame):
        for strategy in self.strategies:
            df = strategy.transform(df)
            print("-" * 40)
        return df
