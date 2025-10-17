from typing import List, Union
from sklearn.preprocessing import LabelEncoder
import pandas as pd
from .base_transform import BaseEncoder



class LabelColumnsEncoder(BaseEncoder):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = columns

    def encode(self, df: pd.DataFrame):
        for col in self.columns:
            le = LabelEncoder()
            encoded_col = f"encoded_{col}"
            df[encoded_col] = le.fit_transform(df[col])
            mapping = {cls: idx for idx, cls in enumerate(le.classes_)}
            print(f"Mapping for {col}: {mapping}")
        return df


class CustomMapEncoder(BaseEncoder):
    def __init__(self, columns=None, mapping=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = columns

        self.mapping = mapping if mapping is not None else {}

    def encode(self, df: pd.DataFrame):
        for col in self.columns:
            encoded_col = f"mapped_{col}"
            if pd.CategoricalDtype.is_dtype(df[col].dtype):
                df[encoded_col] = df[col].cat.rename_categories(self.mapping)
            else:
                df[encoded_col] = df[col].replace(self.mapping)
            print(f"Mapping for {col}: {self.mapping}")
        return df


class DataEncoder:
    def __init__(self, strategies: Union[List[BaseEncoder], BaseEncoder]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseEncoder], BaseEncoder]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def encode(self, df: pd.DataFrame):
        for strategy in self.strategies:
            df = strategy.encode(df)
            print("-" * 40)
        return df
