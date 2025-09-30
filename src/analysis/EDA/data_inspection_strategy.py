from typing import List, Union
from .base_eda import BaseInspector
import pandas as pd

class ShapeInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print(f"Shape: {df.shape}")


class DtypeInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("Data Types:")
        print(df.dtypes)


class HeadInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("Head:")
        print(df.head())


class TailInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("Tail:")
        print(df.tail())


class NullInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("Missing Values per column:")
        print(df.isnull().sum())


class DuplicatesInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        n_dup = df.duplicated().sum()
        print(f"Duplicated Rows: {n_dup}")


class DescribeInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("Summary Statistics:")
        print(df.describe())


class InfoInspection(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("summary of non-missing values and data types:")
        print(df.info())


class ValueCountsInspection(BaseInspector):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = None
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = columns

    def inspect(self, df: pd.DataFrame):
        cols = self.columns or df.columns
        for col in cols:
            print(f"Value Counts for '{col}':")
            value_counts = df[col].value_counts(dropna=False, ascending=True)
            print(value_counts)


class DataMemoryUsageInspector(BaseInspector):
    def inspect(self, df: pd.DataFrame):
        print("Memory Usage:")
        print(df.memory_usage(deep=True))
        print(
            f"Total Memory Usage: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB"
        )


class DataInspector:
    def __init__(self, strategies: Union[List[BaseInspector], BaseInspector]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseInspector], BaseInspector]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def inspect(self, df: pd.DataFrame):
        for strategy in self.strategies:
            strategy.inspect(df)
            print("-" * 40)


if __name__ == "__main__":
    # Example usage
    data = {
        "column1": [1, 2, None],
        "column2": ["a", "b", "c"],
        "column3": [1.1, 2.2, 3.3],
    }
    df = pd.DataFrame(data)
    strategies = [
        ShapeInspection(),
        # DtypeInspection(),
        # HeadInspection(),
        # NullInspection(),
        # DuplicatesInspection(),
        # DescribeInspection(),
    ]
    inspector = DataInspector(strategies)
    inspector.inspect(df)
    inspector.set_strategies(ShapeInspection())
    inspector.inspect(df)
    inspector.set_strategies(ValueCountsInspection("column1"))
    inspector.inspect(df)
    inspector.set_strategies(ValueCountsInspection(["column1", "column2"]))
    inspector.inspect(df)
    inspector.set_strategies(InfoInspection())
    inspector.inspect(df)
