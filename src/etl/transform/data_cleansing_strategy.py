from abc import ABC, abstractmethod
from typing import List, Union
import pandas as pd


class BaseCleansing(ABC):
    @abstractmethod
    def clean(self, df: pd.DataFrame):
        """Perform data inspection and return a DataFrame with results."""
        pass


class DropColumnsCleansing(BaseCleansing):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)

    def clean(self, df: pd.DataFrame):
        """Drop specified columns from DataFrame and return a new DataFrame."""
        if not self.columns:
            print("No columns specified, nothing to drop.")
            return df
        print(f"Dropping columns: {self.columns}")
        return df.drop(columns=self.columns, errors="ignore")


class DropDuplicateKeepFirstCleansing(BaseCleansing):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)

    def clean(self, df: pd.DataFrame):
        if not self.columns:
            print("No columns specified, dropping duplicates based on all columns.")
            return df.drop_duplicates(keep="first")
        for column in self.columns:
            if column not in df.columns:
                print(f"Column '{column}' not found in DataFrame, skipping.")
                continue
            print(f"Dropping duplicates based on column: {column} ...")
            df = df.drop_duplicates(subset=[column], keep="first")
            print(f"Done. Duplicates based on '{column}' have been dropped.")
        return df


class DropNaCleansing(BaseCleansing):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)

    def clean(self, df: pd.DataFrame):
        if not self.columns:
            print("No columns specified, nothing to drop.")
            return df
        print(f"Dropping rows with empty or NaN in '{self.columns}'")
        df = df.dropna(subset=self.columns, axis=0, how="any")
        return df


class SpaceCleansing(BaseCleansing):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)

    def clean(self, df: pd.DataFrame):
        for column in self.columns:
            if column not in df.columns:
                print(f"Column '{column}' not found in DataFrame, skipping.")
                continue
            print(f"Stripping spaces from column: {column}")
            df[column] = df[column].str.strip()
        return df


class LowerCaseCleansing(BaseCleansing):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)

    def clean(self, df: pd.DataFrame):
        for column in self.columns:
            if column not in df.columns:
                print(f"Column '{column}' not found in DataFrame, skipping.")
                continue
            print(f"Converting values in column '{column}' to lowercase.")
            df[column] = df[column].str.lower()
        return df


class ReplaceForCleansing(BaseCleansing):
    def __init__(self, columns=None, to_replace=None, to_replace_with=None):
        ## Initialize the cleansing strategy with columns to clean, values to replace, and their replacements.
        if columns is None:
            self.columns = []
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = list(columns)

        ## Set default values for to_replace and to_replace_with if not provided.
        if to_replace is None:
            self.to_replace = ["", " "]
        elif isinstance(to_replace, (str, float, int)):
            self.to_replace = [to_replace]
        else:
            self.to_replace = list(to_replace)

        # Set the replacement value, which can be a scalar or None.
        if to_replace_with is None:
            self.to_replace_with = None
        elif isinstance(to_replace_with, (list, tuple, set)):
            raise ValueError(
                "Parameter 'to_replace_with' must be a scalar (string, number, or np.nan), not a list/tuple/set."
            )
        else:
            self.to_replace_with = to_replace_with

    def clean(self, df: pd.DataFrame):
        for column in self.columns:
            if column not in df.columns:
                print(f"Column '{column}' not found in DataFrame, skipping.")
                continue
            print(
                f"Replacing {self.to_replace} with {self.to_replace_with} in column: {column}"
            )
            df[column] = df[column].replace(
                to_replace=self.to_replace, value=self.to_replace_with
            )
        return df


class DataCleansing:
    def __init__(self, strategies: Union[List[BaseCleansing], BaseCleansing]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseCleansing], BaseCleansing]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def clean(self, df: pd.DataFrame):
        for strategy in self.strategies:
            df = strategy.clean(df)
            print("-" * 40)
        return df


if __name__ == "__main__":
    # Example usage
    data = {"A": [1, None, 3, None], "B": [4, 5, None, 7]}
    df = pd.DataFrame(data)
    strategies = [
        DropNaCleansing(),
        DropNaCleansing("B"),
        DropNaCleansing(["A", "B"]),
    ]
    cleansing = DataCleansing(strategies)
    df_cleaned = cleansing.clean(df)
    print(df_cleaned)
