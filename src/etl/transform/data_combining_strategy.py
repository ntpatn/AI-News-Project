from abc import ABC, abstractmethod
from typing import List, Union
import pandas as pd


class BaseCombining(ABC):
    @abstractmethod
    def combine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform data combining and return a DataFrame with results."""
        pass


class ConcatenateDataFramesCombining(BaseCombining):
    def __init__(self, dataframes: List[pd.DataFrame]):
        self.dataframes = dataframes

    def combine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Concatenate multiple DataFrames into one."""
        if not self.dataframes:
            print("No DataFrames provided, returning the original DataFrame.")
            return df
        print(f"Concatenating {len(self.dataframes)} DataFrames...")
        df = pd.concat([df] + self.dataframes, ignore_index=True)
        print("Done. DataFrames have been concatenated.")
        return df


class JoinDataFrames(BaseCombining):
    def __init__(
        self, other_df: pd.DataFrame, on=None, how="inner", lsuffix="", rsuffix=""
    ):
        self.other_df = other_df
        self.on = on
        self.how = how
        self.lsuffix = lsuffix
        self.rsuffix = rsuffix

    def combine(self, df: pd.DataFrame):
        print(f"Joining with method {self.how} on {self.on} ...")
        df = df.join(
            self.other_df,
            on=self.on,
            how=self.how,
            lsuffix=self.lsuffix,
            rsuffix=self.rsuffix,
        )
        print("Join complete.")
        return df


class DataCombining:
    def __init__(self, strategies: Union[List[BaseCombining], BaseCombining]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseCombining], BaseCombining]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def combine(self, df: pd.DataFrame) -> pd.DataFrame:
        for strategy in self.strategies:
            df = strategy.combine(df)
            print("-" * 40)
        return df
