from .base_eda import BaseStatistic
from typing import List, Union
import pandas as pd


class MeanResult(BaseStatistic):
    def __init__(self, columns=None):
        if columns is None:
            self.columns = None
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = columns

    def result(self, df: pd.DataFrame):
        if self.columns is None:
            result = df.mean(numeric_only=True)
            print(result)
            return result
        else:
            result = {}
            for col in self.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    result[col] = df[col].mean()
                else:
                    result[col] = None
            print(result)
            return result


class DataStatistic:
    def __init__(self, strategies: Union[List[BaseStatistic], BaseStatistic]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseStatistic], BaseStatistic]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def result(self, df: pd.DataFrame):
        results = []
        for strategy in self.strategies:
            results.append(strategy.result(df))
        return results


if __name__ == "__main__":
    # Example usage
    data = {
        "column1": [1, 2, None],
        "column2": ["a", "b", "c"],
        "column3": [1.1, 2.2, 3.3],
    }
    df = pd.DataFrame(data)
    strategies = [
        MeanResult(),
        MeanResult("column3"),
        MeanResult(["column1", "column3"]),
    ]
    statistic = DataStatistic(strategies)
    result = statistic.result(df)
    print("." * 40)
    print("." * 40)
    # Mean ของทุกคอลัมน์ที่เป็นตัวเลข
    stat1 = DataStatistic(MeanResult())
    stat1.result(df)

    # Mean ของเฉพาะ column3
    stat2 = DataStatistic(MeanResult("column3"))
    stat2.result(df)

    # Mean ของหลาย column
    stat3 = DataStatistic(MeanResult(["column1", "column3"]))
    stat3.result(df)
