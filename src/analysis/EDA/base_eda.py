from abc import ABC, abstractmethod
import pandas as pd

class BaseInspector(ABC):
    @abstractmethod
    def inspect(self, df: pd.DataFrame):
        """Perform data inspection and return a DataFrame with results."""
        pass

class BaseStatistic(ABC):
    @abstractmethod
    def result(self, df: pd.DataFrame):
        pass