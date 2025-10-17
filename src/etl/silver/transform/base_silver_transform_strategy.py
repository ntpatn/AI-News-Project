from abc import ABC, abstractmethod
import pandas as pd


class BaseSilverTransform(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame):
        pass
