from abc import ABC, abstractmethod
from typing import Any
import pandas as pd


class BaseCleansing(ABC):
    @abstractmethod
    def clean(self, df: pd.DataFrame):
        pass


class BaseCombining(ABC):
    @abstractmethod
    def combine(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class BaseEncoder(ABC):
    @abstractmethod
    def encode(self, df: pd.DataFrame):
        pass


class BaseTransform(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame):
        pass


class BaseFormatting(ABC):
    @abstractmethod
    def formatting(self, obj: Any):
        pass


class BaseMeta(ABC):
    @abstractmethod
    def meta(self, obj: Any):
        pass
