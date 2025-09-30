from abc import ABC, abstractmethod
import pandas as pd
from typing import Any


class BaseStructureExtractor(ABC):
    @abstractmethod
    def extractor(self) -> pd.DataFrame:
        pass


class BaseUnstructureExtractor(ABC):
    @abstractmethod
    def extractor(self) -> Any:
        pass
