from abc import ABC, abstractmethod
from typing import Any, Union
import pandas as pd
from pathlib import Path

DataInput = Union[pd.DataFrame, str, Path]


class BaseStructureLoader(ABC):
    @abstractmethod
    def loader(self, data: DataInput, **kwargs) -> Any:
        pass
