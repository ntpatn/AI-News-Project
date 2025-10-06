from abc import ABC, abstractmethod
from typing import Any, Union, Dict
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class LoadResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)


DataInput = Union[pd.DataFrame, str, Path]
BinaryInput = Union[bytes, str, Path]


class BaseStructureLoader(ABC):
    @abstractmethod
    def create(self, data: DataInput, **kwargs) -> Any:
        pass

    @abstractmethod
    def update(self, data: DataInput, **kwargs) -> Any:
        pass


class BaseUnstructureLoader(ABC):
    @abstractmethod
    def create(self, data: BinaryInput, key: Any, **kwargs) -> Any:
        pass

    @abstractmethod
    def update(self, data: BinaryInput, key: Any, **kwargs) -> Any:
        pass

    @abstractmethod
    def delete(self, *, by: str, value: Any, key: Any, **kwargs) -> Any:
        pass
