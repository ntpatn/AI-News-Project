from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, spark: SparkSession) -> DataFrame:
        """Extract data and return as Spark DataFrame"""
        pass
