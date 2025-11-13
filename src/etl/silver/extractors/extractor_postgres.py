from urllib.parse import urlparse
from pyspark.sql import SparkSession, DataFrame

# from etl.silver.extractors.base_silver_extractors_strategy import BaseExtractor
from src.registry import extractor
import logging

logger = logging.getLogger(__name__)


@extractor.register("postgres", "pg")
class PostgresExtractor:
    def __init__(
        self,
        conn_str: str = None,
        schema: str = "silver",
        table: str = None,
        query: str = None,
        fetch_size: int = 10000,
        partition_column: str = None,
        lower_bound: int = None,
        upper_bound: int = None,
        num_partitions: int = None,
    ):
        if not conn_str:
            raise ValueError("Must provide conn_str")

        parsed = urlparse(conn_str)

        self.host = parsed.hostname
        self.port = parsed.port or 5432
        self.database = parsed.path.lstrip("/")
        self.user = parsed.username
        self.password = parsed.password
        self.schema = schema
        self.table = table
        self.query = query

        self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        self.properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            "fetchsize": str(fetch_size),
        }

        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extract data into Spark DataFrame"""
        logger.info(f"Connecting to: {self.host}:{self.port}/{self.database}")
        logger.info(f"Schema: {self.schema}")

        if self.query:
            source = "custom query"
            read_target = f"({self.query}) as subquery"
        elif self.table:
            source = f"{self.schema}.{self.table}"
            read_target = f"{self.schema}.{self.table}"
        else:
            raise ValueError("Either 'table' or 'query' must be provided.")

        logger.info(f"Extracting from {source} ...")

        if self.partition_column and self.num_partitions:
            df = spark.read.jdbc(
                url=self.url,
                table=read_target,
                column=self.partition_column,
                lowerBound=self.lower_bound or 0,
                upperBound=self.upper_bound or 100000,
                numPartitions=self.num_partitions,
                properties=self.properties,
            )
        else:
            df = spark.read.jdbc(
                url=self.url,
                table=read_target,
                properties=self.properties,
            )

        row_count = df.count()
        logger.info(f"✅ Extracted {row_count:,} rows from {source}")
        return df
