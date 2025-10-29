from urllib.parse import urlparse
from pyspark.sql import SparkSession, DataFrame
from src.etl.silver.base.base_extractor import BaseExtractor
from src.etl.silver.registry import register_extractor


@register_extractor("postgres")
class PostgresExtractor(BaseExtractor):
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
        if conn_str:
            resolved_conn = conn_str

        else:
            raise ValueError("Must provide either conn_str or env_var")

        parsed = urlparse(resolved_conn)

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

        # Optional parallel read config
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions

    # ------------------------------------------------------------------
    def extract(self, spark: SparkSession) -> DataFrame:
        """Extract data into Spark DataFrame"""

        print(f"Connecting to: {self.host}:{self.port}/{self.database}")
        print(f"Schema: {self.schema}")

        if self.query:
            source = "custom query"
            wrapped_query = f"({self.query}) as subquery"
            read_target = wrapped_query
        elif self.table:
            source = f"{self.schema}.{self.table}"
            read_target = f"{self.schema}.{self.table}"
        else:
            raise ValueError("Either table or query must be provided.")

        print(f"Extracting from {source} ...")

        read_options = dict(
            url=self.url,
            table=read_target,
            properties=self.properties,
        )

        # Parallel read if parameters are provided
        if self.partition_column and self.num_partitions:
            read_options.update(
                {
                    "column": self.partition_column,
                    "lowerBound": self.lower_bound or 0,
                    "upperBound": self.upper_bound or 100000,
                    "numPartitions": self.num_partitions,
                }
            )

        df = spark.read.jdbc(**read_options)

        print(f"Extracted {df.count()} rows from {source}")
        return df
