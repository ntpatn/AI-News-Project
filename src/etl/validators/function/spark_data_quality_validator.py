from src.etl.validators.function.base_quality_validator import BaseDQEngine
from pyspark.sql import functions as F


class SparkDQEngine(BaseDQEngine):
    def schema(self, df, columns):
        return set(columns) - set(df.columns)

    def not_null(self, df, columns):
        errors = []
        for col in columns:
            cnt = df.filter(F.col(col).isNull()).count()
            if cnt > 0:
                errors.append(col)
        return errors

    def unique(self, df, columns):
        total = df.count()
        distinct = df.dropDuplicates(columns).count()
        return total - distinct

    def dtypes(self, df, rules):
        wrong = []
        spark_schema = dict(df.dtypes)  # {'col': 'string', ...}
        for col, dtype in rules.items():
            if spark_schema.get(col) != dtype:
                wrong.append(col)
        return wrong
