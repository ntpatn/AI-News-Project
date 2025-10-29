import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import Union, List

class CleanTextPreserveMeaningFast:
    def __init__(self, text_columns: Union[str, List[str]]):
        self.text_columns = (
            [text_columns] if isinstance(text_columns, str) else text_columns
        )

    def _clean_column(self, col):
        """Apply regex-based cleaning using Spark native functions only."""

        # 1. Remove control/invisible chars
        cleaned = F.regexp_replace(col, r"[\x00-\x1F\x7F\u200B]+", " ")

        # 2. Remove HTML tags
        cleaned = F.regexp_replace(cleaned, r"<[^>]+>", " ")

        # 3. Remove URLs
        cleaned = F.regexp_replace(cleaned, r"http\S+|www\S+", " ")

        # 4. Remove NewsAPI-style tail e.g. [+3880 chars]
        cleaned = F.regexp_replace(cleaned, r"\[\+\d+\s*chars\]", " ")

        # 5. Normalize punctuation (collapse repeats)
        cleaned = F.regexp_replace(cleaned, r"([!?.,])\1+", r"\1")

        # 6. Normalize spaces
        cleaned = F.regexp_replace(cleaned, r"\s+", " ")

        # 7. Fix spacing around punctuation
        cleaned = F.regexp_replace(cleaned, r"\s+([.,!?;:])", r"\1")
        cleaned = F.regexp_replace(cleaned, r"([.,!?;:])(?=[^\s])", r"\1 ")

        # 8. Trim leading/trailing spaces
        cleaned = F.trim(cleaned)

        # 9. Replace empty strings with null
        cleaned = F.when(F.length(cleaned) == 0, None).otherwise(cleaned)

        return cleaned

    def transform(self, df: DataFrame) -> DataFrame:
        """Clean multiple text columns efficiently (no UDF)."""
        for col_name in self.text_columns:
            if col_name in df.columns:
                print(f"Cleaning text column: {col_name}")
                df = df.withColumn(col_name, self._clean_column(F.col(col_name)))
            else:
                print(f"⚠️ Warning: Column '{col_name}' not found.")
        print("✅ Fast text cleaning complete.")
        return df
