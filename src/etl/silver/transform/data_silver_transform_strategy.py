from typing import List, Union
import pandas as pd
import re, html, unicodedata
from abc import ABC, abstractmethod
from .base_silver_transform_strategy import BaseSilverTransform


class CleanTextPreserveMeaning(BaseSilverTransform):
    def __init__(self, text_columns: Union[str, List[str]]):
        self.text_columns = (
            [text_columns] if isinstance(text_columns, str) else text_columns
        )

    def _clean_text(self, text: str) -> Union[str, pd._libs.missing.NAType]:
        # 🔹 หากค่าเป็น None หรือ NaN → คืน pd.NA
        if pd.isnull(text) or not isinstance(text, str):
            return pd.NA

        # 1. Normalize unicode
        text = unicodedata.normalize("NFKC", text)

        # 2. Remove control / invisible chars
        text = re.sub(r"[\x00-\x1F\x7F\u200B]+", " ", text)

        # 3. Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # 4. Decode HTML entities
        text = html.unescape(text)

        # 5. Remove URLs
        text = re.sub(r"http\S+|www\S+", "", text)

        # 6. Remove NewsAPI-style tail e.g. '[+3880 chars]'
        text = re.sub(r"\[\+\d+\s*chars\]", "", text)

        # 7. Normalize punctuation
        text = re.sub(r"([!?.,])\1+", r"\1", text)

        # 8. Normalize spaces
        text = re.sub(r"\s+", " ", text).strip()

        # 9. Fix spacing around punctuation
        text = re.sub(r"\s+([.,!?;:])", r"\1", text)
        text = re.sub(r"([.,!?;:])(?=[^\s])", r"\1 ", text)

        return text.strip() if text else pd.NA

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean multiple text columns while preserving meaning and structure."""
        for col in self.text_columns:
            if col in df.columns:
                print(f"Cleaning text column: {col}")
                df[col] = df[col].apply(self._clean_text)
            else:
                print(f"Warning: Column '{col}' not found in DataFrame.")
        print("Text cleaning complete.")
        return df
