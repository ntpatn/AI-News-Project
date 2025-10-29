import pandas as pd
from .base_transform import BaseSilverCleaning
from src.etl.bronze.transform.utils.silver_clean_utils import (
    normalize_text,
    normalize_url,
    normalize_timestamp,
    NONE_VALUES,
)


class SilverCleaningCurrentsapi(BaseSilverCleaning):
    def silverCleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(list(NONE_VALUES), None)
        df.rename(
            columns={
                "image": "image_url",
                "source": "source_name",
                "published": "published_at",
            },
            inplace=True,
        )
        for col in [
            "author",
            "title",
            "description",
            "category",
            "language",
            "source_name",
        ]:
            if col in df.columns:
                df[col] = df[col].map(normalize_text)
        df["url"] = df["url"].map(normalize_url)
        if "language" in df.columns:
            df["language"] = df["language"].str.lower()
        df["published_at"] = df["published_at"].map(
            lambda x: normalize_timestamp(x, "+0700")
        )
        df["region"] = df.get("region", None)
        df["content"] = None
        df = df[df["url"].notna()].drop_duplicates(subset=["url"])
        return df


class SilverCleaningMediastack(BaseSilverCleaning):
    def silverCleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(list(NONE_VALUES), None)
        df.rename(
            columns={
                "image": "image_url",
                "country": "region",
                "source": "source_name",
            },
            inplace=True,
        )
        for col in [
            "author",
            "title",
            "description",
            "category",
            "language",
            "source_name",
        ]:
            if col in df.columns:
                df[col] = df[col].map(normalize_text)
        df["url"] = df["url"].map(normalize_url)
        if "language" in df.columns:
            df["language"] = df["language"].str.lower()
        df["published_at"] = df["published_at"].map(
            lambda x: normalize_timestamp(x, "+0700")
        )
        df["content"] = None
        df = df[df["url"].notna()].drop_duplicates(subset=["url"])
        return df


class SilverCleaningNewsapi(BaseSilverCleaning):
    def silverCleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(list(NONE_VALUES), None)
        df.rename(
            columns={
                "url_to_image": "image_url",
                "source_id": "source_id",
                "source_name": "source_name",
                "publishedAt": "published_at",
            },
            inplace=True,
        )
        for col in ["author", "title", "description", "source_name", "content"]:
            if col in df.columns:
                df[col] = df[col].map(normalize_text)
        df["content"] = df["content"].str.replace(r"\[\+\d+\s*chars\]", "", regex=True)
        df["url"] = df["url"].map(normalize_url)
        if "language" in df.columns:
            df["language"] = df["language"].str.lower()
        df["published_at"] = df["published_at"].map(
            lambda x: normalize_timestamp(x, "+0700")
        )
        if "region" not in df.columns:
            df["region"] = None
        df = df[df["url"].notna()].drop_duplicates(subset=["url"])
        return df
