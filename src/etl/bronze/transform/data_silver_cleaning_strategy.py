import pandas as pd
from .base_transform import BaseSilverCleaning
from src.etl.bronze.transform.utils.silver_clean_utils import (
    normalize_text,
    normalize_url,
    normalize_timestamp,
    extract_root_domain,
    NONE_VALUES,
)


class SilverCleaningCurrentsapi(BaseSilverCleaning):
    def silverCleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(list(NONE_VALUES), None)
        df.rename(
            columns={
                "image": "image_url",
                "source": "source_news",
                "published": "published_at",
            },
            inplace=True,
        )
        df["source_news"] = df["url"].map(extract_root_domain)
        for col in [
            "author",
            "title",
            "description",
            "category",
            "language",
            "source_news",
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
        df = df[df["url"].notna()].drop_duplicates(subset=["url"])

        return df


class SilverCleaningMediastack(BaseSilverCleaning):
    def silverCleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        df.replace(list(NONE_VALUES), None, inplace=True)
        df.rename(
            columns={
                "image": "image_url",
                "country": "region",
                "source": "source_news",
            },
            inplace=True,
        )

        cols_to_clean = [
            "author",
            "title",
            "description",
            "category",
            "language",
            "source_news",
        ]
        for col in [c for c in cols_to_clean if c in df.columns]:
            df[col] = df[col].map(normalize_text)

        # ✅ clean url
        for col in ["url", "image_url"]:
            if col in df.columns:
                df[col] = df[col].map(normalize_url)

        # ✅ normalize language
        if "language" in df.columns:
            df["language"] = df["language"].str.lower()

        # ✅ normalize timestamp
        if "published_at" in df.columns:
            df["published_at"] = df["published_at"].map(
                lambda x: normalize_timestamp(x, "+0700")
            )

        # ✅ drop null url & duplicates
        df.dropna(subset=["url"], inplace=True)
        df.drop_duplicates(subset=["url"], inplace=True)

        return df


class SilverCleaningNewsapi(BaseSilverCleaning):
    def silverCleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(list(NONE_VALUES), None)
        df.rename(
            columns={
                "url_to_image": "image_url",
                "source_id": "source_id",
                "source": "source_news",
                "publishedAt": "published_at",
            },
            inplace=True,
        )
        df["source_news"] = df["url"].map(extract_root_domain)
        for col in ["author", "title", "description", "source_news", "content"]:
            if col in df.columns:
                df[col] = df[col].map(normalize_text)
        for col in ["url", "image_url"]:
            if col in df.columns:
                df[col] = df[col].map(normalize_url)
        if "language" in df.columns:
            df["language"] = df["language"].str.lower()
        df["published_at"] = df["published_at"].map(
            lambda x: normalize_timestamp(x, "+0700")
        )
        if "region" not in df.columns:
            df["region"] = None
        df = df[df["url"].notna()].drop_duplicates(subset=["url"])
        return df
