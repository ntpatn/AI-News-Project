from .base_visualization import BaseUniViz
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Union


class HistogramUniViz(BaseUniViz):
    def __init__(self, sns_kwargs=None, plt_kwargs=None):
        self.sns_kwargs = sns_kwargs or {}
        self.plt_kwargs = plt_kwargs or {}

    def univiz(self, df: pd.DataFrame, feature: str):
        self.set_thai_font()

        plt.figure(figsize=self.plt_kwargs.get("figsize", (8, 4)))
        ax = sns.histplot(
            df[feature],
            **self.sns_kwargs,
        )

        title = self.plt_kwargs.get("title", f"Distribution of {feature}")
        xlabel = self.plt_kwargs.get("xlabel", feature)
        ylabel = self.plt_kwargs.get("ylabel", "Count")
        rotation = self.plt_kwargs.get("rotation", 0)
        fontsize = self.plt_kwargs.get("fontsize", 10)

        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=rotation)
        plt.tight_layout()

        # add count on each bin
        for p in ax.patches:
            height = p.get_height()
            if height > 0:
                ax.annotate(
                    f"{int(height)}",
                    (p.get_x() + p.get_width() / 2, height),
                    ha="center",
                    va="bottom",
                    fontsize=fontsize,
                )
        plt.show()


class BarplotUniViz(BaseUniViz):
    def __init__(self, sns_kwargs=None, plt_kwargs=None):
        self.sns_kwargs = sns_kwargs or {}
        self.plt_kwargs = plt_kwargs or {}

    def univiz(self, df: pd.DataFrame, feature: str):
        self.set_thai_font()
        value_counts = df[feature].value_counts().sort_index()
        df_category = pd.DataFrame(
            {"category": value_counts.index, "count": value_counts.values}
        )

        plt.figure(figsize=self.plt_kwargs.get("figsize", (8, 4)))
        ax = sns.barplot(x="category", y="count", data=df_category, **self.sns_kwargs)

        title = self.plt_kwargs.get("title", f"Category Count: {feature}")
        xlabel = self.plt_kwargs.get("xlabel", "Category")
        ylabel = self.plt_kwargs.get("ylabel", "Count")
        rotation = self.plt_kwargs.get("rotation", 0)
        fontsize = self.plt_kwargs.get("fontsize", 8)

        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=rotation)

        for p in ax.patches:
            height = p.get_height()
            if height > 0:
                ax.annotate(
                    f"{int(height)}",
                    (p.get_x() + p.get_width() / 2, height),
                    ha="center",
                    va="bottom",
                    fontsize=fontsize,
                )
        plt.show()


class UniVisualizer:
    def __init__(self, strategies: Union[List[BaseUniViz], BaseUniViz]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def set_strategies(self, strategies: Union[List[BaseUniViz], BaseUniViz]):
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

    def visualize(self, df: pd.DataFrame, feature: str, **kwargs):
        for strategy in self.strategies:
            strategy.univiz(df, feature, **kwargs)
            print("-" * 40)
