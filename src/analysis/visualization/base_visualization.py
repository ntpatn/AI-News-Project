from abc import ABC, abstractmethod
import pandas as pd
import matplotlib.pyplot as plt


class BaseUniViz(ABC):
    @staticmethod
    def set_thai_font():
        plt.rcParams["font.family"] = "Tahoma"
        plt.rcParams["axes.unicode_minus"] = False

    @abstractmethod
    def univiz(self, df: pd.DataFrame, feature: str):
        pass
