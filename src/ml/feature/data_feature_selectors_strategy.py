# from abc import ABC, abstractmethod
# from typing import List, Union
# import pandas as pd
# from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

# class BaseFeatureExtractor(ABC):
#     @abstractmethod
#     def extract(self): 
#         pass

# class TfidfExtractor(BaseFeatureExtractor):
#     def __init__(self,**kwargs):
#         self.kw = kwargs
#     def extract(self): 
#         return TfidfVectorizer(**self.kw)
    
# class CountExtractor(BaseFeatureExtractor):
#     def __init__(self, **kwargs):
#         self.kw = kwargs
#     def build(self): 
#         return CountVectorizer(**self.kw)  

# class FeatureExtractor:
#     def __init__(self, strategies: Union[List[BaseFeatureExtractor], BaseFeatureExtractor]):
#         if isinstance(strategies, list):
#             self.strategies = strategies
#         else:
#             self.strategies = [strategies]

#     def set_strategies(self, strategies: Union[List[BaseFeatureExtractor], BaseFeatureExtractor]):
#         if isinstance(strategies, list):
#             self.strategies = strategies
#         else:
#             self.strategies = [strategies]

#     def extract(self, ):
#         for strategy in self.strategies:
#              = strategy.extract(df)
#             print("-" * 40)
#         return 


