# src/etl/silver/registry.py
import importlib

EXTRACTOR_REGISTRY = {}


def register_extractor(name: str):
    def decorator(cls):
        EXTRACTOR_REGISTRY[name] = cls
        return cls

    return decorator


def load_extractors():
    """Import extractors so their decorators execute"""
    importlib.import_module("src.etl.silver.extract.extractor_postgres")
