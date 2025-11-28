from typing import Callable, Dict, Any


class Registry:
    def __init__(self, name: str):
        self.name = name
        self._makers: Dict[str, Callable[..., Any]] = {}
        self._meta: Dict[str, dict] = {}

    def register(self, *aliases: str, meta: dict | None = None):
        def deco(fn: Callable[..., Any]):
            for a in aliases:
                key = a.lower()
                if key in self._makers:
                    raise KeyError(f"[{self.name}] Duplicate alias: {key}")
                self._makers[key] = fn
                self._meta[key] = dict(meta or {})
            return fn

        return deco

    def create(self, alias: str, **kwargs):
        key = (alias or "").lower()
        if key not in self._makers:
            known = ", ".join(sorted(self._makers)[:1000])
            raise ValueError(
                f"[{self.name}] Unknown alias '{alias}'. Known: {known}..."
            )
        return self._makers[key](**kwargs)

    def meta(self, alias: str) -> dict:
        return self._meta.get((alias or "").lower(), {})

    def keys(self):
        return sorted(self._makers.keys())


def load_plugins(base_pkg, subfolder: str):
    import importlib
    import pkgutil

    pkg_name = f"{base_pkg}.{subfolder}"
    pkg = importlib.import_module(pkg_name)

    for _, name, _ in pkgutil.iter_modules(pkg.__path__):
        full_name = f"{pkg_name}.{name}"
        importlib.import_module(full_name)


extractor = Registry("extractor")
transformer = Registry("transformer")
loader = Registry("loader")
model = Registry("model")
feature = Registry("feature")
reducer = Registry("reducer")
selector = Registry("selector")
search = Registry("search")
