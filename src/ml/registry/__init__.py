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
            known = ", ".join(sorted(self._makers)[:30])
            raise ValueError(
                f"[{self.name}] Unknown alias '{alias}'. Known: {known}..."
            )
        return self._makers[key](**kwargs)

    def meta(self, alias: str) -> dict:
        return self._meta.get((alias or "").lower(), {})

    def keys(self):
        return sorted(self._makers.keys())


# --- 4 registries (หนึ่งครั้งพอ) ---
model = Registry("model")
feature = Registry("feature")
reducer = Registry("reducer")
selector = Registry("selector")
train_tuning = Registry("train_tuning")
