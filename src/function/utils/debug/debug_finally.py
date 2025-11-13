from typing import Optional
import gc


def finalize_task(tag: str, obj: Optional[object] = None, debug_func=None):
    try:
        if obj is not None:
            del obj
        gc.collect()

        if callable(debug_func):
            debug_func(tag)
        else:
            print(f"[DEBUG] {tag} finalized (no debug func)")
    except Exception as e:
        print(f"[WARN] finalize_task({tag}) failed: {e}")
