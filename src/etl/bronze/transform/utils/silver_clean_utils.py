import re
import pandas as pd
from dateutil import parser
import pytz

NONE_VALUES = {
    "",
    " ",
    "-",
    "none",
    "None",
    "null",
    "NULL",
    "undefined",
    "Undefined",
    "N/A",
}


def normalize_timestamp(ts, default_tz="+0700"):
    """Return datetime aware object (TIMESTAMPTZ-ready)"""
    if ts in NONE_VALUES or pd.isna(ts):
        return None
    try:
        dt = parser.parse(str(ts))
        if not dt.tzinfo:
            # ถ้าไม่มี timezone -> ใส่ timezone ตาม default
            sign = 1 if default_tz.startswith("+") else -1
            hours = int(default_tz[1:3])
            minutes = int(default_tz[3:5])
            offset = sign * (hours * 60 + minutes)
            dt = dt.replace(tzinfo=pytz.FixedOffset(offset))
        return dt  # ✅ คืน datetime object ไม่ใช่ string
    except Exception:
        return None


def normalize_text(s):
    """Trim, remove HTML, and normalize None-like strings"""
    if s is None or pd.isna(s):
        return None
    s = str(s).strip()
    if s in NONE_VALUES:
        return None
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("\u200b", "").strip()


def normalize_url(u):
    """Validate and normalize URL, handle None-like values"""
    if u is None or pd.isna(u):
        return None
    u = str(u).strip()
    if u in NONE_VALUES:
        return None
    return u if u.lower().startswith("http") else None
