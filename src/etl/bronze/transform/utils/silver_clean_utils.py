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
    import re
    import html

    """Aggressive text cleaner — remove HTML, entities, control chars, and symbol noise."""
    if s is None or pd.isna(s):
        return None

    s = str(s).strip()
    if s in NONE_VALUES:
        return None

    # 1️⃣ Decode HTML entities (&amp; -> &, &#8217; -> ’)
    s = html.unescape(s)

    # 2️⃣ Remove HTML tags
    s = re.sub(r"<[^>]+>", " ", s)

    # 3️⃣ Remove control and invisible chars
    s = re.sub(r"[\r\n\t\u200b\xa0]+", " ", s)

    # 4️⃣ Remove entity-like junk (&quot;, &apos;, etc.)
    s = re.sub(r"&[a-z]+;", " ", s)

    # 5️⃣ Remove symbol noise (keep letters, numbers, and sentence punctuations)
    s = re.sub(r"[•·●►▶▷☛→←↔⇨⇦■□★☆◆◇※→→©®™∞¶§]+", " ", s)
    s = re.sub(r"[“”‘’‛❛❜‚„‹›«»]", "'", s)  # normalize quotes
    s = re.sub(r"[‐-‒–—―]", "-", s)  # normalize dash

    # 6️⃣ Remove remaining non-word symbols except basic punctuations
    s = re.sub(r"[^A-Za-z0-9ก-๙\s.,!?%:;()'\-_/]", " ", s)

    # 7️⃣ Normalize spaces
    s = re.sub(r"\s{2,}", " ", s).strip()

    # 8️⃣ If after cleaning it’s empty → None
    if not s or s in NONE_VALUES:
        return None

    return s


def normalize_url(u):
    """Validate and normalize URL, handle None-like values"""
    if u is None or pd.isna(u):
        return None
    u = str(u).strip()
    if u in NONE_VALUES:
        return None
    return u if u.lower().startswith("http") else None


def extract_root_domain(url: str):
    from urllib.parse import urlparse

    if not url:
        return None
    try:
        domain = urlparse(url).netloc.replace("www.", "")
        parts = domain.split(".")
        if len(parts) >= 2:
            return parts[-2]
        return parts[0]
    except Exception:
        return None
