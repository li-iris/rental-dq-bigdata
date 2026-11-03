"""纯 Python 业务规则：地址标准化、日期解析（含无年份日期推断）、租金口径判断。

这些函数不依赖 Spark，在 dwd_clean.py / dws_rent_roll.py 里注册成 UDF 使用，
同时可以直接用 unittest 测试（见 tests/test_rules.py）。
"""
from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

CONF_DIR = Path(__file__).resolve().parents[2] / "conf"

# 街道类型统一成缩写
SUFFIX_MAP = {
    "STREET": "ST", "STR": "ST", "ST": "ST",
    "ROAD": "RD", "RD": "RD",
    "AVENUE": "AVE", "AVE": "AVE", "AV": "AVE",
    "PARADE": "PDE", "PDE": "PDE",
    "LANE": "LN", "LN": "LN",
    "DRIVE": "DR", "DR": "DR",
    "PLACE": "PL", "PL": "PL",
    "CRESCENT": "CRES", "CRES": "CRES",
}


def load_token_fixes(path: Optional[Path] = None) -> dict:
    """读取街道名拼写纠错表 conf/street_token_fixes.csv（wrong_token,right_token）。"""
    path = path or CONF_DIR / "street_token_fixes.csv"
    fixes = {}
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                fixes[row["wrong_token"].strip().upper()] = row["right_token"].strip().upper()
    return fixes


_TOKEN_FIXES = load_token_fixes()

# 单元号写法：Unit 3/12、U3/12、3/12、Unit 3, 12、Room 2 12 ...
_UNIT_PREFIX = re.compile(r"^(?:UNIT|APT|APARTMENT|ROOM|RM|U)\s*", re.I)
_ADDR = re.compile(
    r"^(?:(?P<unit>[0-9]+[A-Z]?)\s*[/,\s]\s*)?(?P<num>[0-9]+[A-Z]?)\s+(?P<street>[A-Z][A-Z ]*?)\s+(?P<suffix>[A-Z]+)$"
)


def normalize_address(raw: Optional[str], fixes: Optional[dict] = None) -> Tuple[Optional[str], bool]:
    """把各种写法的地址统一成 property_key。

    返回 (property_key, parse_ok)。解析失败时 property_key 为 None，parse_ok=False，
    由 DQ 规则 W05 报出来，不硬猜。

    >>> normalize_address("Unit 3/12 Smith Street")
    ('3/12 SMITH ST', True)
    """
    if raw is None:
        return None, False
    fixes = _TOKEN_FIXES if fixes is None else fixes
    s = raw.upper().strip()
    s = re.sub(r"[.。]", " ", s)          # 去掉句点
    s = re.sub(r",\s*NSW.*$", "", s)           # 去掉州和邮编尾巴
    s = re.sub(r"\s+", " ", s).strip()
    s = _UNIT_PREFIX.sub("", s)
    m = _ADDR.match(s)
    if not m:
        return None, False
    suffix = SUFFIX_MAP.get(m.group("suffix"))
    if suffix is None:
        return None, False
    street_tokens = [fixes.get(t, t) for t in m.group("street").split()]
    street = " ".join(street_tokens)
    unit = m.group("unit")
    head = f"{unit}/{m.group('num')}" if unit else m.group("num")
    return f"{head} {street} {suffix}", True


_FULL_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y%m%d")
_NO_YEAR = re.compile(r"^\s*(\d{1,2})[./](\d{1,2})\s*$")


def parse_date(raw: Optional[str], anchor: Optional[str] = None, max_ahead_days: int = 60) -> Tuple[Optional[str], str]:
    """解析日期，返回 (yyyy-MM-dd 或 None, 状态)。

    状态：ok / empty / no_year_inferred / no_year_unresolved / invalid
    只写了"日.月"没有年份时，只有能给出规则才补年份：
    取离锚点日期最近、且不晚于 锚点 + max_ahead_days 的那一年；没有锚点就不猜。
    """
    if raw is None or str(raw).strip() == "":
        return None, "empty"
    s = str(raw).strip()
    for fmt in _FULL_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat(), "ok"
        except ValueError:
            pass
    m = _NO_YEAR.match(s)
    if m:
        if not anchor:
            return None, "no_year_unresolved"
        try:
            a = date.fromisoformat(anchor)
            day, month = int(m.group(1)), int(m.group(2))
            candidates = []
            for y in (a.year - 1, a.year, a.year + 1):
                try:
                    candidates.append(date(y, month, day))
                except ValueError:
                    continue
            ok = [c for c in candidates if c <= a + timedelta(days=max_ahead_days)]
            if not ok:
                return None, "no_year_unresolved"
            best = min(ok, key=lambda c: abs((c - a).days))
            return best.isoformat(), "no_year_inferred"
        except ValueError:
            return None, "no_year_unresolved"
    return None, "invalid"


def parse_amount(raw: Optional[str]) -> Optional[float]:
    """'$1,234.50' / '1234.5' / ' 600 ' -> float；解析不了返回 None。"""
    if raw is None:
        return None
    s = re.sub(r"[$,\s]|AUD|元", "", str(raw), flags=re.I)
    if s == "":
        return None
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def rent_basis(active_cnt: int, weekly_rent: Optional[float], threshold: float) -> str:
    """判断某房产某月的租金口径（老板规则：周租低于阈值算单间，否则算整套）。

    no_tenancy：当月没有有效租约
    room_rents：同时有多份租约，按单间分别收
    unclear：只有一份租约但租金缺失，不硬算
    unit_rent / room_rent：按阈值判断
    """
    if not active_cnt:
        return "no_tenancy"
    if active_cnt > 1:
        return "room_rents"
    if weekly_rent is None:
        return "unclear"
    return "unit_rent" if weekly_rent >= threshold else "room_rent"
