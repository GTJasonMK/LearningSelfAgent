#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gold price fetch + normalize to CNY/g and output CSV.

Design goals:
- Support multiple sources (some may 403); keep source adapters isolated.
- Prefer direct CNY/g sources; otherwise convert from USD/oz using USD/CNY.
- Keep script usable both as module (for tests) and CLI.

This file is written early in the plan; later steps will run it.
"""

from __future__ import annotations

import csv
import dataclasses
import datetime as dt
import json
import math
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


OZ_TO_G = 31.1034768


@dataclasses.dataclass(frozen=True)
class Row:
    ts_utc: dt.datetime
    price_cny_per_g: float
    source: str


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_utc(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc)


def _parse_iso_date(s: str) -> dt.date:
    # Accept YYYY-MM-DD.
    return dt.date.fromisoformat(s.strip())


def _parse_float(x: object) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        v = float(s)
    except ValueError:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _http_get(url: str, timeout_s: int = 25, headers: Optional[Dict[str, str]] = None) -> bytes:
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "*/*",
    }
    if headers:
        hdrs.update(headers)

    req = urllib.request.Request(url, headers=hdrs, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read()


def _json_loads_bytes(b: bytes) -> object:
    # Try utf-8 first; fallback to latin-1 to avoid hard failure.
    try:
        return json.loads(b.decode("utf-8"))
    except UnicodeDecodeError:
        return json.loads(b.decode("latin-1"))


def _daterange_days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    step = dt.timedelta(days=1)
    while cur <= end:
        yield cur
        cur += step


def _last_n_days_range(n_days: int, end_utc: Optional[dt.datetime] = None) -> Tuple[dt.date, dt.date]:
    end_dt = _to_utc(end_utc or _utc_now())
    end_date = end_dt.date()
    start_date = end_date - dt.timedelta(days=n_days)
    return start_date, end_date


def _normalize_daily_rows(rows: Sequence[Row]) -> List[Row]:
    # De-duplicate by date (UTC date); keep latest timestamp if multiple.
    by_day: Dict[dt.date, Row] = {}
    for r in rows:
        day = _to_utc(r.ts_utc).date()
        prev = by_day.get(day)
        if prev is None or _to_utc(r.ts_utc) > _to_utc(prev.ts_utc):
            by_day[day] = r
    out = list(by_day.values())
    out.sort(key=lambda x: x.ts_utc)
    return out


def fetch_usdcny_exchangerate(host: str = "exchangerate.host") -> float:
    """Fetch latest USD/CNY spot-like rate.

    Note: host is configurable to allow switching sources if needed.
    """
    if host == "exchangerate.host":
        url = "https://api.exchangerate.host/latest?base=USD&symbols=CNY"
        data = _json_loads_bytes(_http_get(url))
        # Expected: {"rates": {"CNY": 7.2}, ...}
        rates = data.get("rates") if isinstance(data, dict) else None
        v = None
        if isinstance(rates, dict):
            v = _parse_float(rates.get("CNY"))
        if v is None:
            raise RuntimeError("Failed to parse USD/CNY from exchangerate.host")
        return v

    raise ValueError(f"Unsupported FX host: {host}")


def fetch_gold_usd_per_oz_stooq() -> List[Tuple[dt.date, float]]:
    """Fetch XAUUSD daily from Stooq (free CSV). Returns (date, close_usd_per_oz)."""
    # Stooq symbol for gold in USD: xauusd
    url = "https://stooq.com/q/d/l/?s=xauusd&i=d"
    b = _http_get(url)
    text = b.decode("utf-8", errors="replace")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines or not lines[0].lower().startswith("date"):
        raise RuntimeError("Unexpected Stooq CSV format")

    out: List[Tuple[dt.date, float]] = []
    rdr = csv.DictReader(lines)
    for row in rdr:
        d_s = (row.get("Date") or "").strip()
        c_s = row.get("Close")
        if not d_s:
            continue
        try:
            d = _parse_iso_date(d_s)
        except ValueError:
            continue
        close = _parse_float(c_s)
        if close is None:
            continue
        out.append((d, close))

    if not out:
        raise RuntimeError("No rows parsed from Stooq")

    out.sort(key=lambda x: x[0])
    return out


def convert_usd_per_oz_to_cny_per_g(usd_per_oz: float, usdcny: float) -> float:
    return (usd_per_oz * usdcny) / OZ_TO_G


def build_last_3_months_daily_cny_per_g(
    days_back: int = 92,
    fx_host: str = "exchangerate.host",
    source: str = "stooq_xauusd",
) -> List[Row]:
    """Build a daily series for the last ~3 months.

    This implementation uses:
    - Stooq XAUUSD daily close (USD/oz)
    - exchangerate.host USD/CNY latest

    Later plan steps may add additional sources; for now keep it robust.
    """
    start_d, end_d = _last_n_days_range(days_back)

    if source != "stooq_xauusd":
        raise ValueError(f"Unsupported source: {source}")

    usdcny = fetch_usdcny_exchangerate(host=fx_host)
    xau = fetch_gold_usd_per_oz_stooq()

    rows: List[Row] = []
    for d, usd_per_oz in xau:
        if d < start_d or d > end_d:
            continue
        price = convert_usd_per_oz_to_cny_per_g(usd_per_oz, usdcny)
        # Use end-of-day timestamp in UTC for the given date.
        ts = dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=dt.timezone.utc)
        rows.append(Row(ts_utc=ts, price_cny_per_g=price, source=f"{source}+{fx_host}"))

    if not rows:
        raise RuntimeError("No rows after filtering to requested date range")

    return _normalize_daily_rows(rows)


def write_csv(rows: Sequence[Row], out_path: str) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "price_cny_per_g", "source"])
        for r in rows:
            d = _to_utc(r.ts_utc).date().isoformat()
            w.writerow([d, f"{r.price_cny_per_g:.6f}", r.source])


def _parse_args(argv: Sequence[str]) -> Dict[str, object]:
    # Minimal arg parsing to avoid extra deps.
    out: Dict[str, object] = {
        "out": None,
        "days": 92,
        "fx_host": "exchangerate.host",
        "source": "stooq_xauusd",
    }
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-o", "--out"):
            i += 1
            out["out"] = argv[i] if i < len(argv) else None
        elif a in ("--days",):
            i += 1
            out["days"] = int(argv[i])
        elif a in ("--fx-host",):
            i += 1
            out["fx_host"] = argv[i]
        elif a in ("--source",):
            i += 1
            out["source"] = argv[i]
        else:
            raise SystemExit(f"Unknown arg: {a}")
        i += 1

    if not out["out"]:
        raise SystemExit("Missing required --out")
    return out


def main(argv: Sequence[str]) -> int:
    args = _parse_args(list(argv))
    out_path = str(args["out"])
    days = int(args["days"])
    fx_host = str(args["fx_host"])
    source = str(args["source"])

    rows = build_last_3_months_daily_cny_per_g(days_back=days, fx_host=fx_host, source=source)
    write_csv(rows, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
