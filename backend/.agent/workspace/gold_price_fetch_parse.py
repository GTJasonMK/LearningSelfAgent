#!/usr/bin/env python3
# Fetch last ~3 months gold price series and convert to CNY/gram.
# Source currently expected: Stooq daily CSV XAUUSD (USD/oz).
# FX source: Stooq daily CSV USDCNY (CNY per USD).
# Output: JSON lines (for review) and CSV (date, price_cny_per_gram, xauusd_close_usd_per_oz, usdcny_close, sources).

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import sys
import urllib.request

OZ_TO_GRAM = 31.1034768


def utc_today() -> dt.date:
    # Use UTC date to align with task's UTC timestamp.
    return dt.datetime.utcnow().date()


def fetch_url_text(url: str, timeout_s: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LearningSelfAgent/1.0; +https://example.invalid)"
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = resp.read()
    # Stooq CSV is ASCII/UTF-8.
    return data.decode("utf-8", errors="replace")


def parse_stooq_daily_csv(text: str, want_cols: list[str]) -> list[dict]:
    # Stooq CSV header: Date,Open,High,Low,Close
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for r in reader:
        out = {}
        for c in want_cols:
            if c not in r:
                raise ValueError(f"Missing column {c} in CSV")
            out[c] = r[c]
        rows.append(out)
    return rows


def to_float(x: str) -> float:
    x = (x or "").strip()
    if x in ("", "-"):
        raise ValueError("empty numeric")
    return float(x)


def parse_date(x: str) -> dt.date:
    return dt.datetime.strptime(x.strip(), "%Y-%m-%d").date()


def build_index(rows: list[dict], key: str) -> dict:
    idx = {}
    for r in rows:
        idx[r[key]] = r
    return idx


def daterange_cutoff(days: int) -> dt.date:
    return utc_today() - dt.timedelta(days=days)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=92, help="Lookback days; ~3 months default 92")
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument(
        "--xauusd_url",
        default="https://stooq.com/q/d/l/?s=xauusd&i=d",
        help="Stooq daily CSV for XAUUSD",
    )
    ap.add_argument(
        "--usdcny_url",
        default="https://stooq.com/q/d/l/?s=usdcny&i=d",
        help="Stooq daily CSV for USDCNY",
    )
    args = ap.parse_args()

    cutoff = daterange_cutoff(args.days)

    xau_text = fetch_url_text(args.xauusd_url)
    cny_text = fetch_url_text(args.usdcny_url)

    xau_rows = parse_stooq_daily_csv(xau_text, ["Date", "Close"])
    cny_rows = parse_stooq_daily_csv(cny_text, ["Date", "Close"])

    cny_by_date = build_index(cny_rows, "Date")

    out = []
    for xr in xau_rows:
        d = parse_date(xr["Date"])
        if d < cutoff:
            continue
        date_s = xr["Date"]
        if date_s not in cny_by_date:
            # Skip if FX missing for that date.
            continue
        xau_close = to_float(xr["Close"])  # USD/oz
        usdcny_close = to_float(cny_by_date[date_s]["Close"])  # CNY/USD
        price_cny_per_gram = (xau_close * usdcny_close) / OZ_TO_GRAM
        out.append(
            {
                "date": date_s,
                "price_cny_per_gram": round(price_cny_per_gram, 6),
                "xauusd_close_usd_per_oz": xau_close,
                "usdcny_close": usdcny_close,
                "source_xauusd": args.xauusd_url,
                "source_usdcny": args.usdcny_url,
                "note": "Converted: (XAUUSD USD/oz * USDCNY CNY/USD) / 31.1034768 g/oz",
            }
        )

    out.sort(key=lambda r: r["date"])

    with open(args.out_json, "w", encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with open(args.out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "price_cny_per_gram",
                "xauusd_close_usd_per_oz",
                "usdcny_close",
                "source_xauusd",
                "source_usdcny",
                "note",
            ],
        )
        w.writeheader()
        for r in out:
            w.writerow(r)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
