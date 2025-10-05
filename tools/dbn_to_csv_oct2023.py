#!/usr/bin/env python3
"""
Low-memory converter: Databento DBN (MBP-1 / Trades) -> lean CSVs for October 2023.

Outputs under <DATA_ROOT>/csv/:
  csv/mbp-1/mbp1_YYYYMMDD.csv       -> ts_recv_ns,bid_px_int,bid_sz,ask_px_int,ask_sz
  csv/trades/trades_YYYYMMDD.csv    -> ts_recv_ns,px_int,sz,side (1=buy,2=sell,0=unk)
"""

from __future__ import annotations
import argparse, gc, pathlib, re
from datetime import date, timedelta
import pandas as pd
import databento as db

# -------- helpers --------

def day_iter(year: int, month: int, start_day: int = 1):
    d = date(year, month, start_day)
    # next month
    ny, nm = (year + (month == 12), (month % 12) + 1)
    end = date(ny, nm, 1)
    while d < end:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)

def ensure_outdirs(root: pathlib.Path):
    (root / "csv" / "mbp-1").mkdir(parents=True, exist_ok=True)
    (root / "csv" / "trades").mkdir(parents=True, exist_ok=True)

def find_dbn_for_day(root: pathlib.Path, ymd: str, kind: str) -> pathlib.Path | None:
    # case-insensitive “glbx-mdp3-YYYYMMDD.<kind>.dbn.zst”
    pat = re.compile(rf"(?i)glbx[-_]mdp3-{ymd}\.{re.escape(kind)}\.dbn\.zst$")
    for p in root.rglob("*.dbn.zst"):
        if pat.search(p.name):
            return p
    return None

def to_df_columns(store: db.DBNStore, cols: list[str]) -> pd.DataFrame:
    """
    Low-memory: request only specific columns from the store.
    Many Databento builds support 'columns=' for to_df(); if not, we fall back to full to_df().
    We also reset the time index if it's on the index.
    """
    try:
        df = store.to_df(columns=cols)  # preferred (column-pruned)
    except TypeError:
        # Column selection unsupported in this build -> fallback to full then subset
        df = store.to_df()
    # move time index to a column if needed
    if isinstance(df.index, pd.MultiIndex) or df.index.name in ("ts_recv", "ts_event"):
        df = df.reset_index()
    # subset (handles fallback path)
    keep = [c for c in cols if c in df.columns]
    df = df[keep]
    return df

def normalize_mbp1(df: pd.DataFrame) -> pd.DataFrame:
    # We want: ts_recv_ns, bid_px_int, bid_sz, ask_px_int, ask_sz
    # Accept common variants for ts column name
    if "ts_recv" in df.columns and "ts_recv_ns" not in df.columns:
        df = df.rename(columns={"ts_recv": "ts_recv_ns"})
    if "ts_event" in df.columns and "ts_recv_ns" not in df.columns:
        df = df.rename(columns={"ts_event": "ts_recv_ns"})
    # price columns
    if "bid_px" in df.columns and "bid_px_int" not in df.columns:
        df = df.rename(columns={"bid_px": "bid_px_int"})
    if "ask_px" in df.columns and "ask_px_int" not in df.columns:
        df = df.rename(columns={"ask_px": "ask_px_int"})
    # final narrow set
    cols = [c for c in ("ts_recv_ns","bid_px_int","bid_sz","ask_px_int","ask_sz") if c in df.columns]
    if len(cols) < 5:
        raise RuntimeError(f"MBP-1 column mismatch; got {df.columns.tolist()}")
    return df[cols]

def _map_side_val(x) -> int:
    if pd.isna(x): return 0
    s = str(x).strip().lower()
    if s in ("1","b","buy","bid","buyer","aggressor_buy"): return 1
    if s in ("2","s","sell","ask","seller","aggressor_sell"): return 2
    return 0

def normalize_trades(df: pd.DataFrame) -> pd.DataFrame:
    # We want: ts_recv_ns, px_int, sz, side (1/2/0)
    if "ts_recv" in df.columns and "ts_recv_ns" not in df.columns:
        df = df.rename(columns={"ts_recv": "ts_recv_ns"})
    if "ts_event" in df.columns and "ts_recv_ns" not in df.columns:
        df = df.rename(columns={"ts_event": "ts_recv_ns"})
    if "px" in df.columns and "px_int" not in df.columns:
        df = df.rename(columns={"px": "px_int"})
    if "size" in df.columns and "sz" not in df.columns:
        df = df.rename(columns={"size": "sz"})
    if "aggressor" in df.columns and "side" not in df.columns:
        df = df.rename(columns={"aggressor":"side"})
    if "side" in df.columns:
        df["side"] = df["side"].map(_map_side_val)
    else:
        df["side"] = 0
    cols = [c for c in ("ts_recv_ns","px_int","sz","side") if c in df.columns]
    if len(cols) < 4:
        raise RuntimeError(f"Trades column mismatch; got {df.columns.tolist()}")
    return df[cols]

def convert_one(dbn_path: pathlib.Path, out_csv: pathlib.Path, kind: str):
    print(f"[convert] {dbn_path} -> {out_csv}")
    store = db.DBNStore.from_file(dbn_path)
    if kind == "mbp-1":
        df = to_df_columns(store, ["ts_recv","ts_event","bid_px","bid_sz","ask_px","ask_sz"])
        out = normalize_mbp1(df)
    else:
        df = to_df_columns(store, ["ts_recv","ts_event","px","size","sz","side","aggressor"])
        out = normalize_trades(df)
    out.to_csv(out_csv, index=False)
    # free memory
    del store, df, out
    gc.collect()

def run(root: pathlib.Path, year: int, month: int, start_day: int, only: str | None):
    ensure_outdirs(root)
    for ymd in day_iter(year, month, start_day):
        did_any = False
        if only in (None, "mbp-1"):
            f = find_dbn_for_day(root, ymd, "mbp-1")
            if f:
                out = root / "csv" / "mbp-1" / f"mbp1_{ymd}.csv"
                if out.exists():
                    print(f"[keep] {out} exists")
                else:
                    try:
                        convert_one(f, out, "mbp-1")
                    except Exception as e:
                        print(f"[error] MBP-1 {ymd}: {e}")
                did_any = True
        if only in (None, "trades"):
            f = find_dbn_for_day(root, ymd, "trades")
            if f:
                out = root / "csv" / "trades" / f"trades_{ymd}.csv"
                if out.exists():
                    print(f"[keep] {out} exists")
                else:
                    try:
                        convert_one(f, out, "trades")
                    except Exception as e:
                        print(f"[error] Trades {ymd}: {e}")
                did_any = True
        if not did_any:
            print(f"[skip] {ymd} — no .dbn.zst found under {root}/**")
        gc.collect()

def parse_args():
    ap = argparse.ArgumentParser(description="Low-memory DBN (MBP-1/Trades) -> CSV converter for a month.")
    ap.add_argument("--data-root", type=pathlib.Path, default=pathlib.Path("data"),
                    help="Root folder to scan (default: ./data)")
    ap.add_argument("--year", type=int, default=2023)
    ap.add_argument("--month", type=int, default=10)
    ap.add_argument("--start-day", type=int, default=1, help="Resume from this day")
    ap.add_argument("--only", choices=["mbp-1","trades"], help="Limit to one schema")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(args.data_root, args.year, args.month, args.start_day, args.only)
