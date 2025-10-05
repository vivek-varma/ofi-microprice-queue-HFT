#!/usr/bin/env python3
"""
Convert Databento DBN files (MBP-1 / Trades) -> lean CSVs, month-wide but memory-safe.

Outputs under <DATA_ROOT>/csv/:
  csv/mbp-1/mbp1_YYYYMMDD.csv       -> ts_recv_ns,bid_px_int,bid_sz,ask_px_int,ask_sz
  csv/trades/trades_YYYYMMDD.csv    -> ts_recv_ns,px_int,sz,side (1=buy,2=sell,0=unk)

Tips:
  - Run MBP-1 first (`--only mbp-1`), then Trades (`--only trades`) if you hit OOM.
  - Use `--start-day` to resume where you left off.
"""
from __future__ import annotations
import argparse, gc, pathlib, re
from datetime import date, timedelta
import pandas as pd
import databento as db

def day_iter(year: int, month: int, start_day: int = 1):
    d = date(year, month, start_day)
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

def store_to_df_one(dbn_path: pathlib.Path) -> pd.DataFrame:
    # Load ONE file, turn into df; move ts index to column if needed.
    store = db.DBNStore.from_file(dbn_path)
    df = store.to_df()
    if isinstance(df.index, pd.MultiIndex) or df.index.name in ("ts_recv", "ts_event"):
        df = df.reset_index()
    return df

def normalize_mbp1(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only: ts_recv_ns, bid_px_int, bid_sz, ask_px_int, ask_sz
    candidates = [
        {"ts_recv":"ts_recv_ns", "bid_px":"bid_px_int", "bid_sz":"bid_sz", "ask_px":"ask_px_int", "ask_sz":"ask_sz"},
        {"ts_event":"ts_recv_ns","bid_px":"bid_px_int", "bid_sz":"bid_sz", "ask_px":"ask_px_int", "ask_sz":"ask_sz"},
    ]
    for m in candidates:
        if all(k in df.columns for k in m):
            return df[list(m)].rename(columns=m)
    keep = [c for c in ("ts_recv","ts_event","bid_px","bid_sz","ask_px","ask_sz") if c in df.columns]
    if not keep:
        raise RuntimeError(f"MBP-1: unexpected columns {df.columns.tolist()}")
    return df[keep].rename(columns={"ts_recv":"ts_recv_ns","ts_event":"ts_recv_ns",
                                    "bid_px":"bid_px_int","ask_px":"ask_px_int"})

def _map_side_val(x) -> int:
    if pd.isna(x): return 0
    s = str(x).strip().lower()
    if s in ("1","b","buy","bid","buyer","aggressor_buy"): return 1
    if s in ("2","s","sell","ask","seller","aggressor_sell"): return 2
    return 0

def normalize_trades(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only: ts_recv_ns, px_int, sz, side
    candidates = [
        {"ts_recv":"ts_recv_ns", "px":"px_int", "size":"sz", "side":"side"},
        {"ts_recv":"ts_recv_ns", "px":"px_int", "sz":"sz",  "side":"side"},
        {"ts_recv":"ts_recv_ns", "px":"px_int", "size":"sz","aggressor":"side"},
    ]
    for m in candidates:
        if all(k in df.columns for k in m):
            out = df[list(m)].rename(columns=m)
            out["side"] = out["side"].map(_map_side_val)
            return out
    cols = df.columns
    ts = "ts_recv" if "ts_recv" in cols else ("ts_event" if "ts_event" in cols else None)
    px = "px" if "px" in cols else ("price" if "price" in cols else None)
    sz = "size" if "size" in cols else ("qty" if "qty" in cols else ("sz" if "sz" in cols else None))
    side = next((c for c in ("side","aggressor","action","liquidity_flag") if c in cols), None)
    keep = [c for c in (ts, px, sz, side) if c]
    if not keep:
        raise RuntimeError(f"Trades: unexpected columns {df.columns.tolist()}")
    out = df[keep].rename(columns={ts:"ts_recv_ns", px:"px_int", sz:"sz", side:"side"})
    out["side"] = out.get("side", 0).map(_map_side_val) if "side" in out.columns else 0
    return out

def convert_one(dbn_path: pathlib.Path, out_csv: pathlib.Path, kind: str):
    print(f"[convert] {dbn_path} -> {out_csv}")
    df = store_to_df_one(dbn_path)
    if kind == "mbp-1":
        out = normalize_mbp1(df)
    else:
        out = normalize_trades(df)
    out.to_csv(out_csv, index=False)
    # free memory promptly
    del df, out
    gc.collect()

def run(root: pathlib.Path, year: int, month: int, start_day: int, only: str | None):
    ensure_outdirs(root)
    for ymd in day_iter(year, month, start_day):
        did_any = False
        # MBP-1
        if only in (None, "mbp-1"):
            f_mbp = find_dbn_for_day(root, ymd, "mbp-1")
            if f_mbp:
                out = root / "csv" / "mbp-1" / f"mbp1_{ymd}.csv"
                if out.exists():
                    print(f"[keep] {out} exists")
                else:
                    try:
                        convert_one(f_mbp, out, "mbp-1")
                    except Exception as e:
                        print(f"[error] MBP-1 {ymd}: {e}")
                did_any = True
        # Trades
        if only in (None, "trades"):
            f_trd = find_dbn_for_day(root, ymd, "trades")
            if f_trd:
                out = root / "csv" / "trades" / f"trades_{ymd}.csv"
                if out.exists():
                    print(f"[keep] {out} exists")
                else:
                    try:
                        convert_one(f_trd, out, "trades")
                    except Exception as e:
                        print(f"[error] Trades {ymd}: {e}")
                did_any = True

        if not did_any:
            print(f"[skip] {ymd} — no .dbn.zst found under {root}/**")
        # small GC safety after each day
        gc.collect()

def parse_args():
    ap = argparse.ArgumentParser(description="Convert Databento DBN (MBP-1/Trades) to lean CSVs, month-wide.")
    ap.add_argument("--data-root", type=pathlib.Path, default=pathlib.Path("data"),
                    help="Root folder to scan (default: ./data)")
    ap.add_argument("--year", type=int, default=2023, help="Year (default: 2023)")
    ap.add_argument("--month", type=int, default=10, help="Month 1-12 (default: 10)")
    ap.add_argument("--start-day", type=int, default=1, help="Start day of month (resume support)")
    ap.add_argument("--only", choices=["mbp-1","trades"], help="Limit to one schema for lower memory")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(args.data_root, args.year, args.month, args.start_day, args.only)
