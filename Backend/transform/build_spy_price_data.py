from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf


# ==========================================================
# STANDARD PATHS AND CONFIG
# ==========================================================
from config import (
    SPY_DS_ROOT,
    FINAL_FILES_FOLDER,
    RUN_META_PATH_SPY
)

OUTPUT_PATH = FINAL_FILES_FOLDER / "spy_prices.parquet"
RUN_META_PATH = RUN_META_PATH_SPY

# ---------------------------
# Utilities
# ---------------------------


def utc_now_str() -> str:
    return str(pd.Timestamp.now("UTC"))


def _flatten_after_reset_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance may return either:
    - regular single-level columns, or
    - MultiIndex columns even for a single ticker.

    After reset_index(), flatten any tuple-like columns so that:
    ('Date', '') -> 'Date'
    ('Adj Close', 'SPY') -> 'Adj Close'
    """
    out = df.reset_index()

    if isinstance(out.columns, pd.MultiIndex):
        flat_cols = []
        for col in out.columns:
            if isinstance(col, tuple):
                chosen = None
                for item in col:
                    if item is not None and str(item).strip() != "":
                        chosen = str(item)
                        break
                flat_cols.append(chosen if chosen is not None else "")
            else:
                flat_cols.append(str(col))
        out.columns = flat_cols
    else:
        out.columns = [str(c) for c in out.columns]

    return out


# ---------------------------
# Config
# ---------------------------

@dataclass
class BuildConfig:
    out_root: Path = SPY_DS_ROOT
    output_path: Path = OUTPUT_PATH
    run_meta_path: Path = RUN_META_PATH
    ticker: str = "SPY"
    start_date: str = "2013-01-01"
    end_date_inclusive: str = "2026-03-31"
    interval: str = "1d"
    compression: str = "zstd"
    overwrite: bool = True

    # retry logic
    yahoo_max_retries: int = 4
    yahoo_base_sleep: float = 3.0


# ---------------------------
# Yahoo download
# ---------------------------


def download_single_ticker_prices(
    ticker: str,
    start_date: str,
    end_date_inclusive: str,
    interval: str = "1d",
    max_retries: int = 4,
    base_sleep: float = 3.0,
) -> pd.DataFrame:
    """
    Download one ticker from Yahoo Finance and return the same core columns
    as the broader stock price pipeline:
    date, ticker, adj_close, volume, open, high, low, close

    Note: yfinance uses an exclusive end date, so we add one day to the
    requested inclusive end date.
    """
    wanted_cols = ["date", "ticker", "adj_close", "volume", "open", "high", "low", "close"]
    ticker = str(ticker).strip().upper()

    if ticker == "":
        raise ValueError("Ticker cannot be empty.")

    start_ts = pd.Timestamp(start_date)
    end_inclusive_ts = pd.Timestamp(end_date_inclusive)
    end_exclusive_ts = end_inclusive_ts + pd.Timedelta(days=1)

    last_err: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            df = yf.download(
                tickers=ticker,
                start=str(start_ts.date()),
                end=str(end_exclusive_ts.date()),
                interval=interval,
                auto_adjust=False,
                actions=False,
                threads=False,
                progress=False,
            )

            if df is None or df.empty:
                return pd.DataFrame(columns=wanted_cols)

            out = _flatten_after_reset_index(df)
            out.columns = [str(c).lower().replace(" ", "_") for c in out.columns]
            out = out.rename(columns={"adjclose": "adj_close"})

            # Extra guard in case yfinance names the index column something odd.
            if "date" not in out.columns:
                first_col = out.columns[0]
                out = out.rename(columns={first_col: "date"})

            for c in ["open", "high", "low", "close", "adj_close", "volume"]:
                if c not in out.columns:
                    out[c] = pd.NA

            out["ticker"] = ticker
            out = out[["date", "ticker", "adj_close", "volume", "open", "high", "low", "close"]]

            out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
            out["ticker"] = out["ticker"].astype(str).str.upper()
            out = out.dropna(subset=["date"]).copy()

            value_cols = ["open", "high", "low", "close", "adj_close", "volume"]
            for c in value_cols:
                out[c] = pd.to_numeric(out[c], errors="coerce")

            out = out.dropna(subset=value_cols, how="all").copy()
            out = out[
                (out["date"] >= start_ts)
                & (out["date"] <= end_inclusive_ts)
            ].copy()

            out = (
                out
                .drop_duplicates(subset=["ticker", "date"], keep="last")
                .sort_values(["ticker", "date"])
                .reset_index(drop=True)
            )

            return out

        except Exception as e:
            last_err = e
            sleep_s = base_sleep * (2 ** attempt)
            print(f"[spy] retry {attempt + 1}/{max_retries} after error: {e}")
            time.sleep(sleep_s)

    raise RuntimeError(f"SPY download failed after retries: {last_err}")


# ---------------------------
# Writer
# ---------------------------


def write_single_parquet(df: pd.DataFrame, output_path: Path, compression: str = "zstd") -> None:
    if df.empty:
        raise ValueError("No SPY price rows were returned from Yahoo Finance.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(output_path), compression=compression)


# ---------------------------
# Main builder
# ---------------------------


def build_spy_dataset(cfg: BuildConfig) -> pd.DataFrame:
    cfg.out_root.mkdir(parents=True, exist_ok=True)

    print(f"[spy] Writing to folder: {cfg.out_root}")
    print(f"[spy] Output parquet: {cfg.output_path}")
    print(f"[spy] Ticker: {cfg.ticker}")
    print(f"[spy] Date range: {cfg.start_date} to {cfg.end_date_inclusive}")
    print(f"[spy] Interval: {cfg.interval}")

    df = download_single_ticker_prices(
        ticker=cfg.ticker,
        start_date=cfg.start_date,
        end_date_inclusive=cfg.end_date_inclusive,
        interval=cfg.interval,
        max_retries=cfg.yahoo_max_retries,
        base_sleep=cfg.yahoo_base_sleep,
    )

    if cfg.overwrite and cfg.output_path.exists():
        cfg.output_path.unlink()

    write_single_parquet(df, cfg.output_path, compression=cfg.compression)

    run_meta = pd.DataFrame([
        {
            "ticker": cfg.ticker,
            "start_date": cfg.start_date,
            "end_date_inclusive": cfg.end_date_inclusive,
            "interval": cfg.interval,
            "compression": cfg.compression,
            "overwrite": cfg.overwrite,
            "rows": len(df),
            "min_date": str(df["date"].min().date()) if not df.empty else "",
            "max_date": str(df["date"].max().date()) if not df.empty else "",
            "asof_utc": utc_now_str(),
        }
    ])
    run_meta.to_csv(cfg.run_meta_path, index=False)

    print(f"[spy] Rows written: {len(df):,}")
    print(f"[spy] Min date: {df['date'].min().date()}")
    print(f"[spy] Max date: {df['date'].max().date()}")
    print(f"[spy] Parquet saved: {cfg.output_path}")
    print(f"[spy] Run meta saved: {cfg.run_meta_path}")

    return df


# ---------------------------
# Main
# ---------------------------


def main() -> None:
    cfg = BuildConfig(
        out_root=SPY_DS_ROOT,
        output_path=OUTPUT_PATH,
        run_meta_path=RUN_META_PATH,
        ticker="SPY",
        start_date="2013-01-01",
        end_date_inclusive="2026-03-31",
        interval="1d",
        compression="zstd",
        overwrite=True,
        yahoo_max_retries=4,
        yahoo_base_sleep=3.0,
    )

    build_spy_dataset(cfg)


if __name__ == "__main__":
    main()