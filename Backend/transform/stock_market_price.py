from __future__ import annotations

import math
import shutil
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------
# Repo paths
# ---------------------------

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]   # script -> transform -> Backend -> project root
DATASETS_DIR = REPO_ROOT / "Datasets"

PRICES_DS_ROOT = DATASETS_DIR / "stock_price_files"
MANIFEST_PATH = PRICES_DS_ROOT / "_manifest.csv"
RUN_META_PATH = PRICES_DS_ROOT / "_run_meta.csv"

TICKER_SOURCE_PATH = DATASETS_DIR / "others" / "cusip_ticker_map.parquet"
TICKER_COL = "ticker"
SECURITY_TYPE_COL = "security_type"


# ---------------------------
# Ticker extraction
# ---------------------------

def load_tickers_from_cusip_map(
    parquet_path: Path,
    ticker_col: str,
    security_type_col: str,
    strict: bool = True,
) -> list[str]:
    if not parquet_path.exists():
        raise FileNotFoundError(f"CUSIP-ticker map parquet not found: {parquet_path}")

    cusip_ticker_df = pd.read_parquet(parquet_path, columns=[ticker_col, security_type_col])

    keep_types = {"Common Stock"}
    cusip_ticker_clean = cusip_ticker_df[
        cusip_ticker_df[security_type_col].isin(keep_types)
    ].copy()

    tickers = (
        cusip_ticker_clean[ticker_col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
    )

    if strict:
        tickers = tickers[
            (tickers != "")
            & (tickers.str.len().between(1, 5))
            & (tickers.str.match(r"^[A-Z]+$"))
        ]
    else:
        tickers = tickers[
            (tickers != "")
        ]

    unique_tickers = list(dict.fromkeys(tickers.tolist()))

    print(f"[tickers] Source file: {parquet_path}")
    print(f"[tickers] Security types kept: {sorted(keep_types)}")
    print(f"[tickers] Unique tickers after filter: {len(unique_tickers)}")
    print("[tickers] Unique ticker list:")
    print(unique_tickers)

    return unique_tickers


# ---------------------------
# Utilities
# ---------------------------

def chunked(seq: list[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def wipe_dataset(root: Path) -> None:
    if root.exists():
        shutil.rmtree(root)


def _partition_dir(root: Path, year: int) -> Path:
    return root / f"year={int(year)}"


def utc_now_str() -> str:
    return str(pd.Timestamp.now("UTC"))


# ---------------------------
# Manifest helpers
# ---------------------------

MANIFEST_COLS = [
    "year",
    "ticker",
    "status",      # ok / retry / nodata
    "rows",
    "min_date",
    "max_date",
    "attempts",
    "last_error",
    "updated_at",
]


def load_manifest(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame(columns=MANIFEST_COLS)

        for c in MANIFEST_COLS:
            if c not in df.columns:
                df[c] = pd.NA

        return df[MANIFEST_COLS].copy()

    return pd.DataFrame(columns=MANIFEST_COLS)


def save_manifest(df: pd.DataFrame, path: Path) -> None:
    if df.empty:
        pd.DataFrame(columns=MANIFEST_COLS).to_csv(path, index=False)
        return

    out = df.copy()
    out = out[MANIFEST_COLS].sort_values(["year", "ticker", "updated_at"]).reset_index(drop=True)
    out.to_csv(path, index=False)


def upsert_manifest(manifest: pd.DataFrame, new_rows: list[dict]) -> pd.DataFrame:
    if not new_rows:
        return manifest

    new_df = pd.DataFrame(new_rows)

    for c in MANIFEST_COLS:
        if c not in new_df.columns:
            new_df[c] = pd.NA

    new_df = new_df[MANIFEST_COLS]

    if manifest.empty:
        combined = new_df.copy()
    else:
        combined = pd.concat([manifest, new_df], ignore_index=True)

    combined["updated_at"] = pd.to_datetime(combined["updated_at"], errors="coerce")
    combined = combined.sort_values(["year", "ticker", "updated_at"])
    combined = combined.drop_duplicates(subset=["year", "ticker"], keep="last")
    combined["updated_at"] = combined["updated_at"].astype(str)

    return combined.reset_index(drop=True)


def get_attempt_count(manifest: pd.DataFrame, year: int, ticker: str) -> int:
    if manifest.empty:
        return 0

    m = manifest[(manifest["year"] == year) & (manifest["ticker"] == ticker)]
    if m.empty:
        return 0

    val = m.iloc[-1].get("attempts", 0)
    try:
        return int(val)
    except Exception:
        return 0


def year_is_resolved(
    manifest: pd.DataFrame,
    year: int,
    tickers: list[str],
    max_attempts_per_ticker_year: int,
) -> bool:
    if not tickers:
        return True

    ticker_set = set(tickers)
    year_manifest = manifest[manifest["year"] == year].copy()

    if year_manifest.empty:
        return False

    resolved = set()

    for _, row in year_manifest.iterrows():
        t = str(row["ticker"]).upper()
        status = str(row["status"]) if not pd.isna(row["status"]) else ""
        attempts = row.get("attempts", 0)

        try:
            attempts = int(attempts)
        except Exception:
            attempts = 0

        if status == "ok":
            resolved.add(t)
        elif status == "nodata":
            resolved.add(t)
        elif status == "retry" and attempts >= max_attempts_per_ticker_year:
            resolved.add(t)

    return resolved == ticker_set


def infer_resume_start_year(
    manifest: pd.DataFrame,
    tickers: list[str],
    start_year: int,
    end_year: int,
    max_attempts_per_ticker_year: int,
) -> int:
    for year in range(start_year, end_year + 1):
        if not year_is_resolved(manifest, year, tickers, max_attempts_per_ticker_year):
            return year
    return end_year + 1


# ---------------------------
# Yahoo download
# ---------------------------

def yahoo_prices_chunk(
    tickers: list[str],
    start: str,
    end: str,
    interval: str = "1d",
    max_retries: int = 4,
    base_sleep: float = 3.0,
) -> pd.DataFrame:
    wanted_cols = ["date", "ticker", "adj_close", "volume", "open", "high", "low", "close"]

    if not tickers:
        return pd.DataFrame(columns=wanted_cols)

    last_err = None

    for attempt in range(max_retries):
        try:
            df = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=False,
                actions=False,
                group_by="ticker",
                threads=False,
                progress=False,
            )

            if df is None or df.empty:
                return pd.DataFrame(columns=wanted_cols)

            out_frames = []

            if isinstance(df.columns, pd.MultiIndex):
                available_tickers = set(str(x).upper() for x in df.columns.get_level_values(0))

                for t in tickers:
                    t = str(t).upper()
                    if t not in available_tickers:
                        continue

                    tmp = df[t].copy().reset_index()
                    tmp.columns = [str(c).lower().replace(" ", "_") for c in tmp.columns]
                    tmp = tmp.rename(columns={"adjclose": "adj_close"})

                    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
                        if c not in tmp.columns:
                            tmp[c] = pd.NA

                    tmp["ticker"] = t
                    tmp = tmp[["date", "ticker", "adj_close", "volume", "open", "high", "low", "close"]]
                    out_frames.append(tmp)

            else:
                tmp = df.copy().reset_index()
                tmp.columns = [str(c).lower().replace(" ", "_") for c in tmp.columns]
                tmp = tmp.rename(columns={"adjclose": "adj_close"})

                for c in ["open", "high", "low", "close", "adj_close", "volume"]:
                    if c not in tmp.columns:
                        tmp[c] = pd.NA

                tmp["ticker"] = str(tickers[0]).upper()
                tmp = tmp[["date", "ticker", "adj_close", "volume", "open", "high", "low", "close"]]
                out_frames.append(tmp)

            if not out_frames:
                return pd.DataFrame(columns=wanted_cols)

            out = pd.concat(out_frames, ignore_index=True)
            out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
            out["ticker"] = out["ticker"].astype(str).str.upper()

            out = out.dropna(subset=["date"]).copy()

            value_cols = ["open", "high", "low", "close", "adj_close", "volume"]
            for c in value_cols:
                if c not in out.columns:
                    out[c] = pd.NA

            out = out.dropna(subset=value_cols, how="all").copy()

            out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
            return out

        except Exception as e:
            last_err = e
            sleep_s = base_sleep * (2 ** attempt)
            print(f"[prices] retry {attempt + 1}/{max_retries} for chunk of {len(tickers)} tickers after error: {e}")
            time.sleep(sleep_s)

    print(f"[prices] chunk failed after retries: {last_err}")
    return pd.DataFrame(columns=wanted_cols)


# ---------------------------
# Chunk writer
# ---------------------------

def write_chunk_partition(
    df_chunk: pd.DataFrame,
    root: Path,
    year: int,
    chunk_id: int,
    compression: str = "zstd",
) -> int:
    if df_chunk.empty:
        return 0

    df_chunk = df_chunk.copy()
    df_chunk["date"] = pd.to_datetime(df_chunk["date"]).dt.tz_localize(None)
    df_chunk = df_chunk[df_chunk["date"].dt.year == year].copy()

    if df_chunk.empty:
        return 0

    df_chunk["ticker"] = df_chunk["ticker"].astype(str).str.upper()

    df_chunk = (
        df_chunk
        .drop_duplicates(subset=["ticker", "date"], keep="last")
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )

    part_dir = _partition_dir(root, year)
    part_dir.mkdir(parents=True, exist_ok=True)

    suffix = pd.Timestamp.now("UTC").strftime("%Y%m%d%H%M%S%f")
    file_path = part_dir / f"part-{year}-{chunk_id:06d}-{suffix}.parquet"

    table = pa.Table.from_pandas(df_chunk, preserve_index=False)
    pq.write_table(table, str(file_path), compression=compression)

    return len(df_chunk)


# ---------------------------
# Config
# ---------------------------

@dataclass
class BuildConfig:
    out_root: Path = PRICES_DS_ROOT
    start_year: int = 2013
    end_year: int = pd.Timestamp.now().year
    interval: str = "1d"
    chunk_size: int = 20
    mode: str = "fresh"              # "fresh" or "update"
    limit_tickers: Optional[int] = None
    strict_ticker_filter: bool = True

    # pacing
    sleep_between_chunks: float = 2.0
    sleep_between_years: float = 1.0

    # retry logic
    yahoo_max_retries_per_chunk: int = 4
    yahoo_base_sleep: float = 3.0
    max_attempts_per_ticker_year: int = 5


# ---------------------------
# Main builder
# ---------------------------

def build_prices_dataset(
    tickers: Iterable[str],
    cfg: BuildConfig,
) -> pd.DataFrame:
    tickers = [t.strip().upper() for t in tickers if t and isinstance(t, str)]
    tickers = list(dict.fromkeys(tickers))

    if isinstance(cfg.limit_tickers, int) and cfg.limit_tickers > 0:
        tickers = tickers[:cfg.limit_tickers]

    if cfg.mode == "fresh":
        wipe_dataset(cfg.out_root)

    cfg.out_root.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(MANIFEST_PATH)

    if cfg.mode == "update":
        inferred_start = infer_resume_start_year(
            manifest=manifest,
            tickers=tickers,
            start_year=cfg.start_year,
            end_year=cfg.end_year,
            max_attempts_per_ticker_year=cfg.max_attempts_per_ticker_year,
        )

        if inferred_start > cfg.end_year:
            print("[prices] all requested years already resolved")
            return manifest

        if inferred_start != cfg.start_year:
            print(f"[prices] update resume: shifting start_year from {cfg.start_year} to {inferred_start}")
            cfg = replace(cfg, start_year=inferred_start)

    print(f"[prices] Writing to: {cfg.out_root}")
    print(f"[prices] Tickers: {len(tickers)}")
    print(f"[prices] Chunk size: {cfg.chunk_size}")
    print(f"[prices] Mode: {cfg.mode}")
    print(f"[prices] Years: {cfg.start_year} to {cfg.end_year}")

    for year in range(cfg.start_year, cfg.end_year + 1):
        year_start = f"{year}-01-01"
        year_end = f"{year + 1}-01-01"

        print(f"\n[prices] Year {year} ...")

        if year_is_resolved(manifest, year, tickers, cfg.max_attempts_per_ticker_year):
            print(f"[prices]   year {year}: already resolved")
            continue

        year_manifest = manifest[manifest["year"] == year].copy() if not manifest.empty else pd.DataFrame(columns=MANIFEST_COLS)

        done_ok = set(
            year_manifest.loc[year_manifest["status"] == "ok", "ticker"].astype(str).str.upper()
        ) if not year_manifest.empty else set()

        done_nodata = set(
            year_manifest.loc[year_manifest["status"] == "nodata", "ticker"].astype(str).str.upper()
        ) if not year_manifest.empty else set()

        exhausted_retry = set()
        if not year_manifest.empty:
            tmp = year_manifest.copy()
            tmp["attempts"] = pd.to_numeric(tmp["attempts"], errors="coerce").fillna(0).astype(int)
            exhausted_retry = set(
                tmp.loc[
                    (tmp["status"] == "retry") & (tmp["attempts"] >= cfg.max_attempts_per_ticker_year),
                    "ticker"
                ].astype(str).str.upper()
            )

        skip_tickers = done_ok | done_nodata | exhausted_retry
        todo_tickers = [t for t in tickers if t not in skip_tickers]

        if not todo_tickers:
            print(f"[prices]   year {year}: no remaining retryable tickers")
            continue

        total_chunks = math.ceil(len(todo_tickers) / cfg.chunk_size)
        print(f"[prices]   remaining tickers: {len(todo_tickers)}")
        print(f"[prices]   chunks to run: {total_chunks}")

        for chunk_id, chunk in enumerate(chunked(todo_tickers, cfg.chunk_size), start=1):
            now_str = utc_now_str()

            try:
                df_chunk = yahoo_prices_chunk(
                    chunk,
                    start=year_start,
                    end=year_end,
                    interval=cfg.interval,
                    max_retries=cfg.yahoo_max_retries_per_chunk,
                    base_sleep=cfg.yahoo_base_sleep,
                )

                if not df_chunk.empty:
                    df_chunk = df_chunk[df_chunk["date"].dt.year == year].copy()

                returned_tickers = set()
                if not df_chunk.empty:
                    returned_tickers = set(df_chunk["ticker"].astype(str).str.upper().unique().tolist())

                missing_tickers = [t for t in chunk if t not in returned_tickers]

                if returned_tickers:
                    df_ok = df_chunk[df_chunk["ticker"].isin(returned_tickers)].copy()
                    rows_written = write_chunk_partition(df_ok, cfg.out_root, year, chunk_id)
                    print(f"[prices]   wrote {rows_written:,} rows for year {year} chunk {chunk_id}")
                else:
                    print(f"[prices]   no rows returned for year {year} chunk {chunk_id}")

                new_rows = []

                if returned_tickers:
                    value_cols = ["open", "high", "low", "close", "adj_close", "volume"]

                    for t, sub in df_chunk.groupby("ticker"):
                        t = str(t).upper()
                        sub = sub.dropna(subset=value_cols, how="all")
                        if sub.empty:
                            continue

                        prev_attempts = get_attempt_count(manifest, year, t)

                        new_rows.append({
                            "year": year,
                            "ticker": t,
                            "status": "ok",
                            "rows": len(sub),
                            "min_date": str(sub["date"].min().date()),
                            "max_date": str(sub["date"].max().date()),
                            "attempts": prev_attempts + 1,
                            "last_error": "",
                            "updated_at": now_str,
                        })

                for t in missing_tickers:
                    prev_attempts = get_attempt_count(manifest, year, t)

                    new_rows.append({
                        "year": year,
                        "ticker": t,
                        "status": "nodata",
                        "rows": 0,
                        "min_date": None,
                        "max_date": None,
                        "attempts": prev_attempts + 1,
                        "last_error": "no_price_data_for_full_year",
                        "updated_at": now_str,
                    })

                manifest = upsert_manifest(manifest, new_rows)
                save_manifest(manifest, MANIFEST_PATH)

            except Exception as e:
                err = str(e)
                print(f"[prices]   chunk failed in year {year}, chunk {chunk_id}: {err}")

                fail_rows = []
                for t in chunk:
                    prev_attempts = get_attempt_count(manifest, year, t)
                    attempts = prev_attempts + 1

                    status = "retry"
                    if attempts >= cfg.max_attempts_per_ticker_year:
                        status = "nodata"

                    fail_rows.append({
                        "year": year,
                        "ticker": t,
                        "status": status,
                        "rows": 0,
                        "min_date": None,
                        "max_date": None,
                        "attempts": attempts,
                        "last_error": err,
                        "updated_at": now_str,
                    })

                manifest = upsert_manifest(manifest, fail_rows)
                save_manifest(manifest, MANIFEST_PATH)

            print(f"[prices]   year {year}: chunk {chunk_id}/{total_chunks} done")
            time.sleep(cfg.sleep_between_chunks)

        year_manifest = manifest[manifest["year"] == year].copy()
        ok_count = int((year_manifest["status"] == "ok").sum())
        retry_count = int((year_manifest["status"] == "retry").sum())
        nodata_count = int((year_manifest["status"] == "nodata").sum())

        print(
            f"[prices]   year {year} summary: "
            f"ok={ok_count:,} retry={retry_count:,} nodata={nodata_count:,}"
        )

        time.sleep(cfg.sleep_between_years)

    meta = pd.DataFrame([{
        "mode": cfg.mode,
        "start_year": cfg.start_year,
        "end_year": cfg.end_year,
        "interval": cfg.interval,
        "chunk_size": cfg.chunk_size,
        "limit_tickers": cfg.limit_tickers if cfg.limit_tickers is not None else "",
        "strict_ticker_filter": cfg.strict_ticker_filter,
        "sleep_between_chunks": cfg.sleep_between_chunks,
        "sleep_between_years": cfg.sleep_between_years,
        "yahoo_max_retries_per_chunk": cfg.yahoo_max_retries_per_chunk,
        "yahoo_base_sleep": cfg.yahoo_base_sleep,
        "max_attempts_per_ticker_year": cfg.max_attempts_per_ticker_year,
        "asof_utc": utc_now_str(),
    }])
    meta.to_csv(RUN_META_PATH, index=False)

    print("\n[prices] Done.")
    print(f"[prices] Manifest: {MANIFEST_PATH}")
    print(f"[prices] Run meta: {RUN_META_PATH}")

    return manifest


# ---------------------------
# Progress report helper
# ---------------------------

def print_progress_summary(manifest_path: Path = MANIFEST_PATH) -> None:
    if not manifest_path.exists():
        print("[prices] no manifest yet")
        return

    m = pd.read_csv(manifest_path)
    if m.empty:
        print("[prices] manifest is empty")
        return

    summary = (
        m.groupby(["year", "status"])
         .size()
         .unstack(fill_value=0)
         .reset_index()
         .sort_values("year")
    )
    print(summary.to_string(index=False))


# ---------------------------
# Main
# ---------------------------

def main() -> None:
    current_year = pd.Timestamp.now().year

    tickers = load_tickers_from_cusip_map(
        parquet_path=TICKER_SOURCE_PATH,
        ticker_col=TICKER_COL,
        security_type_col=SECURITY_TYPE_COL,
        strict=False,
    )

    cfg = BuildConfig(
        out_root=PRICES_DS_ROOT,
        start_year=2013,
        end_year=current_year,
        interval="1d",
        chunk_size=20,
        mode="fresh",
        limit_tickers=None,
        strict_ticker_filter=False,
        sleep_between_chunks=2.0,
        sleep_between_years=1.0,
        yahoo_max_retries_per_chunk=4,
        yahoo_base_sleep=3.0,
        max_attempts_per_ticker_year=5,
    )

    build_prices_dataset(tickers, cfg)
    print_progress_summary(MANIFEST_PATH)


if __name__ == "__main__":
    main()