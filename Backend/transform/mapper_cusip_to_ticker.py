# this script is used to map CUSIP to ticker for the 13F data, using the OpenFIGI API.
"""
CUSIP → Ticker Mapper for Form 13F Data
Requires: pip install pandas pyarrow requests
Add ticker column to combined_13f df by mapping CUSIPs via OpenFIGI API.
"""

import pandas as pd
from pathlib import Path
import requests
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ==========================================================
# CONFIG
# ==========================================================
from config import (
    DEBUG,
    OPENFIGI_KEY,
    OPENFIGI_URL
)

BATCH_SIZE    = 100   # max allowed by OpenFIGI per request
SLEEP         = 0.25  # seconds between batches (250 req/min with key = 0.24s min)


# ==========================================================
# Build cusip to ticker map and save as parquet
# ============================================================
def build_and_save_cusip_ticker_map(clean_dir: Path, mapper_dir: Path, openfigi_key: str) -> Path:
    """
    One-time function: collect all CUSIPs from clean parquets, call OpenFIGI API,
    and save the cusip->ticker map as a parquet file.

    Run this ONCE. After that, apply_filters_and_mapping_to_all_parquets() will
    read the saved file directly without calling the API again.

    Returns
    -------
    Path to the saved cusip_ticker_map.parquet
    """
    print("=== Step 1: Collecting unique CUSIPs ===")
    all_cusips = get_all_unique_cusips(clean_dir)

    print("\n=== Step 2: Mapping CUSIPs to tickers (one-time API call) ===")
    cusip_ticker_df = build_cusip_ticker_map(all_cusips, openfigi_key)
    print(f"Unique tickers obtained: {cusip_ticker_df['ticker'].nunique():,}")

    output_path = mapper_dir / "cusip_ticker_map.parquet"
    cusip_ticker_df.to_parquet(output_path, index=False)
    print(f"Saved cusip->ticker map to: {output_path}")

    # cusip_ticker_df.to_excel(mapper_dir / "cusip_ticker_map.xlsx", index=False)
    # print("save to excel for manual inspection")


# ==========================================================
# Helper functions for CUSIP to ticker mapping
# ==========================================================

def get_all_unique_cusips(clean_dir: Path) -> list:
    """Step 1: Collect all unique CUSIPs across all parquet files."""
    all_cusips = set()

    for parquet_file in clean_dir.glob("*.parquet"):
        df = pd.read_parquet(parquet_file, columns=["CUSIP"])  # only load CUSIP column, faster
        cusips = df["CUSIP"].dropna()
        cusips = cusips[~cusips.str.startswith("000")] # filter out invalid CUSIPs starting with 000
        all_cusips.update(cusips)

    print(f"Total unique CUSIPs across all files: {len(all_cusips)}")
    return list(all_cusips)


def build_cusip_ticker_map(cusips: list, openfigi_key: str) -> pd.DataFrame:
    """Step 2: Call API once for all CUSIPs, return a cusip->ticker mapping df."""
    cusip_ticker_df = map_cusip_to_ticker(cusips, openfigi_key)  # your existing API call
    print(f"Successfully mapped {cusip_ticker_df['ticker'].notna().sum()} / {len(cusip_ticker_df)} CUSIPs")
    return cusip_ticker_df  # columns: [CUSIP, ticker, security_type, name]


def map_cusip_to_ticker(cusips: list, openfigi_key: str, batch_size=100, sleep=0.25):

    df = pd.DataFrame({"CUSIP": cusips})

    # ── Step 1: Normalise CUSIPs ──────────────────────────────────────────────
    df["CUSIP"] = df["CUSIP"].astype(str).str.strip().str.upper()
    unique_CUSIPs = df["CUSIP"].dropna().unique().tolist()
    logger.info(f"Unique CUSIPs to map: {len(unique_CUSIPs):,}")

    headers = {
        "Content-Type": "application/json",
        "X-OPENFIGI-APIKEY": openfigi_key,  
    }

    # ── Step 2: Map via OpenFIGI ──────────────────────────────────────────────
    CUSIP_to_ticker = {}

    for i in range(0, len(unique_CUSIPs), batch_size):
        batch = unique_CUSIPs[i: i + batch_size]
        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]

        try:
            resp = requests.post(OPENFIGI_URL, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for CUSIP, item in zip(batch, data):
                ticker        = None
                security_type = None
                name          = None
                exchCode      = None

                if "data" in item and item["data"]:
                    # Prefer US equity listings
                    for entry in item["data"]:
                        if entry.get("exchCode") in ("US", "UN", "UA", "UW", "UR", "UP"):
                            ticker        = entry.get("ticker")
                            security_type = entry.get("securityType")
                            name          = entry.get("name")
                            exchCode      = entry.get("exchCode")
                            break
                    # Fall back to first result
                    if ticker is None:
                        ticker        = item["data"][0].get("ticker")
                        security_type = item["data"][0].get("securityType")
                        name          = item["data"][0].get("name")
                        exchCode      = item["data"][0].get("exchCode")

                CUSIP_to_ticker[CUSIP] = {
                    "ticker":        ticker,
                    "security_type": security_type,
                    "name":          name,
                    "exchCode":      exchCode,
                }

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed at batch {i // batch_size + 1}: {e}")
            for CUSIP in batch:
                CUSIP_to_ticker[CUSIP] = {"ticker": None, "security_type": None, "name": None, "exchCode": None}

        if (i // batch_size + 1) % 10 == 0:
            mapped = sum(1 for v in CUSIP_to_ticker.values() if v["ticker"])
            logger.info(f"  Progress: {min(i + batch_size, len(unique_CUSIPs)):,}/{len(unique_CUSIPs):,} | Mapped so far: {mapped:,}")

        time.sleep(sleep)

    # ── Step 3: Build mapping df ──────────────────────────────────────────────
    mapping_df = (
        pd.DataFrame.from_dict(CUSIP_to_ticker, orient="index")
        .reset_index()
        .rename(columns={"index": "CUSIP"})
    )

    # ── Step 4: Summary ───────────────────────────────────────────────────────
    total    = len(unique_CUSIPs)
    mapped   = mapping_df["ticker"].notna().sum()
    unmapped = total - mapped

    logger.info(f"Done. Mapped: {mapped:,}/{total:,} | Unmapped (NaN): {unmapped:,}")

    return mapping_df