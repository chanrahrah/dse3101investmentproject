# this script runs cleaning, filtering and CUSIP to ticker mapping for all form13F data, and saves the cleaned combined df as parquet main file for project.

import logging
from pathlib import Path
import pandas as pd
import zipfile
import os
from Backend.transform.clean_all_form13f import run_batch
from Backend.transform.general_filter_form13f import get_combined_df, get_whitelist_ciks_list
from Backend.transform.mapper_cusip_to_ticker import map_cusip_to_ticker
from Backend.transform.apply_filters_and_mapping_form13f import build_and_save_cusip_ticker_map, apply_filters_and_mapping_to_all_parquets

# ==========================================================
# PATHS
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2] #dse3101investmentproject
DATA_DIR = PROJECT_ROOT / "Datasets"

RAW_DIR = DATA_DIR / "13F_zip_files"
CLEAN_DIR = DATA_DIR / "13F_clean_files"
FILTERED_AND_MAPPED_DIR = DATA_DIR / "13F_filtered_and_mapped_files"
TEMP_DIR = PROJECT_ROOT / "temp"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
FILTERED_AND_MAPPED_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================================
# CONFIG
# ==========================================================
WHITELIST_CIKS = []  # set of CIKs to include, or None for all

OPENFIGI_KEY  = "585a86ab-668f-41ee-a72d-25d77ea9b58d"     
OPENFIGI_URL  = "https://api.openfigi.com/v3/mapping"
BATCH_SIZE    = 100   # max allowed by OpenFIGI per request
SLEEP         = 0.25  # seconds between batches (250 req/min with key = 0.24s min)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ==========================================================
# MAIN
# ==========================================================
def main():
    # Step 0: Clean 
    # run_batch(RAW_DIR, CLEAN_DIR, TEMP_DIR)
    # Note: Uncomment to run this to process zip into clean parquet files.
    # Note: if you have already run this once and have the clean parquet files, you can skip this step to save time.

    # 0.5: Build cusip->ticker map and save as parquet 
    # build_and_save_cusip_ticker_map(CLEAN_DIR, FILTERED_AND_MAPPED_DIR, OPENFIGI_KEY)
    # Note: one-time step, run once to save the mapping file, then comment out to reuse the saved file and save time

    # Step 1: Get filtered list of institutions (whitelist_ciks)
    combined_df = get_combined_df(CLEAN_DIR)
    whitelist_ciks = get_whitelist_ciks_list(combined_df, min_aum=100_000_000, min_years=5, min_quarters_pct=0.80, aum_in_thousands=False)

    
    # Step 2: Apply all filters and map CUSIP to ticker for each quarter's clean parquet files, and save the final filtered + mapped data as separate parquet files. 
    apply_filters_and_mapping_to_all_parquets(CLEAN_DIR, FILTERED_AND_MAPPED_DIR, whitelist_ciks)

if __name__ == "__main__":
    main()