# this script runs cleaning, filtering and CUSIP to ticker mapping for all form13F data, and saves the cleaned combined df as parquet main file for project.
import logging
from pathlib import Path
import pandas as pd
import zipfile
import os
from Backend.transform.download_data_from_kaggle import download_data_from_kaggle
from Backend.transform.clean_all_form13f import run_batch
from Backend.transform.general_filter_form13f import get_combined_df, get_whitelist_ciks_list, build_and_save_whitelist_ciks
from Backend.transform.mapper_cusip_to_ticker import map_cusip_to_ticker, build_and_save_cusip_ticker_map
from Backend.transform.apply_filters_and_mapping_form13f import apply_filters_and_mapping_to_all_parquets
from Backend.transform.light_heterogeneity_screen import run_light_heterogeneity_screen

# ==========================================================
# STANDARD PATHS AND CONFIG
# ==========================================================
from config import (
    DEBUG,
    KAGGLE_KEY,
    KAGGLE_USERNAME,
    OPENFIGI_KEY,
    OPENFIGI_URL,
    RAW_DIR,
    CLEAN_DIR,
    FILTERED_AND_MAPPED_DIR,
    SCREENED_DIR,
    MAPPER_DIR,
    TEMP_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ==========================================================
# MAIN
# ==========================================================
def main():
    # Step 0: download data files from kaggle
    # if DEBUG is True, downloads everything from Kaggle into ./Datasets/
    # if DEBUG is False, downloads only the 13F_zip_files folder into ./Datasets/13F_zip_files/
    download_data_from_kaggle()

    if not DEBUG:
        # Only perform all the steps in production mode. 

        # Step 1: Clean zip files and convert to parquet files
        logger.info("=== Step 1: Unzip raw form13f files and convert to clean parquet files ===")
        run_batch(RAW_DIR, CLEAN_DIR, TEMP_DIR)

        # Step 2: Build whitelist_cik list to filter institutions
        logger.info("=== Step 2: Build whitelist of CIKs based on filters and save as parquet ===")
        build_and_save_whitelist_ciks(CLEAN_DIR, MAPPER_DIR)

        # Step 3: Build cusip to ticker map and save as parquet 
        logger.info("=== Step 3: Build cusip to ticker map using OpenFIGI API and save as parquet ===")
        build_and_save_cusip_ticker_map(CLEAN_DIR, MAPPER_DIR, OPENFIGI_KEY)

        # Step 4: Apply all filters and map CUSIP to ticker for each quarter's clean parquet files, and save the final filtered + mapped data as separate parquet files. 
        logger.info("=== Step 4: Apply filters and mapping to all parquets ===")
        apply_filters_and_mapping_to_all_parquets(CLEAN_DIR, FILTERED_AND_MAPPED_DIR, MAPPER_DIR)

        # Step 5: Light heterogeneity screening
        logger.info("=== Step 5: Run light heterogeneity screening ===")
        run_light_heterogeneity_screen(
            input_dir=FILTERED_AND_MAPPED_DIR,
            output_dir=SCREENED_DIR,
            mapper_dir=MAPPER_DIR,   
            threshold=400
        )

# Test
if __name__ == "__main__":
    main()