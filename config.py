import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ==========================================================
# ENVIRONMENT
# ==========================================================
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# ==========================================================
# SECRETS
# ==========================================================
KAGGLE_USERNAME  = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY       = os.getenv("KAGGLE_KEY")
OPENFIGI_KEY = os.getenv("OPENFIGI_KEY")
OPENFIGI_URL     = os.getenv("OPENFIGI_URL")

# ==========================================================
# KAGGLE
# ==========================================================
DATASET          = "tayyihong/dse3101-ver-22-mar"
KAGGLE_SUBFOLDER = "13F_zip_files"                # subfolder name on Kaggle

# ==========================================================
# KAGGLE — 13F ZIP FILES ONLY DATASET (DEBUG=False / Production)
# ==========================================================
DATASET_13F_ZIP  = "chanrahrah/13f-zip-files"  

# ==========================================================
# ROOT & DATA DIRECTORIES
# ==========================================================
PROJECT_ROOT = Path(__file__).resolve().parents[0]  # dse3101investmentproject
DATA_DIR     = PROJECT_ROOT / "Datasets"

# ==========================================================
# PATHS for KAGGLE DOWNLOAD
# ==========================================================
DOWNLOAD_DIR = DATA_DIR                   
ZIP_FOLDER   = DATA_DIR / "13F_zip_files" 

# ==========================================================
#  PATHS for transform form13f data
# ==========================================================
RAW_DIR                 = DATA_DIR / "13F_zip_files"
CLEAN_DIR               = DATA_DIR / "13F_clean_files"
FILTERED_AND_MAPPED_DIR = DATA_DIR / "13F_filtered_and_mapped_files"
SCREENED_DIR            = DATA_DIR / "13F_filtered_and_mapped_and_screened_files"
MAPPER_DIR              = DATA_DIR / "others"
TEMP_DIR                = PROJECT_ROOT / "temp"

# ==========================================================
# PATHS for transform stock price files
# ==========================================================
PRICES_DS_ROOT     = DATA_DIR / "stock_price_data"
MANIFEST_PATH      = PRICES_DS_ROOT / "_manifest.csv"
RUN_META_PATH_STOCK      = PRICES_DS_ROOT / "_run_meta.csv"
TICKER_SOURCE_PATH = MAPPER_DIR / "cusip_ticker_map.parquet"

# ==========================================================
# PATHS for transform SPY stock price files
# ==========================================================
SPY_DS_ROOT = DATA_DIR / "SPY_price_data"
SPY_DATA_DIR = DATA_DIR / "final_files" / "spy_prices_2013-01-01_to_2026-03-31.parquet"
RUN_META_PATH_SPY = SPY_DS_ROOT / "_run_meta.csv"

# ==========================================================
# PATHS for backtesting
# ==========================================================
FORM13F_FOLDER_PATH          = DATA_DIR / "13F_filtered_and_mapped_and_screened_files"
PRICES_FILE_FULL             = DATA_DIR / "stock_price_data" / "stock_prices_all.parquet"
FINAL_FILES_FOLDER           = DATA_DIR / "final_files"
BEST_INSTITUTION_RANKING_DIR = DATA_DIR / "best_instituition_ranking"




