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
#  PATHS for transform
# ==========================================================
RAW_DIR                 = DATA_DIR / "13F_zip_files"
CLEAN_DIR               = DATA_DIR / "13F_clean_files"
FILTERED_AND_MAPPED_DIR = DATA_DIR / "13F_filtered_and_mapped_files"
SCREENED_DIR            = DATA_DIR / "13F_filtered_and_mapped_and_screened_files"
MAPPER_DIR              = DATA_DIR / "others"
TEMP_DIR                = PROJECT_ROOT / "temp"

# ==========================================================
# PATHS for backtesting
# ==========================================================
FORM13F_FOLDER_PATH = DATA_DIR / "13F_filtered_and_mapped_and_screened_files"
PRICES_FILE_FULL    = DATA_DIR / "stock_price_data" / "stock_prices_all.parquet"
FINAL_FILES_FOLDER  = DATA_DIR / "final_files"

# ==========================================================
# ENSURE DIRECTORIES EXIST
# ==========================================================
for _dir in [
    RAW_DIR,
    CLEAN_DIR,
    FILTERED_AND_MAPPED_DIR,
    SCREENED_DIR,
    TEMP_DIR,
    FINAL_FILES_FOLDER,
]:
    _dir.mkdir(parents=True, exist_ok=True)


