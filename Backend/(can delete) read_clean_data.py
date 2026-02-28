from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "Datasets"
CLEAN_DIR = DATA_DIR / "13F_clean_individual"


full_df = pd.read_parquet(CLEAN_DIR)
