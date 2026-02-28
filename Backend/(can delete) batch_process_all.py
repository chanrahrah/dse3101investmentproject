from pathlib import Path
import pandas as pd
from transform.process_single_zip import process_single_zip

# ==========================================================
# PATHS
# ==========================================================

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "Datasets"
RAW_DIR = DATA_DIR / "13F_zip_files"
CLEAN_DIR = DATA_DIR / "13F_clean_individual"
CLEAN_DIR.mkdir(exist_ok=True)
TEMP_DIR = RAW_DIR

# ==========================================================
# MAIN BATCH FUNCTION
# ==========================================================

def run_batch():

    zip_files = list(RAW_DIR.glob("*.zip"))

    print(f"Found {len(zip_files)} zip files.")

    for zip_path in zip_files:

        base_name = zip_path.stem.replace("_form13f", "")
        clean_filename = f"{base_name}_clean_df.parquet"
        clean_path = CLEAN_DIR / clean_filename

        # Skip if already processed
        if clean_path.exists():
            print(f"Skipping {zip_path.name} (already processed)")
            continue

        print(f"Processing {zip_path.name}...")

        clean_df = process_single_zip(zip_path, TEMP_DIR)
        clean_df.to_parquet(clean_path, index=False)

        print(f"Saved {clean_filename}")

    print("Batch processing complete.")

if __name__ == "__main__":
    run_batch()

