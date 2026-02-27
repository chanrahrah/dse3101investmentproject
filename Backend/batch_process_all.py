from pathlib import Path
import pandas as pd
from transform.process_single_zip import process_single_zip
# from Backend.transform.process_single_zip import process_single_zip

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
        clean_filename = f"{base_name}_clean_df.csv"
        clean_path = CLEAN_DIR / clean_filename

        # Skip if already processed
        if clean_path.exists():
            print(f"Skipping {zip_path.name} (already processed)")
            continue

        print(f"Processing {zip_path.name}...")

        clean_df = process_single_zip(zip_path)

        # Save individual cleaned file
        clean_df.to_csv(clean_path, index=False)

        print(f"Saved cleaned file → {clean_filename}")

        # Append to master
        append_to_master(clean_df)

    print("Batch processing complete.")

# ==========================================================
# MASTER APPEND FUNCTION
# ==========================================================

def append_to_master(new_df: pd.DataFrame):

    if MASTER_PATH.exists():
        master_df = pd.read_parquet(MASTER_PATH)

        # Prevent duplicate (CIK, PERIODOFREPORT, CUSIP)
        combined = pd.concat([master_df, new_df], ignore_index=True)

        combined = combined.drop_duplicates(
            subset=["CIK", "PERIODOFREPORT", "CUSIP"],
            keep="last"
        )

    else:
        combined = new_df

    combined.to_parquet(MASTER_PATH, index=False)

    print("Master dataset updated.")

clean_df = process_single_zip(zip_path, TEMP_DIR)

# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    run_batch()

