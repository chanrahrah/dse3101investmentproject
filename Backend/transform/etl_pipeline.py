# this script calls process_single_zip to extract and clean then upload_form13f to push into Neon PostgreSQL database.

from pathlib import Path
from transform.process_single_zip import process_single_zip

# ==========================================================
# PATHS
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2] #dse3101investmentproject
DATA_DIR = PROJECT_ROOT / "Datasets"

RAW_DIR = DATA_DIR / "13F_zip_files"
CLEAN_DIR = DATA_DIR / "13F_clean_files"
TEMP_DIR = PROJECT_ROOT / "temp"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================================
# Single-file ETL
# ==========================================================
def main(zip_path: Path):
    """Process a single zip file and save as parquet."""
    base_name = zip_path.stem.replace("_form13f", "")
    clean_filename = f"{base_name}_clean_df.parquet"
    clean_path = CLEAN_DIR / clean_filename

    # Skip if already processed
    if clean_path.exists():
        print(f"Skipping {zip_path.name} (already processed)")
        return

    print(f"Processing {zip_path.name}...")
    clean_df = process_single_zip(zip_path, TEMP_DIR)
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_parquet(clean_path, index=False)
    print(f"Saved {clean_filename}")


# ==========================================================
# Batch ETL
# ==========================================================
def run_batch():
    """Process all zip files in RAW_DIR."""
    zip_files = list(RAW_DIR.glob("*.zip"))
    print(f"Found {len(zip_files)} zip files.")

    for zip_path in zip_files:
        main(zip_path)

    print("Batch processing complete.")


# ==========================================================
# Test
# ==========================================================
if __name__ == "__main__":
    run_batch()
