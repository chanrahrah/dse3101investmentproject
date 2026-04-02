import os
from pathlib import Path
import zipfile
from config import (
    DEBUG, 
    DATASET, 
    DOWNLOAD_DIR, 
    ZIP_FOLDER, 
    KAGGLE_SUBFOLDER, 
    KAGGLE_USERNAME,
    KAGGLE_KEY,
    TEMP_DIR,
    FINAL_FILES_FOLDER
    )
import kaggle

# ==========================================================
# VERSION HELPERS (DEBUG mode only)
# ==========================================================

from datetime import datetime, timezone

def _get_latest_kaggle_timestamp(dataset: str) -> str:
    """Returns the most recent file creation date as a proxy for dataset version."""
    files = kaggle.api.dataset_list_files(dataset)
    dates = [f.creationDate for f in files.files if f.creationDate]
    latest = max(dates)
    return str(latest)

def _get_local_timestamp(download_dir: Path) -> str:
    """Reads the locally saved timestamp. Returns empty string if not found."""
    ts_file = download_dir / ".kaggle_version"
    if ts_file.exists():
        return ts_file.read_text().strip()
    return ""

def _save_local_timestamp(download_dir: Path, timestamp: str):
    """Saves the latest timestamp locally."""
    ts_file = download_dir / ".kaggle_version"
    ts_file.write_text(timestamp)


# ==========================================================
# DIRECTORY HELPERS
# ==========================================================

def _ensure_extra_dirs():
    """Create pipeline dirs that are not part of the Kaggle dataset."""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_FILES_FOLDER.mkdir(parents=True, exist_ok=True)

# ==========================================================
# UNZIP HELPERS
# ==========================================================
def _unzip_dataset(download_dir: Path):
    """
    Unzips the downloaded Kaggle dataset zip file.
    The zip contains a 'Datasets' folder — extracts it to PROJECT_ROOT level.
    """
    zip_files = list(download_dir.glob("*.zip"))

    if not zip_files:
        print("No zip file found to extract.")
        return

    for zip_path in zip_files:
        print(f"Extracting {zip_path.name}...")
        with zipfile.ZipFile(zip_path, "r") as z:
            # Extract to PROJECT_ROOT (one level up from Datasets/)
            # so that Datasets/ lands at dse3101investmentproject/Datasets/
            z.extractall(download_dir.parent)
        
        zip_path.unlink()  # delete the zip after extracting
        print(f"Extracted and deleted {zip_path.name}")

# ==========================================================
# MAIN DOWNLOAD FUNCTION
# ==========================================================

def download_data_from_kaggle():
    """
    DEBUG=True  : Downloads ALL folders from Kaggle, only if a newer version exists.
                  Skips download if already on the latest version.
    DEBUG=False : Downloads only the 13F_zip_files folder.
                  Skips download if the folder already exists locally.
    """

    kaggle.api.authenticate()

    if DEBUG:
        latest_ts = _get_latest_kaggle_timestamp(DATASET)
        local_ts  = _get_local_timestamp(DOWNLOAD_DIR)

        if local_ts == latest_ts:
            print(f"Already on latest version ({latest_ts}). Skipping download.")
            _ensure_extra_dirs()
            return

        print(f"New version found ({latest_ts}). Downloading...")
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        kaggle.api.dataset_download_files(
            DATASET,
            path=str(DOWNLOAD_DIR),
            unzip=False,
            quiet=False,
        )

        print("\nDownload complete. Extracting...")
        _unzip_dataset(DOWNLOAD_DIR)             # ← unzip here

        _save_local_timestamp(DOWNLOAD_DIR, latest_ts)
        _ensure_extra_dirs()
        print(f"Done. Timestamp: {latest_ts}")

    else:
        # ── Production: only download 13F_zip_files ───────
        if ZIP_FOLDER.exists() and any(ZIP_FOLDER.iterdir()):
            print(f"'{KAGGLE_SUBFOLDER}' already exists. Skipping download.")
            _ensure_extra_dirs()
            return

        ZIP_FOLDER.mkdir(parents=True, exist_ok=True)
        print(f"PRODUCTION mode: downloading '{KAGGLE_SUBFOLDER}' from Kaggle ({DATASET})...")
        print(f"Saving to: {ZIP_FOLDER}\n")

        kaggle.api.dataset_download_file(
            DATASET,
            file_name=KAGGLE_SUBFOLDER,
            path=str(ZIP_FOLDER),
            quiet=False,
        )

        _ensure_extra_dirs()

        print(f"\nDownload complete.")
        print(f"Files available in: {ZIP_FOLDER}")


if __name__ == "__main__":
    download_data_from_kaggle()