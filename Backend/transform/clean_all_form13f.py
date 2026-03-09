# this script extracts and cleans each quarter's form13f data, and then save the cleaned data as parquet files in the Datasets/13F_clean_files folder.
import logging
from pathlib import Path
import pandas as pd
import zipfile
import os
import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

import shutil
import gc

# ==========================================================
# Process single zip file
# ==========================================================
def process_single_zip(zip_path: Path, temp_dir: Path):
    # , whitelist_ciks: set) -> pd.DataFrame:    

    extract_path = temp_dir / "temp_extract"

    if extract_path.exists():
        shutil.rmtree(extract_path)
    extract_path.mkdir()

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    coverpage_path = next(extract_path.rglob("COVERPAGE.tsv"))
    infotable_path = next(extract_path.rglob("INFOTABLE.tsv"))
    submission_path = next(extract_path.rglob("SUBMISSION.tsv"))
    summarypage_path = next(extract_path.rglob("SUMMARYPAGE.tsv"))

    coverpage = pd.read_csv(coverpage_path, sep="\t")

    infotable = pd.read_csv(
        infotable_path,
        sep="\t",
        dtype={
            "CUSIP": "string",
            "SSHPRNTYPE": "string",
            "PUTCALL": "string",
            "TITLEOFCLASS": "string",
            "INVESTMENTDISCRETION": "string"
        }
    )

    submission = pd.read_csv(
        submission_path,
        sep="\t",
        dtype={
            "SUBMISSIONTYPE": "string",
            "CIK": "string"
        }
    )

    summarypage = pd.read_csv(
        summarypage_path,
        sep="\t",
        dtype={
            "ISCONFIDENTIALOMITTED": "string"
        }
    )

    # ------------------------------------------------------
    # DATE PARSING
    # ------------------------------------------------------

    submission["FILING_DATE"] = pd.to_datetime(
        submission["FILING_DATE"],
        format="%d-%b-%Y",
        errors="coerce"
    )

    submission["PERIODOFREPORT"] = pd.to_datetime(
        submission["PERIODOFREPORT"],
        format="%d-%b-%Y",
        errors="coerce"
    )
    # ------------------------------------------------------
    # FILTER SUBMISSION
    # ------------------------------------------------------

    submission = submission[submission["SUBMISSIONTYPE"].isin(["13F-HR", "13F-HR/A"])]

    # Filter institutions using whitelist
    # submission = submission[submission["CIK"].isin(whitelist_ciks)]

    # Merge coverpage
    submission = submission.merge(
        coverpage[["ACCESSION_NUMBER", "FILINGMANAGER_NAME"]],
        on="ACCESSION_NUMBER",
        how="left"
    )

    # Deduplicate amendments
    submission = submission.sort_values(
        ["CIK", "PERIODOFREPORT", "FILING_DATE", "ACCESSION_NUMBER"]
    )

    submission = (
        submission
        .groupby(["CIK", "PERIODOFREPORT"], as_index=False)
        .tail(1)
    )

    # Merge summary
    submission = submission.merge(
        summarypage[[
            "ACCESSION_NUMBER",
            "TABLEVALUETOTAL",
            "TABLEENTRYTOTAL",
            "ISCONFIDENTIALOMITTED"
        ]],
        on="ACCESSION_NUMBER",
        how="left"
    )

    # Merge infotable
    df = infotable.merge(
        submission,
        on="ACCESSION_NUMBER",
        how="inner"
    )

    # ------------------------------------------------------
    # INFOTABLE FILTERS
    # ------------------------------------------------------

    df["filing_delay_days"] = (df["FILING_DATE"] - df["PERIODOFREPORT"]).dt.days
    df = df[(df["filing_delay_days"] >= 0) & (df["filing_delay_days"] <= 90)]
    df = df[df["SSHPRNAMTTYPE"] == "SH"]
    df = df[df["PUTCALL"].isna()]
    df = df[df["TITLEOFCLASS"] == "COM"]
    df = df[df["INVESTMENTDISCRETION"] == "SOLE"]
    df = df[df["CUSIP"].notna()]
    df = df[df["CUSIP"] != "000000000"]

    # ------------------------------------------------------
    # UNIT SCALING
    # ------------------------------------------------------

    cutoff = pd.Timestamp("2023-01-03")

    df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce")
    df["TABLEVALUETOTAL"] = pd.to_numeric(df["TABLEVALUETOTAL"], errors="coerce")

    df.loc[df["FILING_DATE"] < cutoff, "VALUE"] *= 1000
    df.loc[df["FILING_DATE"] < cutoff, "TABLEVALUETOTAL"] *= 1000

    df["weight"] = df["VALUE"] / df["TABLEVALUETOTAL"]

    # ------------------------------------------------------
    # FINAL COLUMNS
    # ------------------------------------------------------

    clean_df = df[[
        "CIK",
        "FILINGMANAGER_NAME",
        "PERIODOFREPORT",
        "FILING_DATE",
        "SUBMISSIONTYPE",
        "TABLEVALUETOTAL",
        "TABLEENTRYTOTAL",
        "ISCONFIDENTIALOMITTED",
        "NAMEOFISSUER",
        "CUSIP",
        "VALUE",
        "SSHPRNAMT",
        "weight"
    ]].copy()

    logging.info(f"Processed {zip_path.name}: {len(clean_df):,} rows.")
    return clean_df


# ==========================================================
# Single-file ETL
# ==========================================================
def main(zip_path: Path, temp_dir: Path):
        #  , whitelist_ciks: set)

    logging.info(f"Processing {zip_path.name}")

    clean_df = process_single_zip(
        zip_path,
        temp_dir)
        # whitelist_ciks

    return clean_df


# ==========================================================
# Batch ETL
# =========================================================

def run_batch(raw_dir: Path, clean_dir: Path, temp_dir: Path): 
            #   , whitelist_ciks: set
    """
    Process all zip files in raw_dir:
      - Clean each zip
      - Save each cleaned DataFrame to Parquet immediately
    """

    zip_files = sorted(raw_dir.glob("*.zip"))
    logging.info(f"Found {len(zip_files)} zip files to process.")

    clean_dir.mkdir(parents=True, exist_ok=True)
    parquet_files = []

    for zip_path in zip_files:
        # Process the zip
        clean_df = main(zip_path, temp_dir
                        # , whitelist_ciks
                        )

        if clean_df is not None and len(clean_df) > 0:
            # Save immediately to parquet
            clean_filename = f"{zip_path.stem}_clean.parquet"
            clean_path = clean_dir / clean_filename
            clean_df.to_parquet(clean_path, index=False)
            parquet_files.append(str(clean_path))

            logging.info(f"Saved {clean_filename} ({len(clean_df):,} rows).")

        del clean_df
        gc.collect()

    logging.info("All individual zips processed.")


# # ==========================================================
# # Test
# # ==========================================================
# if __name__ == "__main__":
#     run_batch()
