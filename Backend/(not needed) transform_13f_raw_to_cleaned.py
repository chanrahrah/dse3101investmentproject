from pathlib import Path
import pandas as pd
import zipfile
import os

# ==========================================================
# PROJECT ROOT DETECTION
# ==========================================================

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "Datasets"
RAW_DIR = DATA_DIR / "13F_zip_files"
CLEAN_DIR = DATA_DIR / "13F_clean_individual"

CLEAN_DIR.mkdir(exist_ok=True)

# ==========================================================
# CORE TRANSFORMATION FUNCTION
# ==========================================================

def process_single_zip(zip_path: Path) -> pd.DataFrame:

    extract_path = RAW_DIR / "temp_extract"
    extract_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    submission = pd.read_csv(extract_path / "SUBMISSION.tsv", sep="\t")
    coverpage = pd.read_csv(extract_path / "COVERPAGE.tsv", sep="\t")
    summarypage = pd.read_csv(extract_path / "SUMMARYPAGE.tsv", sep="\t")
    infotable = pd.read_csv(extract_path / "INFOTABLE.tsv", sep="\t")

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

    submission = submission[
        submission["SUBMISSIONTYPE"].isin(["13F-HR", "13F-HR/A"])
    ]

    # Merge coverpage
    submission = submission.merge(
        coverpage[["ACCESSION_NUMBER", "FILINGMANAGER_NAME", "ISAMENDMENT"]],
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

    df.loc[df["PERIODOFREPORT"] < cutoff, "VALUE"] *= 1000
    df.loc[df["PERIODOFREPORT"] < cutoff, "TABLEVALUETOTAL"] *= 1000

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
        "ISAMENDMENT",
        "TABLEVALUETOTAL",
        "TABLEENTRYTOTAL",
        "ISCONFIDENTIALOMITTED",
        "NAMEOFISSUER",
        "CUSIP",
        "TITLEOFCLASS",
        "VALUE",
        "SSHPRNAMT",
        "weight"
    ]].copy()

    return clean_df

# ==========================================================
# LOOP THROUGH ALL ZIP FILES
# ==========================================================

def main():

    zip_files = list(RAW_DIR.glob("*.zip"))

    all_dfs = []

    for zip_path in zip_files:

        print(f"Processing {zip_path.name}...")

        clean_df = process_single_zip(zip_path)

        # Save individual cleaned file
        clean_filename = zip_path.stem.replace("_form13f", "") + "_clean_df.csv"
        clean_df.to_csv(CLEAN_DIR / clean_filename, index=False)

        all_dfs.append(clean_df)

    # Create master dataset
    master_df = pd.concat(all_dfs, ignore_index=True)

    master_df.to_parquet(DATA_DIR / "master_13f.parquet", index=False)

    print("All files processed successfully.")

if __name__ == "__main__":
    main()