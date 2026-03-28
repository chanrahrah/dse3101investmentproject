import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def run_light_heterogeneity_screen(
    input_dir: Path,
    output_dir: Path,
    mapper_dir: Path,   # <-- use Datasets/others
    threshold: int = 400
):
    """
    Filters institutions with avg_unique_stocks < threshold
    Saves:
    1. CSV of valid CIKs (to mapper_dir)
    2. Filtered parquet files with suffix '_and_screened'
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    mapper_dir.mkdir(parents=True, exist_ok=True)

    results_common_us = []

    logger.info("Step 3A: Computing institution-level statistics...")

    # ============================
    # PASS 1: Compute stats
    # ============================
    for file in input_dir.glob("*.parquet"):

        if file.name == "cusip_ticker_map.parquet":
            continue

        logger.info(f"Processing file for stats: {file.name}")

        df = pd.read_parquet(file)

        common_us_stock = df[
            (df["security_type"] == "Common Stock") &
            (df["exchCode"] == "US")
        ]

        summary = (
            common_us_stock
            .groupby(["CIK", "PERIODOFREPORT"])
            .agg(
                num_unique_stocks=("CUSIP", "nunique")
            )
            .reset_index()
        )

        results_common_us.append(summary)

    df_common_us = pd.concat(results_common_us, ignore_index=True)

    inst_level = (
        df_common_us
        .groupby("CIK")
        .agg(
            avg_unique_stocks=("num_unique_stocks", "mean"),
            num_quarters=("PERIODOFREPORT", "nunique")
        )
        .reset_index()
    )

    # ============================
    # FILTER CIKs
    # ============================
    screened_inst = inst_level[inst_level["avg_unique_stocks"] < threshold]
    cik_list = screened_inst["CIK"].to_frame()

    # Save CSV to Datasets/others
    cik_csv_path = mapper_dir / "light_screening_institutions.parquet"
    cik_list.to_parquet(cik_csv_path, index=False)

    logger.info(f"Saved screened CIK list to: {cik_csv_path}")
    logger.info(f"Remaining institutions: {len(cik_list)}")

    cik_set = set(screened_inst["CIK"])

    # ============================
    # PASS 2: Filter parquet files
    # ============================
    logger.info("Step 3B: Filtering parquet files...")

    for file in input_dir.glob("*.parquet"):

        if file.name == "cusip_ticker_map.parquet":
            continue

        logger.info(f"Filtering file: {file.name}")

        df = pd.read_parquet(file)

        df_filtered = df[df["CIK"].isin(cik_set)]

        # new filename
        new_name = file.stem + "_and_screened.parquet"
        output_path = output_dir / new_name

        df_filtered.to_parquet(output_path, index=False)

    logger.info("Light heterogeneity screening completed.")