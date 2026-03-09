# this script performs general filtering and produce a list of whitelist_ciks for 

from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def get_combined_df(clean_dir):
    all_quarters = []

    # loop through all parquet files
    for file in clean_dir.glob("*.parquet"):
        print("Processing:", file.name)

        df = pd.read_parquet(file)

        # keep latest filing per institution per quarter
        latest = (
            df.sort_values("FILING_DATE")
            .groupby(["CIK", "PERIODOFREPORT"], as_index=False)
            .last()
        )

        all_quarters.append(latest)

    # combine all quarters
    df_all = pd.concat(all_quarters, ignore_index=True)
    return df_all


def get_whitelist_ciks_list(
    df,
    min_aum=100_000_000,        # raw dollars 
    min_years=5,
    min_quarters_pct=0.80,      # filter 4: must file in ≥80% of quarters
    aum_in_thousands=False,      # set False if TABLEVALUETOTAL is raw dollars
):
    """
    Returns a set of CIKs passing all filters:
      1. Active for at least 5 years
      2. Still active in the latest normal quarter (on or before cutoff_recent)
      3. Average AUM >= $100M
      4. Filed in >= 80% of quarters within their active window
    """
    df = df.copy()

    # --- Prep ---
    df["PERIODOFREPORT"] = pd.to_datetime(df["PERIODOFREPORT"])

    # Remove abnormal future quarter (assumed to be 2025Q2)
    cutoff_recent = pd.Timestamp("2024-03-31")   # avoid 2025Q2 abnormal filings
    df = df[df["PERIODOFREPORT"] <= cutoff_recent]

    # Normalise AUM to dollars
    aum_multiplier = 1_000 if aum_in_thousands else 1

    # --- Build per-institution stats ---
    stats = (
        df.groupby("CIK")
        .agg(
            first_report=("PERIODOFREPORT", "min"),
            last_report=("PERIODOFREPORT", "max"),
            num_reports=("PERIODOFREPORT", "count"),
            avg_aum=("TABLEVALUETOTAL", "mean"),
        )
        .reset_index()
    )

    stats["avg_aum_dollars"] = stats["avg_aum"] * aum_multiplier
    stats["years_active"] = (
        stats["last_report"] - stats["first_report"]
    ).dt.days / 365

    # Total number of unique quarters in the full dataset (the "universe")
    all_quarters = df["PERIODOFREPORT"].nunique()

    # Filter 4: expected quarters = quarters that existed during their active window
    # More precise than using the global count
    quarter_set = sorted(df["PERIODOFREPORT"].unique())

    def quarters_in_window(first, last):
        return sum(1 for q in quarter_set if first <= q <= last)

    stats["possible_quarters"] = stats.apply(
        lambda r: quarters_in_window(r["first_report"], r["last_report"]), axis=1
    )
    stats["filing_pct"] = stats["num_reports"] / stats["possible_quarters"]

    # --- Apply filters ---

    # Filter 1: active >= 5 years
    f1 = stats["years_active"] >= min_years

    # Filter 2: last report is recent (still active)
    f2 = stats["last_report"] >= cutoff_recent

    # Filter 3: avg AUM >= $100M
    f3 = stats["avg_aum_dollars"] >= min_aum

    # Filter 4: filed in >= 80% of quarters in their window
    f4 = stats["filing_pct"] >= min_quarters_pct

    filtered = stats[f1 & f2 & f3 & f4]

    # --- Summary ---
    print(f"Total institutions before filtering : {len(stats)}")
    print(f"  Pass filter 1 (≥{min_years} years active)  : {f1.sum()}")
    print(f"  Pass filter 2 (still active)        : {f2.sum()}")
    print(f"  Pass filter 3 (avg AUM ≥ ${min_aum:,.0f}): {f3.sum()}")
    print(f"  Pass filter 4 (≥{min_quarters_pct:.0%} quarters filed): {f4.sum()}")
    print(f"  Pass ALL filters                    : {len(filtered)}")

    whitelist_ciks = set(filtered["CIK"])
    # whitelist_ciks.to_csv("Datasets/whitelist_ciks.csv", index=False)

    logger.info(f"Whitelist CIKs obtained.")
    return whitelist_ciks

