# this script applies the general filters to each quarter's clean form13f parquet files using whitelist_ciks list. 
# And adds the ticker column by mapping CUSIP to ticker using OpenFIGI API. 
# Each quarter's filtered data will be saved as a separate parquet file in the Datasets/13F_filtered_files folder.
from pathlib import Path
import pandas as pd
import logging
from Backend.transform.mapper_cusip_to_ticker import get_all_unique_cusips, build_cusip_ticker_map, map_cusip_to_ticker

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ==========================================================
# Build cusip-> ticker map and save as parquet
# ============================================================
def build_and_save_cusip_ticker_map(clean_dir: Path, filtered_dir: Path, openfigi_key: str) -> Path:
    """
    One-time function: collect all CUSIPs from clean parquets, call OpenFIGI API,
    and save the cusip->ticker map as a parquet file.

    Run this ONCE. After that, apply_filters_and_mapping_to_all_parquets() will
    read the saved file directly without calling the API again.

    Returns
    -------
    Path to the saved cusip_ticker_map.parquet
    """
    print("=== Step 1: Collecting unique CUSIPs ===")
    all_cusips = get_all_unique_cusips(clean_dir)

    print("\n=== Step 2: Mapping CUSIPs to tickers (one-time API call) ===")
    cusip_ticker_df = build_cusip_ticker_map(all_cusips, openfigi_key)
    print(f"Unique tickers obtained: {cusip_ticker_df['ticker'].nunique():,}")

    output_path = filtered_dir / "cusip_ticker_map.parquet"
    cusip_ticker_df.to_parquet(output_path, index=False)
    print(f"Saved cusip->ticker map to: {output_path}")

    return output_path


# ==========================================================
# Filter single parquet file
# ==========================================================

def filter_and_map_single_parquet(parquet_path: Path, filtered_and_mapped_dir: Path, whitelist_ciks: set,
                                  cusip_ticker_df: pd.DataFrame):
    
    df = pd.read_parquet(parquet_path)

    # Filter by whitelist_ciks
    if whitelist_ciks is not None:
        filtered_df = df[df["CIK"].isin(whitelist_ciks)]
    else:
        filtered_df = df.copy()

    # Merge pre-built cusip->ticker map (no API call)
    # Keep only equity-like securities in cusip ticker mapper. 
    keep_types = {"Common Stock"}  # adjust based on your research scope

    cusip_ticker_clean = cusip_ticker_df[
        cusip_ticker_df["security_type"].isin(keep_types)
    ].copy()

    mapped_df = filtered_df.merge(cusip_ticker_clean, on="CUSIP", how="inner")
    unmapped = mapped_df["ticker"].isna().sum()

    # Recompute equity-only weights
    equity_total = mapped_df.groupby(["CIK", "PERIODOFREPORT"])["VALUE"].transform("sum")
    mapped_df["equity_portfolio_total"] = equity_total
    mapped_df["equity_weight"] = mapped_df["VALUE"] / mapped_df["equity_portfolio_total"]

    # Save filtered data
    output_path = filtered_and_mapped_dir / f"{parquet_path.name}_final.parquet"
    mapped_df.to_parquet(output_path, index=False)
    logger.info(f"Saved Filtered and mapped {parquet_path.name} -> {output_path.name}")

    # Return stats for this file
    return {
        "parquet_file":                  parquet_path.name,
        "original_rows":                 len(df),
        "filtered_rows":                 len(filtered_df),
        "original_unique_institutions":  df["CIK"].nunique(),
        "filtered_unique_institutions":  filtered_df["CIK"].nunique(),
        "unmapped_tickers":             unmapped
    }
   
def apply_filters_and_mapping_to_all_parquets(clean_dir: Path, filtered_dir: Path,
                                               whitelist_ciks: set):
    """
    Reads the pre-built cusip_ticker_map.parquet, then filters and merges
    into each parquet file in clean_dir.
    """
    # Load pre-built cusip->ticker map
    cusip_map_path = filtered_dir / "cusip_ticker_map.parquet"
    if not cusip_map_path.exists():
        raise FileNotFoundError(
            f"cusip_ticker_map.parquet not found at {cusip_map_path}. "
            f"Run build_and_save_cusip_ticker_map() first."
        )
    cusip_ticker_df = pd.read_parquet(cusip_map_path)
    print(f"Loaded cusip->ticker map: {len(cusip_ticker_df):,} rows, "
          f"{cusip_ticker_df['ticker'].nunique():,} unique tickers")

    # Iterate parquets, filter + merge
    print("\n=== Filtering and merging ticker map into each parquet ===")
    summary = []
    for parquet_file in clean_dir.glob("*.parquet"):
        stats = filter_and_map_single_parquet(parquet_file, filtered_dir,
                                              whitelist_ciks, cusip_ticker_df)
        summary.append(stats)
        print(f"  Done: {parquet_file.name}")

    # Summary table
    summary_df = pd.DataFrame(summary).sort_values("parquet_file").reset_index(drop=True)
    totals = {
        "parquet_file":                 "TOTAL",
        "original_rows":                summary_df["original_rows"].sum(),
        "filtered_rows":                summary_df["filtered_rows"].sum(),
        "original_unique_institutions": summary_df["original_unique_institutions"].sum(),
        "filtered_unique_institutions": summary_df["filtered_unique_institutions"].sum(),
        "unmapped_tickers":             summary_df["unmapped_tickers"].sum(),
    }
    summary_df = pd.concat([summary_df, pd.DataFrame([totals])], ignore_index=True)
    print("\n=== Summary ===")
    print(summary_df.to_string(index=False))

    return summary_df