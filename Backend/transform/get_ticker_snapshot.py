import pandas as pd
import yfinance as yf
import numpy as np
import time
from datetime import datetime
from config import DATA_DIR

# --- Load original ticker list ---
df = pd.read_parquet(DATA_DIR/"final_files"/"stock_prices_all.parquet")
all_tickers = df["ticker"].unique().tolist()
print(f"Found {len(all_tickers)} unique tickers. Starting fetch...\n")

# --- Field map ---
INFO_FIELDS = {
    "exchange_country":       "country",
    "previous_close":         "previousClose",
    "bid":                    "bid",
    "ask":                    "ask",
    "day_low":                "dayLow",
    "day_high":               "dayHigh",
    "fifty_two_week_low":     "fiftyTwoWeekLow",
    "fifty_two_week_high":    "fiftyTwoWeekHigh",
    "avg_volume":             "averageVolume",
    "market_cap":             "marketCap",
    "beta":                   "beta",
    "pe_ratio":               "trailingPE",
    "eps":                    "trailingEps",
    "earnings_date":          "earningsTimestamp",
    "forward_dividend_yield": "dividendYield",
    "ex_dividend_date":       "exDividendDate",
    "one_year_target_est":    "targetMeanPrice",
}

COLUMN_ORDER = [
    "ticker", "adj_close", "volume", "open", "high", "low", "close", "year",
    "exchange_country", "previous_close", "bid", "ask", "day_low", "day_high",
    "fifty_two_week_low", "fifty_two_week_high", "avg_volume", "market_cap",
    "beta", "pe_ratio", "eps", "earnings_date", "forward_dividend_yield",
    "ex_dividend_date", "one_year_target_est",
]

NUMERIC_COLS = [
    "adj_close", "volume", "open", "high", "low", "close",
    "previous_close", "bid", "ask", "day_low", "day_high",
    "fifty_two_week_low", "fifty_two_week_high", "avg_volume", "market_cap",
    "beta", "pe_ratio", "eps", "earnings_date", "forward_dividend_yield",
    "ex_dividend_date", "one_year_target_est",
]

BATCH_SIZE = 200
SLEEP_PER_TICKER = 0.5
SLEEP_AFTER_BATCH = 10
OUTPUT_PATH = DATA_DIR/"final_files"/"stock_snapshot.parquet"


def sanitise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce numeric columns to float, replacing Infinity strings,
    float inf, and anything else that can't convert with NaN.
    """
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                .replace({"Infinity": np.nan, "infinity": np.nan, "-Infinity": np.nan})
                .apply(lambda x: np.nan if isinstance(x, float) and not np.isfinite(x) else x)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["year"] = df["year"].astype("int32")
    return df


def main():
    """Fetch ticker snapshots and save to parquet."""
    # --- Fetch loop ---
    records = []
    saved_df = pd.DataFrame(columns=COLUMN_ORDER)  # accumulates all saved batches

    for i, ticker in enumerate(all_tickers, 1):
        print(f"[{i}/{len(all_tickers)}] Fetching {ticker}...")

        try:
            tkr = yf.Ticker(ticker)
            hist = tkr.history(period="1d")

            if hist.empty:
                print(f"  ⚠ No price history for {ticker}, skipping.")
                continue

            latest = hist.iloc[-1]
            info = tkr.info

            row = {
                "ticker":    ticker,
                "adj_close": latest.get("Close"),
                "volume":    latest.get("Volume"),
                "open":      latest.get("Open"),
                "high":      latest.get("High"),
                "low":       latest.get("Low"),
                "close":     latest.get("Close"),
                "year":      datetime.today().year,
            }
            for col, key in INFO_FIELDS.items():
                row[col] = info.get(key)

            records.append(row)

        except Exception as e:
            print(f"  ✗ Error on {ticker}: {e}")
            continue

        time.sleep(SLEEP_PER_TICKER)

        # Batch save
        if i % BATCH_SIZE == 0:
            print(f"\n  💾 Saving batch at ticker {i}...")
            batch_df = sanitise(pd.DataFrame(records, columns=COLUMN_ORDER))
            saved_df = pd.concat([saved_df, batch_df], ignore_index=True)
            saved_df = sanitise(saved_df)
            saved_df.to_parquet(OUTPUT_PATH, index=False)
            print(f"  ✅ Snapshot now has {len(saved_df)} rows.\n")
            records = []  # clear buffer after saving
            time.sleep(SLEEP_AFTER_BATCH)

    # --- Final save ---
    if records:
        print("\nSaving final batch...")
        batch_df = sanitise(pd.DataFrame(records, columns=COLUMN_ORDER))
        saved_df = pd.concat([saved_df, batch_df], ignore_index=True)

    saved_df = sanitise(saved_df)
    saved_df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\n✅ Done. Final snapshot has {len(saved_df)} rows.")


if __name__ == "__main__":
    main()