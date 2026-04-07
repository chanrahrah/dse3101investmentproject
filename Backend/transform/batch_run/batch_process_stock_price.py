from pathlib import Path 
import logging
import pandas as pd
import numpy as np
import time
from Backend.transform.stock_market_price import main as stock_price_main
from Backend.transform.consolidate_stock_price import main as consolidate_main
from Backend.transform.build_spy_price_data import main as build_spy_price_data_main

from config import DEBUG

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def main():
    if not DEBUG: # Only run the batch process in production mode
        start_time = time.time()
        logger.info("Starting batch process for stock price data...")
        # Step 1: Process raw stock price files and save to PRICES_DS_ROOT
        stock_price_main()
        
        # Step 2: Consolidate processed files into a single Parquet file
        consolidate_main()

        # Step 3: Build SPY price data for backtesting
        build_spy_price_data_main()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Batch process completed in {elapsed_time:.2f} seconds.")

# test
if __name__ == "__main__":
    main()