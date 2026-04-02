from pathlib import Path 
import logging
import pandas as pd
import numpy as np
import time
from Backend.transform.stock_market_price import main as stock_price_main
from Backend.transform.consolidate_stock_price import main as consolidate_main

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def main():
    start_time = time.time()
    logger.info("Starting batch process for stock price data...")
    
    # Step 1: Process raw stock price files and save to PRICES_DS_ROOT
    # stock_price_main()
    
    # Step 2: Consolidate processed files into a single Parquet file
    consolidate_main()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Batch process completed in {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    main()