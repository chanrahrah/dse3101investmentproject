# DSE3101 Investment Project

## Prerequisites

- Python 3.x
- A virtual environment 
- Kaggle account with API credentials
- OpenFIGI API key

---

## Quick Start (Development)
1. Clone the repo and set up `.env` with `DEBUG=true`
2. Activate your virtual environment
3. `pip install -r requirements.txt`
4. `python -m Backend.transform.batch_run.batch_process_form13f`

---

## Getting Started
### 1. Clone repo

Clone repo

### 2. Set Working Directory

Ensure your current working directory is the project root:

```
dse3101investmentproject/
```

### 3. Configure the `.env` File

Create a `.env` file at the project root (same level as this `README.md`) with the following contents:

```dotenv
# App config
DEBUG=false               
# true = development mode (for CopyCats teammates) | false = production mode (for people who want to run whole transform process)

# Kaggle
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key

# OpenFIGI
OPENFIGI_API_KEY=your_openfigi_api_key
OPENFIGI_URL=https://api.openfigi.com/v3/mapping
```

> **`DEBUG=true` (Development):** Automatically downloads the latest dataset from Kaggle and skip all batch_processes.
>
> **`DEBUG=false` (Production):** Downloads only the raw zip files required for the pipeline and run all batch_processes.

### 4. Set environment

> a. Set up and activate virtual environment:
> ```bash
> # Create virtual environment
> python -m venv venv
>
> # Activate it
> # Windows:
> venv\Scripts\activate
> # Mac/Linux:
> source venv/bin/activate
> ```

### 5. Install Dependencies

Ensure your virtual environment is activated, then run:
```bash
pip install -r requirements.txt
```

---

## Running the Pipeline
Run Backend.batch_run_all_backend script to run all the batch_processes required to get final_files for dashboard website.

**Run:**

```bash
python -m Backend.batch_run_all_backend
```

## Frontend app deployment on local host
NOTE: Can frontend put how to deploy app locally here and edit project structure if needed
**User Input Parameters:**

| Parameter                   | Default        | Description                                              |
|-----------------------------|----------------|----------------------------------------------------------|
| `userinput_start_date`      | `'2013-12-31'` | Backtest start date                                      |
| `userinput_end_date`        | `'2025-05-23'` | Backtest end date                                        |
| `userinput_initial_capital` | `10_000`       | Starting capital (USD)                                   |
| `userinput_topN`            | `10`           | Number of top stocks to hold per quarter                 |
| `userinput_topM_institutions` | `10`         | Number of top institutions to track (`10`, `20`, or `30`) |
| `userinput_cost_rate`       | `0.001`        | Transaction cost as a fraction of traded value (0.1%)    |

## Project Structure

```
dse3101investmentproject/
├── .env                         ← secrets and config (never commit this)
├── .gitignore
├── README.md
├── config.py                    ← all paths and env variables
├── Datasets/
│   ├── 13F_zip_files/
│   ├── 13F_clean_files/
│   ├── 13F_filtered_and_mapped_files/
│   ├── 13F_filtered_and_mapped_and_screened_files/
│   ├── data_for_frontend/
│   ├── final_files/                    ← tracked by Git LFS
│   │   ├── final_top10_form13f.parquet
│   │   ├── final_top10_prices.parquet
│   │   ├── final_top20_form13f.parquet
│   │   ├── final_top20_prices.parquet
│   │   ├── final_top30_form13f.parquet
│   │   ├── final_top30_prices.parquet
│   │   ├── spy_prices_2013-01-01_to_2026-03-31.parquet
│   │   └── stock_snapshot.parquet
│   ├── others/
│   ├──SPY_price_data
│   ├──best_instituition_ranking
│   └── stock_price_data/
├── Backend/
│   ├── transform/
│   │   ├── batch_run/
│   │   │   ├── batch_process_stock_price.py ← main run function for transform of stock price data
│   │   │   └── batch_process_form13f.py     ← main run function for transform of form13f data
│   │   └── download_data_from_kaggle.py     ← helper to download latest data from kaggle
│   └── backtesting/
│       ├── batch_process_rank_institutions.py ← main run function for backtesting of topM institutions
│       └── batch_process_rank_stocks.py     ← main run function for backtesting of topN stocks (integration with frontend)
├── Frontend/
│   ├── app.py                               ← main file to run dashboard
│   ├── components/                          
│   │   ├── cumulative_returns.py            ← user input functions
│   │   ├── daily_returns.py                 ← user input functions
│   │   ├── performance_metrics.py           ← user input functions
│   │   ├── portfolio_performance.py         ← functions for left panel of dashboard
│   │   └── top_20.py                        ← functions for right panel of dashboard
│   ├── streamlit/
│   │   └── config.toml                      ← customise theme 
```
