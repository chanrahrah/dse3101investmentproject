from Backend.transform.batch_run.batch_process_form13f import main as batch_process_form13f_main
from Backend.transform.batch_run.batch_process_stock_price import main as batch_process_stock_price_main    
from Backend.backtesting.batch_process_rank_institutions import main as batch_process_rank_institutions_main
from Backend.backtesting.batch_process_rank_stocks import get_all_final_files as batch_process_rank_stocks_main

def batch_run_all():
    print("Starting batch process for all steps...")
    batch_process_form13f_main()
    batch_process_stock_price_main()
    batch_process_rank_institutions_main()
    batch_process_rank_stocks_main()
    print("Batch process for all steps completed.")


# execute the batch process when this script is run
if __name__ == "__main__":
    batch_run_all()