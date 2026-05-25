import logging
import os
import pandas as pd
from datetime import datetime
from config import load_config
from extractors import DatabaseExtractor, APIExtractor, FileExtractor
from transformers import DataValidator, SCD2Processor

# Set up logging for the main pipeline execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataPipeline")

def save_to_target(df: pd.DataFrame, file_name: str, output_path: str):
    """Utility function to load/save the final dataframes."""
    if df.empty:
        logger.info(f"No data to save for {file_name}.")
        return

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    full_path = os.path.join(output_path, f"{file_name}.csv")
    df.to_csv(full_path, index=False)
    logger.info(f"Successfully saved {len(df)} records to {full_path}")

def run_data_pipeline(execution_date: str = None):
    """
    Main orchestration function. 
    Can accept an execution_date from Airflow for incremental processing.
    """
    logger.info("=== Starting Automated Data Pipeline ===")
    
    # 1. Load Configuration
    config = load_config()
    
    # Initialize Classes
    db_extractor = DatabaseExtractor(config.database)
    api_extractor = APIExtractor(endpoint=config.api_endpoint, api_key=config.api_key)
    file_extractor = FileExtractor(file_path=config.input_path)
    
    validator = DataValidator()
    scd2_processor = SCD2Processor()

    try:
        # 2. EXTRACT
        logger.info("--- Phase 1: Extraction ---")
        customers_df = db_extractor.extract_customers(last_updated=execution_date)
        
        # The API returns a list of dicts, convert to DataFrame for processing
        orders_raw = api_extractor.extract_orders(date_from=execution_date)
        orders_df = pd.DataFrame(orders_raw)

        logger.info(f"ACTUAL ORDER COLUMNS FOUND: {orders_df.columns.tolist()}")
        
        products_df = file_extractor.extract_products()

        # 3. VALIDATE & STAGE
        logger.info("--- Phase 2: Data Quality Validation ---")
        cust_val = validator.validate_data_quality(customers_df, "Customers", ['customer_id', 'email'])
        ord_val = validator.validate_data_quality(orders_df, "Orders", ['order_id', 'customer_id', 'total_amount'])
        prod_val = validator.validate_data_quality(products_df, "Products", ['product_id', 'price'])

        # Pipeline Gate: Stop if critical validation fails
        if not (cust_val['is_valid'] and ord_val['is_valid'] and prod_val['is_valid']):
            logger.error("Pipeline halted due to Data Quality Validation failures.")
            return

        # 4. TRANSFORM (Apply SCD2 Logic to Customers)
        logger.info("--- Phase 3: Transformation & SCD2 ---")
        
        # Simulate fetching the 'existing' active dimension table
        # In a real scenario, this would be a query to your data warehouse. 
        # Here we mock it by checking if our target file already exists.
        dim_customer_path = os.path.join(config.output_path, "dim_customers.csv")
        if os.path.exists(dim_customer_path):
            existing_customers_df = pd.read_csv(dim_customer_path)
        else:
            existing_customers_df = pd.DataFrame() # Initial load

        # Apply SCD2 to track customer email or status changes
        inserts, updates = scd2_processor.apply_scd2_logic(
            new_data=customers_df,
            existing_data=existing_customers_df,
            primary_key='customer_id',
            compare_columns=['email', 'name', 'status']
        )

        # 5. LOAD
        logger.info("--- Phase 4: Loading Target Tables ---")
        save_to_target(inserts, f"inserts_dim_customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}", config.output_path)
        save_to_target(updates, f"updates_dim_customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}", config.output_path)
        
        # Save orders and products (Overwriting for simplicity in this mockup)
        save_to_target(orders_df, "fact_orders", config.output_path)
        save_to_target(products_df, "dim_products", config.output_path)

        logger.info("=== Pipeline Execution Completed Successfully ===")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Run the pipeline locally for testing
    run_data_pipeline()