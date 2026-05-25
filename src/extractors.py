import pandas as pd
import psycopg2
import json
import logging
from typing import List, Dict
from config import DatabaseConfig

class DatabaseExtractor:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def _get_connection(self):
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            return conn
        except psycopg2.OperationalError as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def extract_customers(self, last_updated: str = None) -> pd.DataFrame:
        self.logger.info("Starting database extraction for customers...")
        conn = None
        try:
            conn = self._get_connection()
            query = "SELECT * FROM customers"
            params = None
            
            if last_updated:
                query += " WHERE registration_date > %s" 
                params = (last_updated,)
                self.logger.info(f"Running incremental load. Fetching records after {last_updated}")
            else:
                self.logger.info("No last_updated date provided. Running full load.")

            df = pd.read_sql_query(query, conn, params=params)
            self.logger.info(f"Successfully extracted {len(df)} customer records.")
            return df
        except Exception as e:
            self.logger.error(f"Error during customer extraction: {str(e)}")
            raise
        finally:
            if conn is not None:
                conn.close()
                self.logger.info("Database connection closed.")


class APIExtractor:
    def __init__(self, endpoint: str, api_key: str):
        self.file_path = endpoint
        self.api_key = api_key
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def extract_orders(self, date_from: str = None) -> List[Dict]:
        self.logger.info(f"Simulating API request to {self.file_path}")
        try:
            with open(self.file_path, 'r') as file:
                raw_response = json.load(file)
            
            # THE FIX: Unwrap the paginated API envelope
            if isinstance(raw_response, dict) and 'orders' in raw_response:
                data = raw_response['orders']
                self.logger.info("Successfully unwrapped paginated API response.")
            else:
                data = raw_response
            
            if date_from:
                filtered_data = [
                    order for order in data 
                    if order.get('order_date', '') >= date_from
                ]
                return filtered_data
            
            self.logger.info(f"Successfully retrieved all {len(data)} orders (Full Load).")
            return data
        except FileNotFoundError:
            self.logger.error(f"API Simulation Failed: Could not find data file at {self.file_path}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during API extraction: {str(e)}")
            raise


class FileExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def extract_products(self) -> pd.DataFrame:
        self.logger.info(f"Starting extraction from {self.file_path}")
        try:
            # Using the standard read_csv that worked for you in the previous run
            df = pd.read_csv(self.file_path)
            
            # Normalize columns to handle hidden spaces and weird casing
            df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
            
            self.logger.info(f"Normalized columns found in file: {df.columns.tolist()}")
            
            expected_columns = ['product_id', 'name', 'category', 'price', 'supplier']
            missing_cols = [col for col in expected_columns if col not in df.columns]
            
            if missing_cols:
                self.logger.error(f"Validation Failed: Missing columns {missing_cols}")
                raise ValueError(f"Source file is missing required columns: {missing_cols}")
            
            initial_count = len(df)
            df.dropna(how='all', inplace=True)
            df['price'] = pd.to_numeric(df['price'], errors='coerce')

            dropped_rows = initial_count - len(df)
            self.logger.info(f"Successfully extracted {len(df)} products. (Dropped {dropped_rows} empty rows)")
            return df
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during file extraction: {str(e)}")
            raise