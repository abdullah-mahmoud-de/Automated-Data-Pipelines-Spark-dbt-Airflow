import pandas as pd
import logging
from datetime import datetime
from typing import Tuple, Dict, Any, List

class DataValidator:
    """
    Validates data quality before it enters the transformation pipeline.
    Checks for structural integrity, missing values, and schema compliance.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def validate_data_quality(
        self, 
        data: pd.DataFrame, 
        table_name: str, 
        required_columns: List[str] = None
    ) -> Dict[str, Any]:
        
        self.logger.info(f"Initiating data quality validation for: {table_name}")
        
        results = {
            "table_name": table_name,
            "is_valid": True,
            "row_count": len(data),
            "errors": [],
            "metrics": {}
        }

        # 1. Empty Data Check
        if data.empty:
            results["is_valid"] = False
            results["errors"].append("Dataset is completely empty.")
            self.logger.error(f"Validation Failed [{table_name}]: Empty DataFrame.")
            return results

        # 2. Schema and Null Checks
        if required_columns:
            # Check for missing columns
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                results["is_valid"] = False
                results["errors"].append(f"Missing required columns: {missing_cols}")
                self.logger.error(f"Validation Failed [{table_name}]: Missing {missing_cols}")
                return results

            # Check for null values in required columns
            null_counts = data[required_columns].isnull().sum()
            cols_with_nulls = null_counts[null_counts > 0].to_dict()
            
            if cols_with_nulls:
                results["is_valid"] = False
                results["errors"].append(f"Null values detected in required fields: {cols_with_nulls}")
                self.logger.error(f"Validation Failed [{table_name}]: Nulls found in {cols_with_nulls}")

        # 3. Calculate basic metrics for the data quality report
        results["metrics"]["total_columns"] = len(data.columns)
        results["metrics"]["memory_usage_mb"] = data.memory_usage(deep=True).sum() / (1024 * 1024)

        if results["is_valid"]:
            self.logger.info(f"Validation Passed [{table_name}]: {len(data)} rows ready for processing.")
            
        return results


class SCD2Processor:
    """
    Implements Slowly Changing Dimension (Type 2) logic.
    Maintains complete historical tracking by comparing incoming records 
    against existing active records.
    """
    def __init__(self, connection=None):
        self.connection = connection
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def apply_scd2_logic(
        self, 
        new_data: pd.DataFrame, 
        existing_data: pd.DataFrame, 
        primary_key: str, 
        compare_columns: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        
        self.logger.info("Executing SCD2 historical tracking logic...")
        current_timestamp = datetime.now()

        # Edge Case: Initial Load (No existing data)
        if existing_data is None or existing_data.empty:
            self.logger.info("No existing records found. Processing as initial load.")
            inserts = new_data.copy()
            inserts['effective_start_date'] = current_timestamp
            inserts['effective_end_date'] = pd.NaT
            inserts['is_active'] = True
            
            # Return empty dataframe for updates, ensuring schema matches
            updates = pd.DataFrame(columns=[primary_key, 'effective_end_date', 'is_active'])
            return inserts, updates

        # Isolate currently active records to compare against
        active_records = existing_data[existing_data['is_active'] == True].copy()

        # Perform an outer join to identify new vs. existing records
        merged = new_data.merge(
            active_records, 
            on=primary_key, 
            how='left', 
            indicator=True, 
            suffixes=('_new', '_old')
        )

        # 1. Identify Brand New Records (Exist in new_data, but not in active_records)
        new_records_mask = merged['_merge'] == 'left_only'
        brand_new_data = new_data[new_data[primary_key].isin(merged.loc[new_records_mask, primary_key])].copy()

        # 2. Identify Changed Records (Exist in both, but tracked attributes have changed)
        both_mask = merged['_merge'] == 'both'
        changed_mask = pd.Series(False, index=merged.index)
        
        for col in compare_columns:
            # Compare columns, treating NaNs as empty strings to avoid false positives
            col_has_changed = merged[f'{col}_new'].fillna('') != merged[f'{col}_old'].fillna('')
            changed_mask = changed_mask | (both_mask & col_has_changed)

        changed_ids = merged.loc[changed_mask, primary_key]

        # 3. Construct the Updates DataFrame (Closing out the old active records)
        records_to_update = pd.DataFrame({
            primary_key: changed_ids,
            'effective_end_date': current_timestamp,
            'is_active': False
        })

        # 4. Construct the Inserts DataFrame (Brand new records + new versions of changed records)
        new_versions_data = new_data[new_data[primary_key].isin(changed_ids)].copy()
        records_to_insert = pd.concat([brand_new_data, new_versions_data], ignore_index=True)
        
        # Apply SCD2 metadata to the inserts
        if not records_to_insert.empty:
            records_to_insert['effective_start_date'] = current_timestamp
            records_to_insert['effective_end_date'] = pd.NaT
            records_to_insert['is_active'] = True

        self.logger.info(
            f"SCD2 Processing Complete: "
            f"{len(brand_new_data)} net-new records, "
            f"{len(changed_ids)} updated records. "
            f"Total to insert: {len(records_to_insert)}, Total to close: {len(records_to_update)}."
        )
        
        return records_to_insert, records_to_update