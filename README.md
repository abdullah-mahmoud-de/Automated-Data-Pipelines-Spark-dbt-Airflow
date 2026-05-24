# Project Building Automated Data Pipelines with Spark, dbt, and Airflow

### Technical Overview

You will build an automated data pipeline using Python, Apache Spark for distributed processing, dbt for data transformations, and Apache Airflow for workflow orchestration. The system will process data from multiple sources, apply slowly-changing dimension (SCD2) logic for historical tracking, and run on a daily schedule with comprehensive monitoring.

---

### Functional Requirements

Your pipeline must implement the following capabilities:

1. **Multi-Source Data Ingestion**: Connect to and extract data from at least three different source types (database tables, API endpoints, CSV files)

2. **Data Quality Validation**: Implement checks for null values, data type consistency, and business rule compliance

3. **Incremental Processing**: Handle only new or changed records to optimize performance

4. **SCD2 Historical Tracking**: Maintain complete change history for key business entities with effective dates

5. **Modular Transformation Logic**: Organize data processing into reusable, testable components

6. **Error Handling and Recovery**: Gracefully handle failures with retry mechanisms and alerting

7. **Automated Scheduling**: Run the complete pipeline daily at a specified time

8. **Data Lineage Documentation**: Track data movement and transformations throughout the pipeline

---

### Technical Specifications

**Input**
The pipeline will process three data sources:

- **Customer Database**: PostgreSQL table with customer records (ID, name, email, registration_date, status)

- **Order API**: REST endpoint returning JSON order data (order_id, customer_id, order_date, total_amount, items)

- **Product Catalog**: CSV file with product information (product_id, name, category, price, supplier)

**Output**
The pipeline produces:

- **Unified Customer Dimension**: SCD2 table tracking customer changes over time

- **Daily Order Facts**: Aggregated order metrics by date and customer segment

- **Product Performance Summary**: Analysis of product sales and trends

- **Data Quality Report**: Validation results and pipeline execution metrics

**Edge Cases**
Your code must handle:

- Missing or malformed data in any source

- API timeouts or connection failures

- Duplicate records across sources

- Schema changes in source systems

- Pipeline failures requiring restart from intermediate points

**Performance Considerations**
**Baseline Expectations**: The system should efficiently process up to 100,000 records within a 2-hour execution window. This baseline assumes a mixed workload of customer updates, order processing, and product catalog refresh on standard hardware (4 CPU cores, 8GB RAM).

**Higher Volume Scenarios**: In production environments, data volumes may significantly exceed 100,000 records. Your implementation should be designed to handle larger datasets through:

- **Batch Processing**: Process data in configurable chunks (default 1,000 records per batch) to manage memory usage and enable progress tracking

- **Incremental Loading**: Only process new or changed records since the last pipeline run to minimize processing overhead

- **Parallel Processing**: Design modular extractors that can run concurrently for different data sources

- **Memory Management**: Use pandas chunking for large CSV files and database result sets to prevent memory overflow

- **Connection Pooling**: Implement database connection reuse to minimize connection overhead during batch processing

**Scalability Design**: The pipeline architecture should support horizontal scaling through:

- Configurable batch sizes that can be tuned based on available resources

- Modular components that can be deployed across multiple processing nodes

- Stateless transformation functions that can be parallelized

- Checkpointing mechanisms to resume processing from failure points

**Performance Monitoring**: Include execution time logging and record processing metrics to identify bottlenecks and optimize batch sizes for your specific environment.

### Code Quality Expectations

Organize your code with clear separation between:

- **Configuration management**: Environment-specific settings in external files

- **Data extraction logic**: Modular connectors for each source type

- **Transformation functions**: Pure functions that can be unit tested

- **Loading operations**: Atomic writes with rollback capability

- **Orchestration workflows**: Airflow DAGs with proper dependencies

Use consistent naming conventions, comprehensive error logging, and inline documentation explaining business logic. Include type hints for function parameters and return values.

### Evaluation Focus Areas

Your work will be assessed on:

- **System Architecture**: How well components integrate and scale

- **Code Quality**: Organization, documentation, and maintainability

- **Data Modeling**: Proper implementation of SCD2 logic and dimension design

- **Error Handling**: Robust failure management and recovery mechanisms

- **Operational Readiness**: Monitoring, alerting, and troubleshooting capabilities

---

## Data Pipeline Automation System

### Instructions

**Before You Begin**
Set up your development environment with the required dependencies:

`pip install apache-airflow pandas psycopg2-binary requests pyspark dbt-core`

Download the starter files and sample data:

- Pipeline configuration templates

- Sample CSV product catalog

- Database connection scripts

- Airflow DAG skeleton

Create a local PostgreSQL database for development and testing.

**Step-by-Step Instructions**
**Step 1: Design the Data Flow Architecture**

Create a comprehensive data flow diagram showing:

- All source systems and their connection methods

- Data transformation stages and their sequence

- Target tables and their relationships

- Error handling and retry paths

Document your architectural decisions, including why you chose specific processing patterns and how the system will scale.

**Step 2: Implement Data Source Connectors**

Build modular extraction components:

- Database connector with connection pooling and query optimization

- API client with authentication, rate limiting, and pagination handling

- File processor with schema validation and error detection

Each connector should return standardized data structures and include comprehensive error handling with detailed logging.

**Step 3: Build SCD2 Data Model**

Implement slowly-changing dimension logic:

- Compare incoming records with existing dimension data

- Create new records for changed attributes with appropriate effective dates

- Close out previous versions by setting end dates

- Handle edge cases like reactivated records or data corrections

Test your SCD2 implementation with sample data that includes creates, updates, and deletes.

**Step 4: Create Transformation Pipeline**

Develop modular transformation functions:

- Data cleaning and standardization routines

- Business rule validation and enrichment

- Aggregation logic for fact table creation

- Data quality metrics calculation

Organize transformations into logical stages that can be executed independently and support incremental processing.

**Step 5: Configure Airflow Orchestration**

Build an Airflow DAG that:

- Schedules daily pipeline execution

- Manages task dependencies and parallel processing

- Implements retry logic with exponential backoff

- Sends notifications on success or failure

- Provides detailed execution logging and monitoring

Include data quality checks as pipeline gates that prevent downstream processing if validation fails.

**Resources**

[Airflow Configuration Guide](https://d3c33hcgiwev3.cloudfront.net/_5b999e4a14cd4bb6b41bc5ad21537d00_Airflow-Configuration-Guide.zip?Expires=1779722005&Signature=fevo1irQY6cmit4Fgy9Z21Je7m8MyYEyRp1PbLILMfbjxtXo84GAgckXHkGBc3N9AxE0GA1aQ58-0izJl9KLQ95bk7Bo9pRbVElcHbl32GAPc4hNzluArJKvQ395P-qrKrUwFsFLSzS9ASeaJ4ForvRZ4yfejrbF~EoO-zdQNYo_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A)
[Environment Configuration Template](https://d3c33hcgiwev3.cloudfront.net/_5b999e4a14cd4bb6b41bc5ad21537d00_Environment-Configuration-Template.zip?Expires=1779722005&Signature=Jk2HIoBZGhfRtdbyWglf~-byCBRQizmu0fCj9F8wcw2KEINvp22ZCs63zExgd~IJxTVwzSPnWCVPacDeT4u5EJAOw2gQPh77LRZXks~Pg2zPypo7XNZNVa~18FDT11usPtF6cZaN9hYk8bYYQfDC6oVXIrU5nZ2dsimC7UN5D6A_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A)
[Basic Project Structure Template](https://d3c33hcgiwev3.cloudfront.net/_5b999e4a14cd4bb6b41bc5ad21537d00_Basic-Project-Structure-Template.zip?Expires=1779722005&Signature=JCx24ElJe4z8MEFKJVhSrOIVQf2~NvXkVN4vRZlDBUjz2A5GM0r6M9INcB~a8SluBog2PrZx0o77vq4kHyfx58X6Pao2U8Ve3RGe4KT7qGUptWbpBap2EtTULY1ZT83~f6wt~F3~HdmfsQkOfNmSsr3JM2ygAUB~nx4~ToxmrOM_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A)
[Sample Data Files](https://d3c33hcgiwev3.cloudfront.net/_5b999e4a14cd4bb6b41bc5ad21537d00_Sample-Data-Files.zip?Expires=1779722005&Signature=fd8Xt7MhjdqyrNbUq8FM6-nHwqx5FvaoCWM42BW5QTKjkxbToUIe2-sju5jevsacdQn84m1PssXtF3KZHjJ3XpYlCJwzMr9Pi0Q8s09WiKPSshMgINyZX5X6XTSLMIm0l5tAxWgl50~2P-QRRiqS02nP-wrQfxj13Bc2MBpsxfg_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A)
[Database Setup Script](https://d3c33hcgiwev3.cloudfront.net/_5b999e4a14cd4bb6b41bc5ad21537d00_Database-Setup-Script.zip?Expires=1779722005&Signature=c4LRXjvJj9Oe-IW9vZgPtWXUMeV7bwGVsb9w3~bv~nkT9vrhvCeYpa4ps9ik6LP-XgNbIOtMucSScqv8HKPbd1rYw3GKjaoZuX0oYt6EFmkK7lS88vwAj9OHF3qLz8mpnSaCQxacP3Z2MQ6cjIAhwLfs-vz-FG2InRxtmi4ocOo_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A)

**Tips for Success**

- Start with small data samples to validate your logic before scaling up

- Implement comprehensive logging at each stage to support troubleshooting

- Use configuration files to manage environment-specific settings rather than hardcoding values

- Test your SCD2 logic thoroughly with edge cases before integrating with the full pipeline

**Common Pitfalls**

- **Forgetting to handle data schema changes**: Build flexibility into your extraction logic to accommodate new columns or data types

- **Not implementing proper error recovery**: Ensure your pipeline can restart from intermediate points rather than reprocessing everything from scratch

- **Ignoring data quality validation**: Include checks at each stage to catch issues early rather than propagating bad data downstream

### Starter Code

```python
# config.py - Configuration management

import os

from dataclasses import dataclass

from typing import Dict, Any

@dataclass

class DatabaseConfig:

    host: str

    port: int

    database: str

    username: str

    password: str



@dataclass

class PipelineConfig:

    database: DatabaseConfig

    api_endpoint: str

    api_key: str

    input_path: str

    output_path: str

    batch_size: int = 1000



def load_config() -> PipelineConfig:

    # YOUR CODE HERE: Load configuration from environment variables or config file

    pass
```

```python
# extractors.py - Data source connectors

import pandas as pd

import psycopg2

import requests

from typing import List, Dict

class DatabaseExtractor:

    def __init__(self, config: DatabaseConfig):

        self.config = config



    def extract_customers(self, last_updated: str = None) -> pd.DataFrame:

        # YOUR CODE HERE: Connect to PostgreSQL and extract customer data

        # Implement incremental extraction using last_updated parameter

        pass



class APIExtractor:

    def __init__(self, endpoint: str, api_key: str):

        self.endpoint = endpoint

        self.api_key = api_key



    def extract_orders(self, date_from: str) -> List[Dict]:

        # YOUR CODE HERE: Extract orders from API endpoint

        # Handle pagination, rate limiting, and authentication

        pass



class FileExtractor:

    def __init__(self, file_path: str):

        self.file_path = file_path



    def extract_products(self) -> pd.DataFrame:

        # YOUR CODE HERE: Read and validate CSV product data

        # Include data quality checks and error handling

        pass
```

```python
# transformers.py - Data transformation logic

from datetime import datetime

import pandas as pd

from typing import Tuple

class SCD2Processor:

    def __init__(self, connection):

        self.connection = connection



    def apply_scd2_logic(self, new_data: pd.DataFrame,

                        existing_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

        # YOUR CODE HERE: Implement SCD2 logic

        # Return (records_to_insert, records_to_update)

        pass



class DataValidator:

    def __init__(self):

        self.validation_results = {}



    def validate_data_quality(self, data: pd.DataFrame,

                            table_name: str) -> Dict[str, Any]:

        # YOUR CODE HERE: Implement data quality checks

        # Return validation metrics and pass/fail status

        pass
```

```python
# pipeline.py - Main pipeline orchestration

from extractors import DatabaseExtractor, APIExtractor, FileExtractor

from transformers import SCD2Processor, DataValidator

from config import load_config

import logging

def run_data_pipeline():

    # YOUR CODE HERE: Orchestrate the complete pipeline

    # 1. Load configuration

    # 2. Extract data from all sources

    # 3. Apply transformations and SCD2 logic

    # 4. Load data to target tables

    # 5. Generate data quality report

    pass

if __name__ == "__main__":

    run_data_pipeline()
```

```python
# airflow_dag.py - Airflow workflow definition

from airflow import DAG

from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from pipeline import run_data_pipeline

default_args = {

    'owner': 'data-engineering-team',

    'depends_on_past': False,

    'start_date': datetime(2024, 1, 1),

    'email_on_failure': True,

    'email_on_retry': False,

    'retries': 2,

    'retry_delay': timedelta(minutes=5)

}

# YOUR CODE HERE: Define the complete DAG with tasks for each pipeline stage
```

---

### Test Cases

Use these test cases to verify your code before submitting. Additional hidden test cases will be used for grading.

**Test Case 1: Basic Data Extraction**

Input: Mock database connection with sample customer records

Expected Output: Pandas DataFrame with correct schema and row count

Tests: Connection handling and data type consistency

**Test Case 2: API Error Handling**

- Input: API endpoint returning HTTP 500 error

- Expected Output: Appropriate exception with retry logic activated

- Tests: Error handling and resilience mechanisms

**Test Case 3: SCD2 New Records**

- Input: Customer data with 3 new customers not in existing dimension

- Expected Output: 3 new records with appropriate effective dates

- Tests: Correct implementation of SCD2 insert logic

**Test Case 4: SCD2 Updated Records**

- Input: Existing customer with changed email address

- Expected Output: Previous record end-dated, new record created with current effective date

- Tests: SCD2 update logic and historical preservation

**Test Case 5: Data Quality Validation**

- Input: Dataset with null values in required fields

- Expected Output: Validation failure with detailed error report

- Tests: Data quality checks and pipeline gating

**Test Case 6: Incremental Processing**

- Input: Large dataset with only 10% new/changed records

- Expected Output: Processing of only the changed subset

- Tests: Efficiency and incremental load logic

**Test Case 7: Pipeline Recovery**

- Input: Pipeline failure after successful extraction but before transformation

- Expected Output: Restart from transformation stage without re-extracting data

- Tests: Checkpointing and recovery mechanisms

**Test Case 8: Airflow DAG Validation**

- Input: DAG definition with task dependencies

- Expected Output: Successful DAG parsing with correct task sequence

- Tests: Proper Airflow workflow configuration
