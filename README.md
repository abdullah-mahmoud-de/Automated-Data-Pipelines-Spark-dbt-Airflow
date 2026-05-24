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

    * **Customer Database**: PostgreSQL table with customer records (ID, name, email, registration_date, status)

    * **Order API**: REST endpoint returning JSON order data (order_id, customer_id, order_date, total_amount, items)

    * **Product Catalog**: CSV file with product information (product_id, name, category, price, supplier)

**Output**
The pipeline produces:

    * **Unified Customer Dimension**: SCD2 table tracking customer changes over time

    * **Daily Order Facts**: Aggregated order metrics by date and customer segment

    * **Product Performance Summary**: Analysis of product sales and trends

    * **Data Quality Report**: Validation results and pipeline execution metrics

**Edge Cases**
Your code must handle:

    * Missing or malformed data in any source

    * API timeouts or connection failures

    * Duplicate records across sources

    * Schema changes in source systems

    * Pipeline failures requiring restart from intermediate points

**Performance Considerations**
**Baseline Expectations**: The system should efficiently process up to 100,000 records within a 2-hour execution window. This baseline assumes a mixed workload of customer updates, order processing, and product catalog refresh on standard hardware (4 CPU cores, 8GB RAM).

**Higher Volume Scenarios**: In production environments, data volumes may significantly exceed 100,000 records. Your implementation should be designed to handle larger datasets through:

    * **Batch Processing**: Process data in configurable chunks (default 1,000 records per batch) to manage memory usage and enable progress tracking

    * **Incremental Loading**: Only process new or changed records since the last pipeline run to minimize processing overhead

    * **Parallel Processing**: Design modular extractors that can run concurrently for different data sources

    * **Memory Management**: Use pandas chunking for large CSV files and database result sets to prevent memory overflow

    * **Connection Pooling**: Implement database connection reuse to minimize connection overhead during batch processing

**Scalability Design**: The pipeline architecture should support horizontal scaling through:

    * Configurable batch sizes that can be tuned based on available resources

    * Modular components that can be deployed across multiple processing nodes

    * Stateless transformation functions that can be parallelized

    * Checkpointing mechanisms to resume processing from failure points

**Performance Monitoring**: Include execution time logging and record processing metrics to identify bottlenecks and optimize batch sizes for your specific environment.

### Code Quality Expectations
Organize your code with clear separation between:

    * **Configuration management**: Environment-specific settings in external files

    * **Data extraction logic**: Modular connectors for each source type

    * **Transformation functions**: Pure functions that can be unit tested

    * **Loading operations**: Atomic writes with rollback capability

    * **Orchestration workflows**: Airflow DAGs with proper dependencies

Use consistent naming conventions, comprehensive error logging, and inline documentation explaining business logic. Include type hints for function parameters and return values.

### Evaluation Focus Areas
Your work will be assessed on:

    * **System Architecture**: How well components integrate and scale

    * **Code Quality**: Organization, documentation, and maintainability

    * **Data Modeling**: Proper implementation of SCD2 logic and dimension design

    * **Error Handling**: Robust failure management and recovery mechanisms

    * **Operational Readiness**: Monitoring, alerting, and troubleshooting capabilities  

