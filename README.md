# Enterprise Water Systems Lakehouse

Joseph. N. Njiru
Complete Data Pipeline and Data Governance

A comprehensive, enterprise grade data lakehouse implementation designed for modern water utility operations. This project demonstrates end to end data engineering workflows using the medallion architecture pattern with Delta Lake and Apache Spark.

## Modern water data platform

The Enterprise Water Systems Lakehouse provides a modern, scalable data platform for water utilities to manage, process, and analyse their operational data. Built using industry standard technologies, this platform enables data driven decision making through clean, trusted, and accessible data products.

The platform addresses common challenges in water utility data management including data quality, real time processing, regulatory compliance, and advanced analytics for non revenue water reduction and predictive maintenance.   

## Architecture

The system implements the medallion architecture pattern with three distinct data layers:

**Bronze Layer**
- Raw data ingestion from CSV files
- Data stored in Delta Lake format with full lineage tracking
- Preserves original data structure and timestamps
- Foundation for all downstream processing

**Silver Layer**
- Data cleaning, validation, and enrichment
- Business rule application and data quality checks
- Dimensional modelling with fact and dimension tables
- Optimised for analytical workloads

**Gold Layer**
- Business ready analytics and aggregations
- Key performance indicators and metrics
- Machine learning feature stores
- Direct consumption by business intelligence tools

The containerised stack includes Apache Spark for distributed processing, MinIO for S3 compatible object storage, and a custom dashboard for data visualisation and lineage tracking.

## Technology stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Processing | Apache Spark 3.4.1 | Distributed data processing and transformation |
| Storage Format | Delta Lake | ACID transactions and schema evolution |
| Object Storage | MinIO | S3 compatible storage layer |
| Orchestration | Docker Compose | Container orchestration and service management |
| Data Generation | Python/Pandas | Synthetic data generation for testing |
| Visualisation | Plotly | Interactive charts and business intelligence |
| Data Science | PySpark MLlib | Machine learning and advanced analytics |
| Web Framework | Flask | Custom dashboard and user interface |

## Key features

- **End to end ETL pipeline**: Complete data pipeline from raw ingestion through to business ready analytics
- **Synthetic data generation**: Realistic water utility datasets including meter readings, asset information, and operational data
- **Medallion architecture**: Industry standard Bronze, Silver, Gold layer implementation with Delta Lake
- **Business intelligence dashboard**: Professional visualisations with interactive exploration capabilities
- **Data quality framework**: Automated validation and cleansing processes
- **Scalable infrastructure**: Containerised deployment suitable for development and production environments
- **Advanced analytics**: Machine learning ready feature stores for predictive maintenance and demand forecasting
- **Real time processing**: Stream processing capabilities for operational monitoring   
## How to run this project

Follow these steps to deploy and run the complete enterprise water systems lakehouse:

1. **Build the custom images**
   ```bash
   docker compose build
   ```

2. **Start all services**
   ```bash
   docker compose up -d
   ```

3. **Generate synthetic data**
   ```bash
   python app/datagen.py
   ```

4. **Execute Bronze to Silver transformation**
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" --conf "spark.hadoop.fs.s3a.access.key=minio" --conf "spark.hadoop.fs.s3a.secret.key=minio123" --conf "spark.hadoop.fs.s3a.path.style.access=true" --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" --conf "spark.pyspark.driver.python=/opt/spark/venv/bin/python" --conf "spark.pyspark.python=/opt/spark/venv/bin/python" /app/bronze_to_silver.py
   ```

5. **Execute Silver to Gold transformation**
   ```bash
   docker compose exec spark-master /opt/spark/bin/spark-submit --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" --conf "spark.hadoop.fs.s3a.access.key=minio" --conf "spark.hadoop.fs.s3a.secret.key=minio123" --conf "spark.hadoop.fs.s3a.path.style.access=true" --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" --conf "spark.pyspark.driver.python=/opt/spark/venv/bin/python" --conf "spark.pyspark.python=/opt/spark/venv/bin/python" /app/silver_to_gold.py
   ```

6. **Generate visualisations**
   ```bash
   docker compose exec spark-master /opt/spark/venv/bin/python /app/visualise.py
   ```

7. **Access the dashboard**
   
   Open your browser to http://localhost:9002 to view the dashboard.

Additional service endpoints:
- **Spark Master UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (credentials: minio/minio123)

## Project showcase

### Data lineage and architecture
<img width="1415" height="247" alt="data-lineage-flow" src="https://github.com/user-attachments/assets/7e092a41-6762-4d20-8d9a-9558dc383625" />

The platform implements a complete medallion architecture with clear data lineage tracking from raw ingestion through to business analytics.

### Business intelligence visualisations

The following visualisations are automatically generated and available through the dashboard:

#### Non revenue water analytics
- **Daily NRW timeseries**: Track daily non revenue water percentages over time
- **Monthly NRW trends**: Analyse long term water loss patterns
- **NRW by day of week**: Identify weekly operational patterns

#### Asset management insights
- **Asset failure analysis**: Distribution of failures by asset age and type
- **Failure rate by age group**: Risk assessment for predictive maintenance
- **Asset type failure breakdown**: Prioritise maintenance programmes by asset category

#### Operational intelligence
- **Hourly consumption heatmap**: Understand demand patterns throughout the day
- **Meter consumption distribution**: Statistical analysis of customer usage patterns

All visualisations are available in both interactive HTML format and high resolution PNG format for reports and presentations.

### Data products

The platform delivers production ready data products through the Gold layer:

- **nrw_daily_summary**: Daily aggregated non revenue water metrics for operational monitoring
- **asset_failure_features**: Machine learning ready features for predictive maintenance models

These data products provide the foundation for advanced analytics, reporting, and data science initiatives across the organisation.

## Contributing

Please to do not alter this original project. You may download a copy and with a copy.

## Licence

This project is released under the MIT licence. See the LICENCE file for full details.   
