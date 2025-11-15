# User guide: Enterprise Water Systems Lakehouse

## Introduction

The Enterprise Water Systems Lakehouse is a modern data platform designed to provide clean, trusted data for analysis and reporting across your water utility operations. This platform consolidates data from multiple sources and transforms it into reliable, business ready data products that support informed decision making.

The platform follows industry best practices for data management, ensuring that all data products are validated, documented, and optimised for use by analysts, managers, and data science teams. Whether you need daily operational reports or advanced predictive analytics, the platform provides the foundation for all your data driven initiatives.

## Available data products

The lakehouse delivers two primary data products through the Gold layer, each designed for specific business use cases:

### Non revenue water analytics: gold.nrw_daily_summary

This data product provides daily calculations of non revenue water across your service areas. Non revenue water represents the difference between the water you produce and the water you successfully bill to customers, helping you identify and quantify water losses in your system.

Use this data product to:
- Track daily NRW performance against targets
- Identify districts with high water loss
- Monitor trends in apparent vs real losses
- Support regulatory reporting requirements
- Analyse the effectiveness of leak detection programmes

**Data columns and descriptions:**

| Column | Description | Example Value |
|--------|-------------|---------------|
| report_date | Date of the water balance calculation | 2024-01-15 |
| district_id | Unique code for the water supply district | DIS_001 |
| district_name | Name of the water supply district | Perth Central |
| total_supplied_kl | Total water supplied to district (kilolitres) | 50,000 |
| total_billed_kl | Total water billed to customers (kilolitres) | 45,000 |
| nrw_kl | Non revenue water volume (kilolitres) | 5,000 |
| nrw_percent | Non revenue water as percentage of supply | 10.0 |
| apparent_loss_kl | Apparent losses from theft, meter errors (kilolitres) | 1,500 |
| real_loss_kl | Real losses from leaks and bursts (kilolitres) | 3,500 |

### Predictive maintenance analytics: gold.asset_failure_features

This data product is designed for the data science team to develop predictive maintenance models. It combines asset characteristics, operational sensor data, and maintenance history to identify assets at risk of failure.

Use this data product to:
- Develop machine learning models for failure prediction
- Prioritise maintenance activities based on risk
- Understand relationships between asset age, conditions, and failures
- Optimise maintenance schedules and resource allocation
- Transition from reactive to predictive maintenance strategies

**Data columns and descriptions:**

| Column | Description | Example Value |
|--------|-------------|---------------|
| asset_id | Unique identifier for the asset | PIPE_12345 |
| snapshot_date | Date when the feature snapshot was taken | 2024-01-15 |
| asset_type | Category of asset | Pipe |
| install_date | When the asset was originally installed | 1995-03-20 |
| material | Material composition of the asset | Steel |
| asset_age_days | Age of asset in days since installation | 10,592 |
| avg_pressure_30d | Average operating pressure over 30 days (kPa) | 450.2 |
| max_vibration_7d | Maximum vibration reading over 7 days (Hz) | 2.1 |
| days_since_last_maintenance | Days since last maintenance activity | 365 |
| failure_event | Whether asset failed in observation period (1=yes, 0=no) | 0 |

## How to access data

These data products are stored in industry standard Delta Lake format and can be accessed using your organisation's standard business intelligence and analytics tools.

### Business intelligence tools

**Power BI**: Connect to the data platform using the Delta Lake connector or through your organisation's data gateway. Both data products are optimised for direct querying and reporting.

**Tableau**: Use Tableau's native Delta Lake support or connect through your data warehouse infrastructure. The Gold layer tables are designed for efficient visualisation and analysis.

**Excel**: For smaller datasets or ad hoc analysis, data can be exported through your organisation's standard data access procedures.

### Advanced analytics platforms

**Python/R**: Data scientists can access the data products directly through PySpark, pandas, or other data science libraries using your organisation's analytics environment.

**Machine learning platforms**: The asset failure features data product is designed for direct consumption by machine learning platforms including Azure ML, AWS SageMaker, or on premises solutions.

### Standard SQL access

Both data products support standard SQL queries through your organisation's data platform. Example queries:

```sql
-- Daily NRW summary for the last 30 days
SELECT report_date, district_name, nrw_percent
FROM gold.nrw_daily_summary
WHERE report_date >= DATE_SUB(CURRENT_DATE, 30)
ORDER BY report_date DESC;

-- Assets requiring maintenance attention
SELECT asset_id, asset_type, days_since_last_maintenance
FROM gold.asset_failure_features
WHERE days_since_last_maintenance > 365
ORDER BY days_since_last_maintenance DESC;
```

## Data freshness and updates

- **NRW daily summary**: Updated daily after completion of meter reading collection and validation processes
- **Asset failure features**: Updated weekly with the latest sensor data and maintenance records

## Support and assistance

For questions about data definitions, access procedures, or reporting requirements, please contact your local data team or system administrator. Technical support for the platform infrastructure is available through your organisation's standard IT service channels.

For training on using these data products effectively, please refer to your organisation's data literacy and analytics training programmes.