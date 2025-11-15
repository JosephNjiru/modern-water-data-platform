from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    rand,
    date_format,
    datediff,
    current_date,
    substring,
    sum as spark_sum,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import make_dataset_urn, make_lineage_mce


def create_spark_session():
    """
    Creates and configures a SparkSession for our local lakehouse environment.
    This sets up the connection to MinIO (our S3-compatible storage) and includes
    the Delta Lake package for ACID transactions. Good on ya for local-first development!
    """
    spark = (
        SparkSession.builder.appName("EWL Silver to Gold Aggregation")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    return spark


def build_nrw_summary_gold(spark):
    """
    Builds the gold.nrw_daily_summary data product for NRW analysis.
    This aggregates meter readings to calculate Non-Revenue Water by district and date.
    Fair dinkum business intelligence!
    """
    print("Building NRW Daily Summary Gold table...")

    # Read from Silver layer
    silver_path = "s3a://silver/fct_meter_readings"
    meter_df = spark.read.format("delta").load(silver_path)

    # Simulate 'water supplied' vs 'water billed': 90% of readings are billed
    # For simplicity, assume all readings are 'supplied', and randomly mark 90% as billed
    nrw_df = meter_df.withColumn("is_billed", when(rand() < 0.9, True).otherwise(False))

    # Derive district from meter_id (e.g., first 3 characters)
    nrw_df = nrw_df.withColumn("district", substring(col("meter_id"), 1, 3))

    # Extract report_date from reading_timestamp
    nrw_df = nrw_df.withColumn(
        "report_date", date_format(col("reading_timestamp"), "yyyy-MM-dd")
    )

    # Aggregate: total_supplied_kl (sum of all consumption), total_billed_kl (sum where is_billed=True)
    agg_df = nrw_df.groupBy("report_date", "district").agg(
        spark_sum("consumption_kl").alias("total_supplied_kl"),
        spark_sum(when(col("is_billed"), col("consumption_kl")).otherwise(0)).alias(
            "total_billed_kl"
        ),
    )

    # Calculate NRW: supplied - billed, and percentage
    agg_df = agg_df.withColumn(
        "nrw_kl", col("total_supplied_kl") - col("total_billed_kl")
    )
    agg_df = agg_df.withColumn(
        "nrw_percent", (col("nrw_kl") / col("total_supplied_kl")) * 100
    )

    # Write to Gold layer
    gold_path = "s3a://gold/nrw_daily_summary"
    agg_df.write.format("delta").mode("overwrite").save(gold_path)
    print("NRW Daily Summary Gold table written successfully.")


def build_asset_features_gold(spark):
    """
    Builds the gold.asset_failure_features data product for predictive maintenance.
    This engineers features from asset data for ML models. No worries, mate!
    """
    print("Building Asset Failure Features Gold table...")

    # Read from Silver layer (assuming dim_asset exists)
    silver_path = "s3a://silver/dim_asset"
    asset_df = spark.read.format("delta").load(silver_path)

    # Engineer features
    features_df = asset_df.withColumn(
        "asset_age_days", datediff(current_date(), col("install_date"))
    )
    features_df = features_df.withColumn(
        "days_since_last_maintenance",
        datediff(current_date(), col("last_maintenance_date")),
    )
    # Simulate a failure event for demonstration purposes
    features_df = features_df.withColumn(
        "failure_event", when(rand() > 0.95, 1).otherwise(0)
    )

    # Select final columns
    features_df = features_df.select(
        "asset_id",
        "asset_type",
        "manufacturer",
        "asset_age_days",
        "days_since_last_maintenance",
        "failure_event",
    )

    # Write to Gold layer
    gold_path = "s3a://gold/asset_failure_features"
    features_df.write.format("delta").mode("overwrite").save(gold_path)
    print("Asset Failure Features Gold table written successfully.")


def main():
    """
    Main aggregation function: builds both Gold data products from Silver layer.
    Robust error handling to keep things running smoothly.
    """
    spark = None
    try:
        # Fire up the Spark session
        spark = create_spark_session()
        print("Spark session ready. Starting Gold layer aggregations...")

        # Build the NRW summary
        build_nrw_summary_gold(spark)

        # Build the asset features
        build_asset_features_gold(spark)

        print("All Gold tables built successfully. Cheers!")

        print("ETL to Gold layer complete. Emitting lineage to DataHub...")

        try:
            # Initialise the DataHub Emitter
            emitter = DatahubRestEmitter(gms_server="http://datahub-gms:8082")

            # Define our Silver tables (Upstream)
            silver_fct_meter_urn = make_dataset_urn(platform="delta-lake", name="silver.fct_meter_readings")
            silver_dim_asset_urn = make_dataset_urn(platform="delta-lake", name="silver.dim_asset")

            # Define our new Gold tables (Downstream)
            gold_nrw_urn = make_dataset_urn(platform="delta-lake", name="gold.nrw_daily_summary")
            gold_asset_urn = make_dataset_urn(platform="delta-lake", name="gold.asset_failure_features")

            # Create the lineage relationships
            # 1. Lineage from Silver tables -> nrw_daily_summary
            lineage_mce_nrw = make_lineage_mce(
                upstream_urns=[silver_fct_meter_urn], # Add dim_asset if you join it
                downstream_urn=gold_nrw_urn
            )
            
            # 2. Lineage from Silver tables -> asset_failure_features
            lineage_mce_asset = make_lineage_mce(
                upstream_urns=[silver_dim_asset_urn], # Assuming both are used
                downstream_urn=gold_asset_urn
            )

            # Emit the metadata to DataHub
            emitter.emit_mce(lineage_mce_nrw)
            emitter.emit_mce(lineage_mce_asset)
            print("Successfully emitted lineage to DataHub.")

        except Exception as e:
            print(f"WARNING: Failed to emit lineage to DataHub: {e}")
            pass

    except Exception as e:
        print(f"Strewth! An error occurred during aggregation: {e}")
        raise  # Re-raise to ensure proper failure handling
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped. Have a good one!")


if __name__ == "__main__":
    main()
