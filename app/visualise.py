from pyspark.sql import SparkSession
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd
import os

# Set professional colour palette and theme
professional_colours = [
    '#2E86AB',  # Blue
    '#A23B72',  # Magenta  
    '#F18F01',  # Orange
    '#C73E1D',  # Red
    '#6A994E',  # Green
    '#577590',  # Blue-grey
    '#F7931E',  # Amber
    '#4CAF50',  # Bright green
]

# Set default template for all plots
pio.templates.default = "plotly_white"


def create_spark_session():
    """
    Creates and configures a SparkSession for reading Delta tables from MinIO.
    This is our local S3-compatible storage for the lakehouse. Fair dinkum setup!
    """
    spark = (
        SparkSession.builder.appName("EWL Visualisation")
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


def plot_nrw_timeseries(df):
    """
    Creates a line chart of NRW percentage over time using Plotly.
    This visualises our key water loss metric with professional styling.
    """
    # Ensure report_date is datetime
    df["report_date"] = pd.to_datetime(df["report_date"])

    fig = px.line(
        df,
        x="report_date",
        y="nrw_percent",
        title="Daily non-revenue water (NRW) percentage",
        labels={"nrw_percent": "NRW %", "report_date": "Date"},
        color_discrete_sequence=professional_colours
    )
    
    # Professional styling
    fig.update_layout(
        title_font_size=16,
        title_x=0.5,
        xaxis_title="Date", 
        yaxis_title="NRW %",
        font=dict(size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        margin=dict(l=60, r=40, t=60, b=60)
    )
    
    # Add grid lines
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')

    # Save as HTML
    html_path = "/app/visualisations/nrw_timeseries.html"
    fig.write_html(html_path)
    print(f"NRW timeseries plot saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/nrw_timeseries.png"
    fig.write_image(png_path, width=1200, height=600, scale=2)
    print(f"NRW timeseries plot saved to {png_path}")


def plot_asset_failures(df):
    """
    Creates a histogram of asset age distribution by failure status.
    This helps understand failure patterns with professional visualisation.
    """
    fig = px.histogram(
        df,
        x="asset_age_days",
        color="failure_event",
        facet_col="failure_event",
        title="Asset age distribution by failure status",
        labels={"asset_age_days": "Asset age (days)", "failure_event": "Failure event"},
        color_discrete_sequence=professional_colours
    )
    
    # Professional styling
    fig.update_layout(
        title_font_size=16,
        title_x=0.5,
        xaxis_title="Asset age (days)",
        font=dict(size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=60, r=40, t=60, b=60)
    )
    
    # Update facet titles
    fig.for_each_annotation(lambda a: a.update(text=a.text.replace("failure_event=", "Failure event: ")))

    # Save as HTML
    html_path = "/app/visualisations/asset_failure_histogram.html"
    fig.write_html(html_path)
    print(f"Asset failure histogram saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/asset_failure_histogram.png"
    fig.write_image(png_path, width=1200, height=600, scale=2)
    print(f"Asset failure histogram saved to {png_path}")


def plot_asset_failures_by_material(df):
    """
    Creates a bar chart showing the count of failure events grouped by asset type.
    """
    # Filter for failures and group by asset_type (matches gold.asset_failure_features schema)
    failures_df = (
        df[df["failure_event"] == 1]
        .groupby("asset_type")
        .size()
        .reset_index(name="count")
    )

    fig = px.bar(
        failures_df,
        x="asset_type",
        y="count",
        title="Asset failures by asset type",
        labels={"count": "Number of failures", "asset_type": "Asset type"},
        color_discrete_sequence=professional_colours
    )
    
    # Professional styling
    fig.update_layout(
        title_font_size=16,
        title_x=0.5,
        xaxis_title="Asset type", 
        yaxis_title="Number of failures",
        font=dict(size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=False,
        margin=dict(l=60, r=40, t=60, b=60)
    )
    
    # Add grid lines
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgrey')

    # Save as HTML
    html_path = "/app/visualisations/asset_failures_by_asset_type.html"
    fig.write_html(html_path)
    print(f"Asset failures by asset type plot saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/asset_failures_by_asset_type.png"
    fig.write_image(png_path, width=1200, height=600, scale=2)
    print(f"Asset failures by asset type plot saved to {png_path}")


def plot_monthly_nrw_trend(df):
    """
    Creates a line chart showing the average NRW percentage over time by month.
    """
    # Ensure report_date is datetime
    df["report_date"] = pd.to_datetime(df["report_date"])

    # Aggregate by month
    df["month"] = df["report_date"].dt.to_period("M").dt.to_timestamp()
    monthly_df = df.groupby("month")["nrw_percent"].mean().reset_index()
    
    fig = px.line(
        monthly_df,
        x="month",
        y="nrw_percent",
        title="Monthly Non-Revenue Water (NRW) Trend",
        labels={"nrw_percent": "Average NRW %", "month": "Month"},
    )
    fig.update_layout(xaxis_title="Month", yaxis_title="Average NRW %")

    # Save as HTML
    html_path = "/app/visualisations/monthly_nrw_trend.html"
    fig.write_html(html_path)
    print(f"Monthly NRW trend plot saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/monthly_nrw_trend.png"
    fig.write_image(png_path)
    print(f"Monthly NRW trend plot saved to {png_path}")


def plot_hourly_consumption_heatmap(spark):
    """
    Creates a heatmap showing average consumption by day of week and hour.
    """
    # Read from silver layer
    meter_path = "s3a://silver/fct_meter_readings"
    meter_df = spark.read.format("delta").load(meter_path).toPandas()

    # Ensure reading_timestamp is datetime (handle ISO format with Z timezone)
    meter_df["reading_timestamp"] = pd.to_datetime(meter_df["reading_timestamp"], format='ISO8601', utc=True)

    # Extract hour and day of week from reading_timestamp (matches Silver schema)
    meter_df["hour"] = meter_df["reading_timestamp"].dt.hour
    meter_df["day_of_week"] = meter_df["reading_timestamp"].dt.day_name()
    
    # Group and average
    heatmap_df = meter_df.groupby(['day_of_week', 'hour'])['consumption_kl'].mean().reset_index()
    
    # Pivot for heatmap
    heatmap_pivot = heatmap_df.pivot(index='day_of_week', columns='hour', values='consumption_kl')
    
    fig = px.imshow(
        heatmap_pivot,
        title="Average Hourly Water Consumption by Day",
        labels=dict(x="Hour of Day", y="Day of Week", color="Avg. Consumption (kL)"),
        x=heatmap_pivot.columns,
        y=heatmap_pivot.index,
    )
    fig.update_layout(xaxis_title="Hour of Day", yaxis_title="Day of Week")

    # Save as HTML
    html_path = "/app/visualisations/hourly_consumption_heatmap.html"
    fig.write_html(html_path)
    print(f"Hourly consumption heatmap saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/hourly_consumption_heatmap.png"
    fig.write_image(png_path)
    print(f"Hourly consumption heatmap saved to {png_path}")


def plot_meter_consumption_distribution(spark):
    """
    Creates a histogram of meter consumption distribution.
    """
    # Read from silver layer
    meter_path = "s3a://silver/fct_meter_readings"
    meter_df = spark.read.format("delta").load(meter_path).toPandas()
    
    fig = px.histogram(
        meter_df,
        x="consumption_kl",
        title="Meter Consumption Distribution",
        labels={"consumption_kl": "Consumption (kL)"},
    )
    fig.update_layout(xaxis_title="Consumption (kL)", yaxis_title="Frequency")

    # Save as HTML
    html_path = "/app/visualisations/meter_consumption_distribution.html"
    fig.write_html(html_path)
    print(f"Meter consumption distribution saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/meter_consumption_distribution.png"
    fig.write_image(png_path)
    print(f"Meter consumption distribution saved to {png_path}")


def plot_nrw_by_day_of_week(df):
    """
    Creates a bar chart of average NRW by day of week.
    """
    # Ensure report_date is datetime
    df["report_date"] = pd.to_datetime(df["report_date"])

    df["day_of_week"] = df["report_date"].dt.day_name()
    weekly_df = df.groupby("day_of_week")["nrw_percent"].mean().reset_index()
    
    fig = px.bar(
        weekly_df,
        x="day_of_week",
        y="nrw_percent",
        title="Average NRW by Day of Week",
        labels={"nrw_percent": "Average NRW %", "day_of_week": "Day of Week"},
    )
    fig.update_layout(xaxis_title="Day of Week", yaxis_title="Average NRW %")

    # Save as HTML
    html_path = "/app/visualisations/nrw_by_day_of_week.html"
    fig.write_html(html_path)
    print(f"NRW by day of week plot saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/nrw_by_day_of_week.png"
    fig.write_image(png_path)
    print(f"NRW by day of week plot saved to {png_path}")


def plot_asset_failure_rate_by_age(df):
    """
    Creates a bar chart of failure rate by asset age groups.
    """
    # Bin ages
    bins = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    labels = ['0-100', '100-200', '200-300', '300-400', '400-500', '500-600', '600-700', '700-800', '800-900', '900-1000']
    df['age_group'] = pd.cut(df['asset_age_days'], bins=bins, labels=labels, right=False)
    
    # Calculate failure rate
    failure_rate_df = df.groupby('age_group').agg(
        total_assets=('failure_event', 'count'),
        failures=('failure_event', 'sum')
    ).reset_index()
    failure_rate_df['failure_rate'] = failure_rate_df['failures'] / failure_rate_df['total_assets']
    
    fig = px.bar(
        failure_rate_df,
        x="age_group",
        y="failure_rate",
        title="Asset Failure Rate by Age Group",
        labels={"failure_rate": "Failure Rate", "age_group": "Age Group (Days)"},
    )
    fig.update_layout(xaxis_title="Age Group (Days)", yaxis_title="Failure Rate")

    # Save as HTML
    html_path = "/app/visualisations/asset_failure_rate_by_age.html"
    fig.write_html(html_path)
    print(f"Asset failure rate by age plot saved to {html_path}")

    # Save as PNG
    png_path = "/app/visualisations/asset_failure_rate_by_age.png"
    fig.write_image(png_path)
    print(f"Asset failure rate by age plot saved to {png_path}")


def main():
    """
    Main function to generate visualisations from Gold layer data.
    Reads Delta tables, converts to Pandas, and creates plots. Robust error handling included.
    """
    spark = None
    try:
        # Start Spark session
        spark = create_spark_session()
        print("Spark session created. Reading Gold layer data...")

        # Read NRW summary
        nrw_path = "s3a://gold/nrw_daily_summary"
        nrw_df = spark.read.format("delta").load(nrw_path).toPandas()
        print("NRW data loaded.")

        # Read asset features
        asset_path = "s3a://gold/asset_failure_features"
        asset_df = spark.read.format("delta").load(asset_path).toPandas()
        print("Asset data loaded.")

        # Ensure visualisations directory exists
        os.makedirs("/app/visualisations", exist_ok=True)

        # Generate plots
        plot_nrw_timeseries(nrw_df)
        plot_asset_failures(asset_df)
        plot_asset_failures_by_material(asset_df)
        plot_monthly_nrw_trend(nrw_df)
        plot_hourly_consumption_heatmap(spark)
        plot_meter_consumption_distribution(spark)
        plot_nrw_by_day_of_week(nrw_df)
        plot_asset_failure_rate_by_age(asset_df)

        print("All visualisations generated successfully. Cheers!")

    except Exception as e:
        print(f"Bugger! An error occurred: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")


if __name__ == "__main__":
    main()
