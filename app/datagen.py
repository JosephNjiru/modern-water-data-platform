import json
import csv
import random
import io
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from faker import Faker
from minio import Minio

# Initialise Faker for Australian data, makes it more authentic, mate.
fake = Faker("en_AU")


def create_minio_client():
    """
    Creates and returns a MinIO client configured for our local Docker service.
    This simulates connecting to an enterprise S3-compatible storage system.
    """
    client = Minio(
        endpoint="localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False,
    )
    return client


def generate_sensus_data(num_records: int, minio_client):
    """
    Generates synthetic 'Sensus iPERL' smart meter readings with a nested JSON structure.
    This function simulates realistic, seasonal water consumption data and uploads directly to MinIO.

    Args:
        num_records (int): The number of meter readings to generate.
        minio_client: The MinIO client instance.
    """
    print(f"Righto, generating {num_records} Sensus iPERL meter readings...")
    data = []

    # Let's create a realistic date range for our readings.
    base_date = datetime.now()
    date_range = pd.to_datetime(
        [base_date - timedelta(hours=i) for i in range(num_records)]
    )

    # Let's whip up some seasonal flow data. We'll use a sine wave for daily and weekly patterns.
    # Water usage is usually higher in the evening and on weekends, yeah?
    flow = (
        100  # Base flow
        + 50 * np.sin(2 * np.pi * date_range.hour / 24)  # Daily cycle
        + 30 * np.sin(2 * np.pi * date_range.dayofweek / 7)  # Weekly cycle
        + np.random.rand(num_records) * 20  # Bit of random noise to keep it real
    )

    meter_ids = [
        f"SEN-{fake.unique.random_number(digits=8)}" for _ in range(num_records // 100)
    ]

    for i in range(num_records):
        record = {
            "meter_id": random.choice(meter_ids),
            "timestamp_utc": date_range[i].isoformat() + "Z",
            "reading_data": {
                "flow_kl": round(
                    max(0, flow[i]), 4
                )  # Can't have negative water flow, can we?
            },
        }
        data.append(record)

    # Generate JSON string in memory
    json_data = json.dumps(data, indent=4)
    json_bytes = json_data.encode("utf-8")

    # Upload to MinIO Bronze layer
    bucket_name = "bronze"
    object_name = "smart-meter/sensus_readings.json"
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )
        print(
            f"Successfully uploaded Sensus data to minio://{bucket_name}/{object_name}. Good on ya!"
        )
    except Exception as e:
        print(f"Bugger! Couldn't upload Sensus data to MinIO. Error: {e}")


def generate_itron_data(num_records: int, minio_client):
    """
    Generates synthetic 'Itron Temetra' smart meter readings with a flat JSON structure.
    Note the different structure and units (cubic metres) compared to Sensus.
    Uploads directly to MinIO Bronze layer.

    Args:
        num_records (int): The number of meter readings to generate.
        minio_client: The MinIO client instance.
    """
    print(f"Now for the Itron data. Generating {num_records} records...")
    data = []
    device_ids = [
        f"ITR-{fake.unique.random_number(digits=10)}" for _ in range(num_records // 100)
    ]

    for _ in range(num_records):
        # Itron readings are often in cubic metres (m3), where 1 m3 = 1 kL.
        consumption_m3 = round(random.uniform(0.01, 1.5), 4)
        record = {
            "itron_device_id": random.choice(device_ids),
            "read_datetime": fake.date_time_this_year().isoformat(),
            "consumption_m3": consumption_m3,
        }
        data.append(record)

    # Generate JSON string in memory
    json_data = json.dumps(data, indent=4)
    json_bytes = json_data.encode("utf-8")

    # Upload to MinIO Bronze layer
    bucket_name = "bronze"
    object_name = "smart-meter/itron_readings.json"
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )
        print(f"Itron data uploaded to minio://{bucket_name}/{object_name}. Sweet as.")
    except Exception as e:
        print(f"Strewth! Failed to upload Itron data to MinIO. Error: {e}")


def generate_asset_data(num_records: int, minio_client):
    """
    Generates a CSV file of synthetic asset maintenance data using Faker.
    This simulates a basic export from a utility's asset management system.
    Uploads directly to MinIO Bronze layer.

    Args:
        num_records (int): The number of asset records to generate.
        minio_client: The MinIO client instance.
    """
    print(f"Creating {num_records} asset maintenance records...")
    headers = [
        "asset_id",
        "install_date",
        "asset_type",
        "material",
        "last_maintenance_date",
        "failure_event",
    ]

    asset_types = ["Pipe", "Pump", "Valve", "Meter"]
    materials = ["PVC", "Cast Iron", "Steel", "Copper", "Ductile Iron"]

    # Generate CSV data in memory
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    for _ in range(num_records):
        asset_id = f"ASSET-{fake.unique.random_number(digits=6)}"
        install_date = fake.date_between(start_date="-20y", end_date="-1y")
        last_maintenance_date = fake.date_between(
            start_date=install_date, end_date="today"
        )

        # A small chance of a failure event.
        failure_event = 1 if random.random() < 0.05 else 0

        writer.writerow(
            [
                asset_id,
                install_date.isoformat(),
                random.choice(asset_types),
                random.choice(materials),
                last_maintenance_date.isoformat(),
                failure_event,
            ]
        )

    # Get CSV data as bytes
    csv_data = csv_buffer.getvalue()
    csv_bytes = csv_data.encode("utf-8")

    # Upload to MinIO Bronze layer
    bucket_name = "bronze"
    object_name = "asset-logs/asset_maintenance.csv"
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv",
        )
        print(
            f"Asset maintenance data uploaded to minio://{bucket_name}/{object_name}. No worries."
        )
    except Exception as e:
        print(f"Drats! Couldn't upload asset data to MinIO. Error: {e}")


def generate_asset_master_data(num_records: int, minio_client):
    """
    Generates a JSON file of synthetic asset master data.
    This simulates a master data repository for physical assets.
    Uploads directly to MinIO Bronze layer.

    Args:
        num_records (int): The number of asset records to generate.
        minio_client: The MinIO client instance.
    """
    print(f"Creating {num_records} asset master records...")
    data = []
    asset_types = ["Pipe", "Pump", "Valve", "Meter"]
    manufacturers = ["AquaFlow", "HydroTech", "FlowServe", "Badger Meter"]

    for _ in range(num_records):
        install_date = fake.date_between(start_date="-15y", end_date="-2y")
        record = {
            "asset_id": f"MTR-{fake.unique.random_number(digits=7)}",
            "asset_type": random.choice(asset_types),
            "install_date": install_date.isoformat(),
            "last_maintenance_date": fake.date_between(
                start_date=install_date, end_date="today"
            ).isoformat(),
            "manufacturer": random.choice(manufacturers),
            "model_number": f"{random.choice(['A', 'B', 'X'])}-{random.randint(1000, 9999)}",
            "location": {
                "latitude": float(fake.latitude()),
                "longitude": float(fake.longitude()),
            },
        }
        data.append(record)

    # Generate JSON string in memory
    json_data = json.dumps(data, indent=4)
    json_bytes = json_data.encode("utf-8")

    # Upload to MinIO Bronze layer
    bucket_name = "bronze"
    object_name = "asset/asset_master_data.json"
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )
        print(
            f"Successfully uploaded asset master data to minio://{bucket_name}/{object_name}."
        )
    except Exception as e:
        print(f"Bummer! Couldn't upload asset master data to MinIO. Error: {e}")


def main():
    """
    Main function to orchestrate the data generation.
    It'll initialise MinIO client and then run all the generator functions.
    """
    print(
        "Starting synthetic data generation for the Enterprise Water-Systems Lakehouse."
    )

    # Initialise MinIO client
    try:
        minio_client = create_minio_client()
        print("MinIO client connected. Ready to upload data to Bronze layer.")
    except Exception as e:
        print(f"Could not connect to MinIO. Error: {e}")
        return  # Can't proceed without MinIO, so we bail.

    # --- Set the number of records you want for each file ---
    NUM_SENSUS_RECORDS = 5000
    NUM_ITRON_RECORDS = 3000
    NUM_ASSET_RECORDS = 1000
    NUM_ASSET_MASTER_RECORDS = 1500
    # ---------------------------------------------------------

    generate_sensus_data(NUM_SENSUS_RECORDS, minio_client)
    generate_itron_data(NUM_ITRON_RECORDS, minio_client)
    generate_asset_data(NUM_ASSET_RECORDS, minio_client)
    generate_asset_master_data(NUM_ASSET_MASTER_RECORDS, minio_client)

    print(
        "\nAll data generation tasks are complete. Data landed in Bronze layer. Have a good one!"
    )


if __name__ == "__main__":
    main()
