from dagster import MonthlyPartitionsDefinition, DailyPartitionsDefinition
from dagster import file_relative_path
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

DBT_PROJECT_PATH = file_relative_path(__file__, "/opt/dagster/dagster_home/dwh")
DBT_PROFILES = file_relative_path(__file__, "/opt/dagster/dagster_home/")

partitions_def = MonthlyPartitionsDefinition(start_date="2022-12", fmt="%Y-%m")
daily_partitions_def = DailyPartitionsDefinition(
    start_date="2022-12-01", fmt="%Y-%m-%d"
)

bucket_name = "tomas-data-lake"


def redefine_dir_for_parquets(dir, file_name: str) -> str:
    return dir.format(file_name=file_name)


base_dir_io_manager = "tmp/io_manager/"
assets_dir_for_raw_parquets = "assets_data/yellow_taxi_raw/{file_name}.parquet"
assets_dir_for_clean_parquets = "assets_data/yellow_taxi_clean/{file_name}.parquet"
assets_dir_for_dirty_parquets = "dirty_assets_data/yellow_taxi_raw/{file_name}.parquet"


connection = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ["SNOWFLAKE_WH"],
    "database": os.environ["SNOWFLAKE_DB"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
    "role": os.environ["SNOWFLAKE_ROLE"],
}
engine = create_engine(URL(**connection))


def get_path_to_write_s3(bucket_name: str, key: str) -> str:
    return "s3://" + bucket_name + "/" + key


columns_names_mapping = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}
