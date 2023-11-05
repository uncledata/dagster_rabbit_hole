from dagster import asset, AssetIn, MetadataValue
from ...helpers.commons import (
    partitions_def,
    assets_dir_for_parquets,
    columns_names_mapping,
    redefine_dir_for_parquets,
    assets_dir_for_dirty_parquets,
    bucket_name,
    get_path_to_write_s3,
)

import requests
import polars as pl
import s3fs


@asset(partitions_def=partitions_def, required_resource_keys={"s3"})
def yellow_tripdata_raw_parquet(context) -> str:
    partition_date_str = context.asset_partition_key_for_output()
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{partition_date_str}.parquet"
    r = requests.get(url)
    data = r.content
    key = redefine_dir_for_parquets(
        assets_dir_for_parquets, file_name=f"yellow_taxi_raw_{partition_date_str}"
    )
    s3_client = context.resources.s3
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
    file_full_path = "s3://" + bucket_name + "/" + key
    return file_full_path


@asset(
    partitions_def=partitions_def,
    # ins={"parquet_path": AssetIn("yellow_tripdata_raw_parquet")},
    io_manager_key="file_io_manager",
)
def yellow_taxi_raw_data_frame(
    context, yellow_tripdata_raw_parquet: str
) -> pl.DataFrame:
    context.log.info(yellow_tripdata_raw_parquet)
    partition_period = context.asset_partition_key_for_output()
    pl_df = (
        pl.read_parquet(yellow_tripdata_raw_parquet)
        .rename(columns_names_mapping)
        .with_columns(pl.lit(partition_period).alias("period"))
    )
    return pl_df


def filter_polars_df(df: pl.DataFrame, is_clean: bool) -> pl.DataFrame:
    base_filter = (
        (pl.col("vendor_id").is_in([1, 2]))
        & (pl.col("rate_code_id").is_in([1, 2, 3, 4, 5, 6]))
        & (pl.col("store_and_fwd_flag").is_in(["Y", "N"]))
        & (pl.col("payment_type").is_in([1, 2, 3, 4, 5, 6]))
        & (pl.col("trip_distance") > 0)
        & (pl.col("fare_amount") > 0)
        & (pl.col("tpep_pickup_datetime").is_not_null())
        & (pl.col("tpep_dropoff_datetime").is_not_null())
        & (pl.col("passenger_count") > 0)
    )
    return df.filter(base_filter) if is_clean else df.filter(~base_filter)


@asset(
    partitions_def=partitions_def, ins={"raw_df": AssetIn("yellow_taxi_raw_data_frame")}
)
def yellow_taxi_fact(context, raw_df: pl.DataFrame) -> None:
    partition_period = context.asset_partition_key_for_output()
    df = filter_polars_df(raw_df, is_clean=True)
    fs = s3fs.S3FileSystem()
    destination = get_path_to_write_s3(
        bucket_name=bucket_name,
        key=redefine_dir_for_parquets(
            assets_dir_for_parquets, file_name=f"yellow_taxi_clean_{partition_period}"
        ),
    )

    # write parquet
    with fs.open(destination, mode="wb") as f:
        df.write_parquet(f)
    context.add_output_metadata(
        metadata={
            "num_records": df.height,  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown(index=False)),
            "statistics": MetadataValue.md(df.describe().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )


@asset(
    partitions_def=partitions_def, ins={"raw_df": AssetIn("yellow_taxi_raw_data_frame")}
)
def yellow_taxi_dirty(context, raw_df: pl.DataFrame) -> None:
    partition_period = context.asset_partition_key_for_output()
    df = filter_polars_df(raw_df, is_clean=False)
    fs = s3fs.S3FileSystem()
    destination = get_path_to_write_s3(
        bucket_name=bucket_name,
        key=redefine_dir_for_parquets(
            assets_dir_for_dirty_parquets,
            file_name=f"yellow_taxi_dirty_{partition_period}",
        ),
    )

    with fs.open(destination, mode="wb") as f:
        df.write_parquet(f)

    context.add_output_metadata(
        metadata={
            "num_records": df.height,  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown(index=False)),
            "statistics": MetadataValue.md(df.describe().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )


extract_and_cleanse_assets = [
    yellow_tripdata_raw_parquet,
    yellow_taxi_raw_data_frame,
    yellow_taxi_fact,
    yellow_taxi_dirty,
]
