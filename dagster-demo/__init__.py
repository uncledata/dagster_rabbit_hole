from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)

from dagster_aws.s3.resources import s3_resource

from dagster_polars import PolarsParquetIOManager
from .assets.yellow_taxi.extract_and_cleanse import extract_and_cleanse_assets
from .helpers.commons import base_dir_io_manager
import os

monthly_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"),
    cron_schedule="0 0 5 * *",
)

s3 = s3_resource.configured(
    {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
)

defs = Definitions(
    assets=extract_and_cleanse_assets,
    resources={
        "s3": s3,
        "file_io_manager": PolarsParquetIOManager(base_dir=base_dir_io_manager),
    },
    schedules=[monthly_refresh_schedule],
)
