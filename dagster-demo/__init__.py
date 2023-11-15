from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
    load_assets_from_modules,
)

from dagster_aws.s3.resources import s3_resource

from dagster_dbt import DbtCliResource

from dagster_polars import PolarsParquetIOManager
from .assets.yellow_taxi import dbt, extract_and_cleanse, reporting

from .helpers.commons import base_dir_io_manager, DBT_PROJECT_PATH, DBT_PROFILES
import os


# Assets
extract_trip_assets = load_assets_from_modules([extract_and_cleanse])
dbt_assets = load_assets_from_modules([dbt])
reporting_assets = load_assets_from_modules([reporting])

# Jobs
daily_job = define_asset_job(
    name="daily_job", selection=AssetSelection.keys("get_top_100")
)
monthly_job = define_asset_job(
    name="monthly_job",
    selection=AssetSelection.all()
    - AssetSelection.groups("dbt_seeds")
    - AssetSelection.keys("get_top_100"),
)
yearly_job = define_asset_job(
    name="yearly_job", selection=AssetSelection.groups("dbt_seeds")
)

# Schedules
monthly_refresh_schedule = ScheduleDefinition(
    job=monthly_job,
    cron_schedule="0 0 5 * *",
)

once_in_a_year = ScheduleDefinition(
    job=yearly_job,
    cron_schedule="0 0 5 1 1",
)

daily = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 0 * * *",
)

jobs = [monthly_job, yearly_job, daily_job]
schedules = [monthly_refresh_schedule, once_in_a_year, daily]

# Resources
s3 = s3_resource.configured(
    {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
)

# Definitions
defs = Definitions(
    assets=[*extract_trip_assets, *dbt_assets, *reporting_assets],
    resources={
        "s3": s3,
        "file_io_manager": PolarsParquetIOManager(base_dir=base_dir_io_manager),
        "dbt": DbtCliResource(
            project_dir=os.fspath(DBT_PROJECT_PATH),
            profiles_dir=os.fspath(DBT_PROFILES),
        ),
    },
    jobs=jobs,
    schedules=schedules,
)
