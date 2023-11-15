from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
)
from dagster import AssetExecutionContext, MetadataValue, asset
import polars as pl

from ...helpers.commons import partitions_def
import json
from pathlib import Path

from ...helpers.commons import DBT_PROJECT_PATH, engine
import matplotlib.pyplot as plt
from dagster_dbt import get_asset_key_for_model
import base64
from io import BytesIO


dbt_manifest_path = Path(DBT_PROJECT_PATH).resolve().joinpath("target", "manifest.json")
dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=dagster_dbt_translator,
    partitions_def=partitions_def,
    select="resource_type:model",
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info(context.partition_key)
    dbt_vars = {"period": context.partition_key}
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]

    yield from dbt.cli(dbt_build_args, context=context).stream()


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=dagster_dbt_translator,
    select="resource_type:seed",
)
def dbt_assets_seeds(context, dbt: DbtCliResource):
    yield from dbt.cli(["seed"], context=context).stream()


@asset(
    compute_kind="polars",
    partitions_def=partitions_def,
    deps=get_asset_key_for_model([dbt_project_assets], "fct_trips"),
    group_name="reporting",
)
def get_counts_per_month(context):
    con = engine.connect()

    counters_df = pl.read_database(
        f"""SELECT date(tpep_pickup_datetime) as dt, count(1) as cnt
        FROM fct_trips where period = '{context.partition_key}'
        group by 1 order by 1 desc""",
        con,
    )
    plt.xticks(rotation=90)
    plt.plot(counters_df["DT"], counters_df["CNT"])
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![Hello World](data:image/png;base64,{image_data.decode()})"
    context.log.info(md_content)
    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(
                counters_df.to_pandas().to_markdown(index=False)
            ),
            "statistics": MetadataValue.md(
                counters_df.describe().to_pandas().to_markdown()
            ),
            "plot": MetadataValue.md(md_content),
        }
    )
