from dagster import asset, MetadataValue

import polars as pl


from ...helpers.commons import daily_partitions_def, engine


@asset(
    compute_kind="polars", partitions_def=daily_partitions_def, group_name="reporting"
)
def get_top_100(context):
    con = engine.connect()

    top_100 = pl.read_database(
        f"""SELECT * FROM fct_trips
        where date(tpep_pickup_datetime) = '{context.partition_key}'
        order by total_amount desc LIMIT 100""",
        con,
    )
    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(top_100.to_pandas().to_markdown(index=False)),
            "statistics": MetadataValue.md(
                top_100.describe().to_pandas().to_markdown()
            ),
        }
    )
