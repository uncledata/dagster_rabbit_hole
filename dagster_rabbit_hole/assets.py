from dagster import asset, AssetIn, PartitionSetDefinition, Partition
from enum import Enum
import requests
import xmltodict
from typing import List, Dict

PARTITION_RANGE = range(1, 10001)


class ThingTypes(Enum):
    BOARDGAME = "boardgame"
    BOARDGAMEEXPANSION = "boardgameexpansion"
    BOARDGAMEACCESSORY = "boardgameaccessory"


API_URL = "https://www.boardgamegeek.com/xmlapi2/thing?type={}&id={}"


def _create_partition(partition):
    return Partition(partition_name=str(partition), run_config={"partition": partition})


partitions_def = PartitionSetDefinition(
    name="my_partitions",
    pipeline_name="my_pipeline",
    partition_fn=_create_partition,
    partitions=PARTITION_RANGE,
)


def call_api_w_thing_type(thing_type: ThingTypes, thing_id: int | List) -> Dict:
    url = API_URL.format(
        thing_type.value,
        thing_id if isinstance(thing_id, int) else ",".join(map(str, thing_id)),
    )
    response = requests.get(url)
    item = xmltodict.parse(response.text)
    return item


@asset(name="boardgame_raw", partitions_def=partitions_def)
def api_response(context):
    item = call_api_w_thing_type(ThingTypes.BOARDGAME, context.partition_key)
    return item


@asset(
    name="boardgame_cleansed",
    ins={"raw_api_dict": AssetIn("boardgame_raw")},
    partitions_def=partitions_def,
)
def cleanse_response(context, raw_api_dict):
    drop_keys = [
        "thumbnail",
        "image",
    ]

    for key in drop_keys:
        raw_api_dict.pop(key, None)

    to_rename = {
        "@id": "id",
        "@type": "type",
        "yearpublished": "year_published",
        "minplayers": "min_players",
        "maxplayers": "max_players",
        "playingtime": "play_time",
        "minplaytime": "min_playtime",
        "maxplaytime": "max_playtime",
        "minage": "min_age",
    }
    for key, value in to_rename.items():
        raw_api_dict[value] = raw_api_dict.pop(key)

    raw_api_dict["name"] = (
        raw_api_dict["name"]["@value"]
        if isinstance(raw_api_dict["name"], dict)
        else raw_api_dict["name"][0]["@value"]
    )
    return raw_api_dict
