import requests
import xmltodict
from enum import Enum
from typing import List, Dict


class ThingTypes(Enum):
    BOARDGAME = "boardgame"
    BOARDGAMEEXPANSION = "boardgameexpansion"
    BOARDGAMEACCESSORY = "boardgameaccessory"


API_URL = "https://www.boardgamegeek.com/xmlapi2/thing?type={}&id={}"


def get_thing_dict(thingtype: ThingTypes, thingid: int | List) -> Dict:
    url = API_URL.format(
        thingtype.value,
        thingid if isinstance(thingid, int) else ",".join(map(str, thingid)),
    )
    response = requests.get(url)
    item = xmltodict.parse(response.text)
    """
    ret_val = []
    for i in item.get('items',{}).get('item',[]):
        ret_val.append(cleanse_response(i))
    return ret_val
    """
    return item


def cleanse_response(item: Dict) -> Dict:
    item_info_raw = item.copy()

    drop_keys = [
        "thumbnail",
        "image",
    ]

    for key in drop_keys:
        item_info_raw.pop(key, None)

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
        item_info_raw[value] = item_info_raw.pop(key)

    item_info_raw["name"] = (
        item_info_raw["name"]["@value"]
        if isinstance(item_info_raw["name"], dict)
        else item_info_raw["name"][0]["@value"]
    )
    return item_info_raw


def extract_list(thingtype: ThingTypes, arr_of_ids: List) -> List[Dict]:
    try:
        return get_thing_dict(thingtype, arr_of_ids)
    except Exception as e:
        print(e)
        batch_size = 10
        arr = [
            arr_of_ids[i : i + batch_size]
            for i in range(0, len(arr_of_ids), batch_size)
        ]
        result = []
        for j in arr:
            result = result + get_thing_dict(thingtype, j)
    return result


if __name__ == "__main__":
    print(get_thing_dict(ThingTypes.BOARDGAME, [2356]))
    """

    processing = []
    max_elem = 100000
    n = 100
    l = list(range(1,max_elem))

    from functools import partial
    from multiprocessing import Pool

    boardgame_proc_partial = partial(extract_list, ThingTypes.BOARDGAME)

    proc = [l[i:i + n] for i in range(0, max_elem, n)]
    with Pool(20) as p:
        processing.append(p.map(boardgame_proc_partial, proc))
    from itertools import chain

    processing = list(chain.from_iterable(list(chain.from_iterable(processing))))


    df = pd.DataFrame(processing)
    print(df.head(10))
    print(df.dtypes)
    df.head(10).to_parquet('bgg.parquet', index=False)
    """
