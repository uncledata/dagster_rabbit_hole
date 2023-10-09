from dagster import Definitions, load_assets_from_modules

from assets import api_response, cleanse_response

all_assets = load_assets_from_modules([api_response, cleanse_response])

defs = Definitions(
    assets=all_assets,
)
