import pandas as pd
import numpy as np
import datetime
import requests_cache
import requests
from retry_requests import retry
from logging import basicConfig, getLogger, INFO, DEBUG
import pytz
import json

from andersan import tile

try:
    import andersan.archive.openmeteo as archive
    from andersan.sqlitedictcache import sqlitedict_cache
    from andersan import Neighbors, prefecture_ranges
except:
    # for test()
    import archive.openmeteo as archive
    from sqlitedictcache import sqlitedict_cache
    from __init__ import Neighbors, prefecture_ranges

OPENMETEO_ITEMS = [
    "temperature_2m",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "pressure_msl",
    "shortwave_radiation",
]


OPENWM_ITEMS = (
    "temp",  # in K
    "weather",  # これは複層になってる。3桁の数字で、601がSnow。互換性はあるか?
    "clouds",  # percentage
    "wind_speed",  # m/s
    "pressure",  # hPa
    # "shortwave_radiation", # このデータはない。
)

import toml

with open("/AIR/andersan/api_keys.toml") as f:
    api_keys = toml.load(f)
API_key = api_keys["openweathermap"]


# @lru_cache
# @shelf_cache("openmeteo")
# @sqlitedict_cache(
#     "openweathermap"
# )  # vscodeで中身をチェックできる分、こちらのほうが便利
def tiles(target_prefecture: str, zoom: int) -> pd.DataFrame:
    logger = getLogger()

    if target_prefecture not in Neighbors:  # 神奈川以外はまだ動かない
        return None

    # 地理院メッシュの間隔
    pref_range = np.array(prefecture_ranges[target_prefecture])  # lon,lat
    tiles, _ = tile.tiles(zoom, pref_range)

    lonlats = tile.lonlat(xy=tiles, zoom=zoom)

    # OpenWeathermapのOne Call APIには時刻指定がない。
    # つまり、同じURLでも、アクセスする時刻によって内容が変化する。
    # 永久にCacheしてはいけない。1時間程度で忘れてしまう必要がある。
    cache_session = requests_cache.CachedSession("openweathermap", expires_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    # one call APIは時刻を指定できない?
    #
    excludes = f"&exclude=current,minutely,daily,alerts"

    # データを格納するリスト
    all_forecast_data = []

    for lat in lonlats[:, 1]:
        for lon in lonlats[:, 0]:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat:.4f}&lon={lon:.4f}&appid={API_key}{excludes}"
            response = retry_session.get(url)
            # response = requests.get(url)
            data = response.json()
            # hourly_data = {
            #     "date": pd.date_range(
            #         start=pd.to_datetime(elem["hourly"]["time"][0]),
            #         periods=len(elem["hourly"]["time"]),
            #         freq="H",
            #         tz="Asia/Tokyo",
            #     ),
            #     "X": x,
            #     "Y": y,
            #     "Z": zoom,
            # } | {item: elem["hourly"][item] for item in OPENMETEO_ITEMS}
            hourly_dataframe = pd.DataFrame(data=data["hourly"])
            # logger.debug(hourly_dataframe)
            cols = list(OPENWM_ITEMS)
            logger.debug(cols)
            logger.debug(hourly_dataframe.columns)
            logger.debug(hourly_dataframe[cols])
            assert False
            all_forecast_data.append(hourly_dataframe)

    all_forecast_dataframe = pd.concat(all_forecast_data, ignore_index=True)

    return all_forecast_dataframe


def test():
    basicConfig(level=DEBUG)
    logger = getLogger()
    df = tiles("kanagawa", zoom=12)
    logger.info(df)
    df = tiles("kanagawa", zoom=12)
    logger.info(df)


if __name__ == "__main__":
    test()
