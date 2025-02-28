import pandas as pd
import numpy as np
import datetime
import requests_cache
from retry_requests import retry
from logging import basicConfig, getLogger, INFO

from andersan import tile
import andersan.archive.openmeteo as archive

try:
    from .sqlitedictcache import sqlitedict_cache
    from .__init__ import Neighbors, prefecture_ranges
except:
    # for test()
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


# @lru_cache
# @shelf_cache("openmeteo")
@sqlitedict_cache("openmeteo")  # vscodeで中身をチェックできる分、こちらのほうが便利
def tiles_(target_prefecture: str, datestr: str, zoom: int) -> pd.DataFrame:
    logger = getLogger()

    if target_prefecture not in Neighbors:  # 神奈川以外はまだ動かない
        return None

    # 地理院メッシュの間隔
    pref_range = np.array(prefecture_ranges[target_prefecture])  # lon,lat
    tiles, shape = tile.tiles(zoom, pref_range)

    lonlats = tile.lonlat(xy=tiles, zoom=zoom)
    # Setup the cache and retry mechanism
    cache_session = requests_cache.CachedSession("airpollution")
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    # dt = datetime.datetime.fromisoformat(isodate)
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": ",".join([f"{x:.4f}" for x in lonlats[:, 1]]),
        "longitude": ",".join([f"{x:.4f}" for x in lonlats[:, 0]]),
        "hourly": ",".join(OPENMETEO_ITEMS),
        "start_date": datestr,
        "end_date": datestr,
        "timezone": "Asia/Tokyo",
    }
    logger.debug(params)
    response = retry_session.get(url, params=params)
    logger.info(response)
    logger.debug(f"Cached: {response.from_cache}")
    data = response.json()

    # データを格納するリスト
    all_forecast_data = []

    # 測定量、時刻、緯度経度の3次元の配列でいいか。
    # dataの構造に沿って、まず緯度経度、物質量x時間で作表する。
    for elem, (x, y) in zip(data, tiles):
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(elem["hourly"]["time"][0]),
                periods=len(elem["hourly"]["time"]),
                freq="H",
                tz="Asia/Tokyo",
            ),
            "X": x,
            "Y": y,
            "Z": zoom,
        } | {item: elem["hourly"][item] for item in OPENMETEO_ITEMS}
        hourly_dataframe = pd.DataFrame(data=hourly_data)
        all_forecast_data.append(hourly_dataframe)

    all_forecast_dataframe = pd.concat(all_forecast_data, ignore_index=True)

    return all_forecast_dataframe


def tiles(target_prefecture: str, isodate: str, zoom: int) -> pd.DataFrame:
    # ここで、isodateに時刻が含まれる場合に日付けだけに修正する。
    # そうしないと、キャッシュに同じデータが24個も保管されてしまう。
    dt = datetime.datetime.fromisoformat(isodate)
    datestr = dt.strftime("%Y-%m-%d")

    if datetime.datetime.fromisoformat(
        dt.strftime("%Y-%m-%dT00:00:00+09:00")
    ) < datetime.datetime.fromisoformat("2021-04-04T00:00:00+09:00"):
        # use archived data of air monitor, which is provided by archive/openmeteo.py
        return archive.tiles_(target_prefecture, datestr, zoom)

    return tiles_(target_prefecture, datestr, zoom)


def test():
    basicConfig(level=INFO)
    logger = getLogger()
    df = tiles("kanagawa", "2015-03-31", zoom=12)
    logger.info(df)
    df = tiles("kanagawa", "2025-02-27", zoom=12)
    logger.info(df)


if __name__ == "__main__":
    test()
