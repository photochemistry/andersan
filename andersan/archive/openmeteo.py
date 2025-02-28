# archived open-meteo-12データ
# om = "../edamame2/open-meteo-12/3628.1614.12.feather"
# D = pd.read_feather(om)
# D

import pandas as pd
import numpy as np
import datetime
from logging import basicConfig, getLogger, INFO
import pytz

from andersan import tile as andersan_tile

try:
    from andersan.sqlitedictcache import sqlitedict_cache
    from andersan.__init__ import Neighbors, prefecture_ranges
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
@sqlitedict_cache(
    "archive_openmeteo"
)  # vscodeで中身をチェックできる分、こちらのほうが便利
def tiles(target_prefecture: str, datehour: str, hours:int, zoom: int) -> pd.DataFrame:
    # 24時間分を返す?
    logger = getLogger()

    if target_prefecture not in Neighbors:  # 神奈川以外はまだ動かない
        return None

    # ここで、isodateに時刻が含まれる場合に日付と時だけに修正する。
    dt = datetime.datetime.fromisoformat(datehour)

    # タイムゾーンを指定（例：日本時間）
    tz = pytz.timezone("Asia/Tokyo")

    # タイムゾーンを付与
    dt = tz.localize(dt)

    dt_start = dt.replace(minute=0, second=0, microsecond=0)
    dt_day = dt_start.replace(hour=0)
    dt_end = dt_start + datetime.timedelta(hours=hours-1)


    # 地理院メッシュの間隔
    pref_range = np.array(prefecture_ranges[target_prefecture])  # lon,lat
    tiles, shape = andersan_tile.tiles(zoom, pref_range)

    all_forecast_dataframe = pd.DataFrame()
    Z = 12
    for X, Y in tiles:
        df = pd.read_feather(f"/AIR/edamame2/open-meteo-12/{X}.{Y}.{Z}.feather")
        df = df[df["date"].between(dt_start, dt_end)]
        all_forecast_dataframe = pd.concat(
            [all_forecast_dataframe, df], ignore_index=True
        )
    all_forecast_dataframe.loc[:, "Z"] = Z

    return all_forecast_dataframe


def test():
    basicConfig(level=INFO)
    logger = getLogger()
    df = tiles("kanagawa", "2021-03-31T06", hours=7, zoom=12)
    logger.info(df)


if __name__ == "__main__":
    test()
