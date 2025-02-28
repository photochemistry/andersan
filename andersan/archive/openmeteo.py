# archived open-meteo-12データ
# om = "../edamame2/open-meteo-12/3628.1614.12.feather"
# D = pd.read_feather(om)
# D

import pandas as pd
import numpy as np
import datetime
from logging import basicConfig, getLogger, INFO

from andersan import tile as andersan_tile

try:
    from .sqlitedictcache import sqlitedict_cache
    from .__init__ import Neighbors, prefecture_ranges
except:
    # for test()
    from andersan.sqlitedictcache import sqlitedict_cache
    from andersan import Neighbors, prefecture_ranges

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
def tiles_(target_prefecture: str, datestr: str, zoom: int) -> pd.DataFrame:
    # 24時間分を返す?
    logger = getLogger()

    if target_prefecture not in Neighbors:  # 神奈川以外はまだ動かない
        return None

    # 地理院メッシュの間隔
    pref_range = np.array(prefecture_ranges[target_prefecture])  # lon,lat
    tiles, shape = andersan_tile.tiles(zoom, pref_range)

    all_forecast_dataframe = pd.DataFrame()
    Z = 12
    for X, Y in tiles:
        df = pd.read_feather(f"/AIR/edamame2/open-meteo-12/{X}.{Y}.{Z}.feather")
        start_date = datetime.datetime.fromisoformat(f"{datestr}T00:00:00+09:00")
        end_date = datetime.datetime.fromisoformat(f"{datestr}T23:59:59+09:00")
        df = df[df["date"].between(start_date, end_date)]
        all_forecast_dataframe = pd.concat(
            [all_forecast_dataframe, df], ignore_index=True
        )
    all_forecast_dataframe.loc[:, "Z"] = Z

    # # データを格納するリスト
    # all_forecast_data = []

    # # 測定量、時刻、緯度経度の3次元の配列でいいか。
    # # dataの構造に沿って、まず緯度経度、物質量x時間で作表する。
    # for elem, (x, y) in zip(data, tiles):
    #     hourly_data = {
    #         "date": pd.date_range(
    #             start=pd.to_datetime(elem["hourly"]["time"][0]),
    #             periods=len(elem["hourly"]["time"]),
    #             freq="H",
    #             tz="Asia/Tokyo",
    #         ),
    #         "X": x,
    #         "Y": y,
    #         "Z": zoom,
    #     } | {item: elem["hourly"][item] for item in OPENMETEO_ITEMS}
    #     hourly_dataframe = pd.DataFrame(data=hourly_data)
    #     all_forecast_data.append(hourly_dataframe)

    # all_forecast_dataframe = pd.concat(all_forecast_data, ignore_index=True)

    return all_forecast_dataframe


def tiles(target_prefecture: str, isodate: str, zoom: int) -> pd.DataFrame:
    # ここで、isodateに時刻が含まれる場合に日付けだけに修正する。
    # そうしないと、キャッシュに同じデータが24個も保管されてしまう。
    dt = datetime.datetime.fromisoformat(isodate)
    datestr = dt.strftime("%Y-%m-%d")
    return tiles_(target_prefecture, datestr, zoom)


def test():
    basicConfig(level=INFO)
    logger = getLogger()
    df = tiles("kanagawa", "2021-03-31", zoom=12)
    logger.info(df)


if __name__ == "__main__":
    test()
