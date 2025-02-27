from logging import getLogger, basicConfig, INFO, DEBUG
import datetime

import pandas as pd
import numpy as np
import json

from andersan import tile
from airpollutionwatch.convert import stations as fullstations
from delaunayextrapolation import DelaunayE
from airpollutionwatch import kanagawa, shizuoka, tokyo, chiba, yamanashi

# archive/airmonitor.pyはgrid12の値を返すか、あるいは局ごとのデータ(items/)からその場で三角メッシュを切り、
# 結果を

try:
    from .sqlitedictcache import sqlitedict_cache
    from .__init__ import Neighbors, prefecture_ranges
except:
    # for test()
    from andersan.sqlitedictcache import sqlitedict_cache
    from andersan import Neighbors, prefecture_ranges


# 県ごとの大気監視ウェブサイトからデータをもってくる関数の名前
prefecture_retrievers = dict(
    kanagawa=kanagawa, shizuoka=shizuoka, tokyo=tokyo, chiba=chiba, yamanashi=yamanashi
)


def station2lonlat(stations: list):
    lonlats = dict()
    for station in stations:
        if station in fullstations.index:
            lonlats[station] = (
                fullstations.loc[station, "経度"],
                fullstations.loc[station, "緯度"],
            )
    return lonlats


def wdws2wxwy(wdws):
    """風向きをベクトルになおす。通例にあわせ、ベクトルは風が来る方向を向いていることに注意。"""
    direc, speed = wdws[:, 0], wdws[:, 1]
    theta = direc * np.pi / 8  # verified with convert_wind.py
    notnan = np.logical_not(np.isnan(speed))
    x = np.full_like(direc, np.nan)  # fill with nan
    y = np.full_like(direc, np.nan)  # fill with nan
    x[notnan] = speed[notnan] * np.sin(theta[notnan])
    y[notnan] = speed[notnan] * np.cos(theta[notnan])
    return x, y


# @lru_cache(maxsize=9999)
# @shelf_cache("airmonitor")
@sqlitedict_cache("archive_airmonitor")  # vscodeで中身をチェックできる分、こちらのほうが便利
def tiles_(
    target_prefecture: str,
    isodate: str,
    zoom: int,
    items=["NMHC", "OX", "NOX", "TEMP", "WX", "WY"],  # order in datatype3
):
    """各県の特定時刻の大気監視データを入手し、地理院メッシュ点での測定値を内挿する。"""

    if target_prefecture not in Neighbors:
        return None  # 神奈川以外はまだ動かない

    # 地理院メッシュの間隔
    pref_range = np.array(prefecture_ranges[target_prefecture])  # lon,lat
    tiles, shape = tile.tiles(zoom, pref_range)

    unixtime = int(datetime.datetime.fromisoformat(isodate).timestamp())
    # 測定値をとってくる。
    BASE="/AIR/edamame2/items"
    full = pd.DataFrame()
    for item in items:
        with open(f"{BASE}/{item}/{unixtime}/stations.json") as j:
            d = json.load(j)
            # これは遅い処理。もっと簡潔にできる。
            for station, value in d.items():
                full.loc[int(station), item] = value
    
    # 全県の測定値を連結。欠測はNaNとする。
    full = full.replace({pd.NA: None})
    
    
    # 以下は../airmonitor.pyと同じはず。(amedasを使うところが省かれているけど)
    
    # 神奈川県の範囲
    # 範囲の指定方法を変更
    lonlats = tile.lonlat(zoom=zoom, xy=tiles)

    # 測定値の表。columnsは測定値名
    table = pd.DataFrame()
    table["lon"] = lonlats[:, 0]
    table["lat"] = lonlats[:, 1]
    table["X"] = tiles[:, 0]
    table["Y"] = tiles[:, 1]
    table["Z"] = zoom
    dt = datetime.datetime.fromisoformat(isodate)
    table["timestamp"] = dt
    table = table.set_index("timestamp")
    for item in items:

        # 欠測の測定局は除外する
        series = full[item].dropna()

        # itemとlonlatだけのdfを作る。
        # 各測定局の経度緯度
        item_df = pd.DataFrame.from_dict(station2lonlat(series.index), orient="index")
        item_df[item] = series
        item_df.columns = ["lon", "lat", item]

        # 副作用をさける
        del series

        # 測定局でDelaunay三角形を作り、gridsの格子点の内挿比を求める
        tri = DelaunayE(item_df[["lon", "lat"]])

        values = []

        for lonlat in lonlats:
            v, mix = tri.mixratio(lonlat)
            if np.all(mix > 0):
                values.append(mix @ item_df.iloc[v][item])
            else:
                # 外挿はしない
                values.append(np.nan)

        table[item] = np.array(values)

    # table.index = table.index.astype(int)

    return table


def tiles(
    target_prefecture: str,
    isodate: str,
    zoom: int,
    items=["NMHC", "OX", "NOX", "TEMP", "WX", "WY"],  # order in datatype3
):  # ここで、isodateに時刻が含まれる場合に日付と時だけに修正する。
    dt = datetime.datetime.fromisoformat(isodate)
    datestr = dt.strftime("%Y-%m-%dT%H:00:00+09:00")
    return tiles_(
        target_prefecture, datestr, zoom, items=items
    )


def test():
    basicConfig(level=DEBUG)
    logger = getLogger()
    logger.info(
        tiles("kanagawa", "2015-02-20T22:00+09:00", zoom=12)
    )


if __name__ == "__main__":
    test()
