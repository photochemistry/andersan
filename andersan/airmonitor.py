from logging import getLogger, basicConfig, INFO, DEBUG
import datetime
import os
import time
from typing import List

import pandas as pd
import numpy as np
import requests
import requests_cache

from andersan import tile
from airpollutionwatch.convert import stations as fullstations
from delaunayextrapolation import DelaunayE
from airpollutionwatch import kanagawa, shizuoka, tokyo, chiba, yamanashi
import andersan.archive.airmonitor as archive


try:
    # from .sqlitedictcache import sqlitedict_cache
    from .__init__ import Neighbors, prefecture_ranges
    from . import amedas
except:
    # for test()
    # from andersan.sqlitedictcache import sqlitedict_cache
    from __init__ import Neighbors, prefecture_ranges
    import amedas


# APW /v1/grid/field は URL+params がキャッシュキーになる。amedas と同様 requests_cache で永続化し、
# 同一条件の連打でネットワークを避ける（TTL 内は SQLite ヒット）。
# 24時間を遡る参照が頻繁なため、既定値は2日保持する。
_APW_FIELD_CACHE_SECONDS = int(os.getenv("APW_FIELD_CACHE_SECONDS", "172800"))
_APW_FIELD_SESSION = None


def _apw_field_session():
    global _APW_FIELD_SESSION
    if _APW_FIELD_SESSION is None:
        _APW_FIELD_SESSION = requests_cache.CachedSession(
            "apw_field",
            expire_after=_APW_FIELD_CACHE_SECONDS,
        )
    return _APW_FIELD_SESSION


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


# @sqlitedict_cache("airmonitor")  # vscodeで中身をチェックできる分、こちらのほうが便利
# def tiles_(
#     target_prefecture: str,
#     isodate: str,
#     zoom: int,
#     use_amedas=False,
#     items=["NMHC", "OX", "NOX", "TEMP", "WX", "WY"],  # order in datatype3
# ):
#     """各県の特定時刻の大気監視データを入手し、地理院メッシュ点での測定値を内挿する。"""

#     logger = getLogger(__name__)
    
#     if target_prefecture not in Neighbors:
#         return None  # 神奈川以外はまだ動かない

#     # 地理院メッシュの間隔
#     pref_range = np.array(prefecture_ranges[target_prefecture])  # lon,lat
#     tiles, shape = tile.tiles(zoom, pref_range)

#     # 測定値をとってくる。
#     # 2回目からのアクセスはairpollution.sqliteに保存された内容を利用する
#     dfs = []
#     for pref in Neighbors[target_prefecture]:
#         try:
#             dfs.append(prefecture_retrievers[pref].retrieve(isodate))
#         except:
#             logger.info(f"Failed to retrieve data from {pref} at {isodate}")
#     if len(dfs) == 0:
#         return None

#     if use_amedas:
#         # 気温はAMeDASから入手 (ゆくゆくは風速も)
#         amedas_df = amedas.retrieve(isodate)
#         amedas_df = amedas_df.replace({pd.NA: None})
#         amedas_df["WX"], amedas_df["WY"] = wdws2wxwy(
#             amedas_df[["WD", "WS"]].to_numpy().astype(float)
#         )

#     # 全県の測定値を連結。欠測はNaNとする。
#     full = pd.concat(dfs, join="outer")
#     full = full.replace({pd.NA: None})
#     full["WX"], full["WY"] = wdws2wxwy(full[["WD", "WS"]].to_numpy().astype(float))

#     # 神奈川県の範囲
#     # 範囲の指定方法を変更
#     lonlats = tile.lonlat(zoom=zoom, xy=tiles)

#     # 測定値の表。columnsは測定値名
#     table = pd.DataFrame()
#     table["lon"] = lonlats[:, 0]
#     table["lat"] = lonlats[:, 1]
#     table["X"] = tiles[:, 0]
#     table["Y"] = tiles[:, 1]
#     table["Z"] = zoom
#     dt = datetime.datetime.fromisoformat(isodate)
#     table["timestamp"] = dt
#     table = table.set_index("timestamp")
#     for item in items:

#         # 欠測の測定局は除外する
#         series = full[item].dropna()

#         # itemとlonlatだけのdfを作る。
#         # 各測定局の経度緯度
#         item_df = pd.DataFrame.from_dict(station2lonlat(series.index), orient="index")
#         item_df[item] = series
#         item_df.columns = ["lon", "lat", item]

#         # 副作用をさける
#         del series

#         # Mix amedas
#         if use_amedas:
#             if item in ("TEMP", "WX", "WY"):
#                 series2 = amedas_df[["lon", "lat", item]].dropna()
#                 item_df = pd.concat([item_df, series2])
#                 # print(item_df.tail())

#         # 測定局でDelaunay三角形を作り、gridsの格子点の内挿比を求める
#         tri = DelaunayE(item_df[["lon", "lat"]])

#         values = []

#         for lonlat in lonlats:
#             v, mix = tri.mixratio(lonlat)
#             if np.all(mix > 0):
#                 values.append(mix @ item_df.iloc[v][item])
#             else:
#                 # 外挿はしない
#                 values.append(np.nan)

#         table[item] = np.array(values)

#     # table.index = table.index.astype(int)

#     return table


def apw_tiles_(
    target_prefecture: str,
    datestr: str,
    zoom: int,
    use_amedas: bool = True,
    items: List[str] = ["NMHC", "OX", "NOX", "TEMP", "WX", "WY"],
    *,
    max_retries: int = 3,
) -> pd.DataFrame | None:
    """
    airpollutionwatch の /v1/grid/field を利用してタイル単位の値を取得する。
    現状は zoom=12, 神奈川県のみ対応。

    items は従来 tiles_ と同じ 6 項目を想定する:
    ["NMHC", "OX", "NOX", "TEMP", "WX", "WY"]
    """
    logger = getLogger(__name__)

    if target_prefecture != "kanagawa":
        raise ValueError(
            f"target_prefecture must be 'kanagawa', but got {target_prefecture}"
        )

    if zoom != 12:
        raise ValueError(f"apw_tiles_ currently supports only zoom=12, got {zoom}")

    if not items:
        raise ValueError("items must contain at least one item name")

    if max_retries < 1:
        raise ValueError(f"max_retries must be >= 1, got {max_retries}")

    # 県の bbox（経度緯度）は prefecture_ranges を利用
    pref_range = np.array(
        prefecture_ranges[target_prefecture]
    )  # [[lon1,lat1],[lon2,lat2]]
    min_lon = float(min(pref_range[0, 0], pref_range[1, 0]))
    max_lon = float(max(pref_range[0, 0], pref_range[1, 0]))
    min_lat = float(min(pref_range[0, 1], pref_range[1, 1]))
    max_lat = float(max(pref_range[0, 1], pref_range[1, 1]))
    bbox_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    base_url = "http://andersan.net:8089"

    # Andersan の items → airpollutionwatch の pollutant 名へのマッピング
    # WX/WY は WD/WS から導出するので、WD/WS をまとめて取得する。
    item_to_pollutant = {
        "NMHC": "nmhc",
        "OX": "ox",
        "NOX": "nox",
        "TEMP": "temp",
    }

    need_wind = any(i in ("WX", "WY") for i in items)
    apw_pollutants: set[str] = set()
    for item in items:
        # AMeDAS を使う場合、気温・風は AMeDAS で補間して上書きするので
        # APW から取得しない（無駄な API 呼び出しと上書きを避ける）
        if use_amedas and item in ("TEMP", "WX", "WY"):
            continue
        if item in item_to_pollutant:
            apw_pollutants.add(item_to_pollutant[item])
    if need_wind and not use_amedas:
        apw_pollutants.update({"wd", "ws"})

    if not apw_pollutants:
        return None

    # APW API は pollutant にカンマ区切り指定を受け付けるので、1回で取得する
    requested_pollutants = sorted(apw_pollutants)
    params = {
        "z": zoom,
        "pollutant": ",".join(requested_pollutants),
        "datetime": datestr,
        "bbox": bbox_str,
        "method": "idw",
        # "smoothing": 0.001,
    }
    request_url = f"{base_url}/v1/grid/field"
    retry_waits = [2, 4, 6]  # APWは短い間隔で再試行
    resp = None
    last_error = None
    for attempt in range(max_retries):
        try:
            logger.debug(
                f"Requesting URL: {request_url} params={params} "
                f"(attempt {attempt + 1}/{max_retries})"
            )
            resp = _apw_field_session().get(request_url, params=params, timeout=10)
            resp.raise_for_status()
            break
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = retry_waits[min(attempt, len(retry_waits) - 1)]
                logger.info(
                    "Failed to fetch APW grid field; retrying: "
                    f"pollutants={requested_pollutants}, wait={wait_time}s, error={e}"
                )
                time.sleep(wait_time)
            else:
                logger.info(
                    f"Failed to fetch grid field ({requested_pollutants}) from airpollutionwatch: {e}"
                )
                return None

    if resp is None:
        logger.info(
            f"Failed to fetch grid field ({requested_pollutants}) from airpollutionwatch: {last_error}"
        )
        return None

    data = resp.json()
    tx_min = data["tile_x_min"]
    ty_max = data["tile_y_max"]

    # APW 側が実際に用いたスナップショット時刻
    apw_snapshot_at = data.get("apw_snapshot_at")
    if apw_snapshot_at is not None:
        try:
            snapshot_dt = datetime.datetime.fromisoformat(apw_snapshot_at)
        except Exception:
            snapshot_dt = None
    else:
        snapshot_dt = None

    # 新API: fields{pollutant: 2d-array}, 旧API: values + item
    fields = data.get("fields")
    if not isinstance(fields, dict) or len(fields) == 0:
        item_key = data.get("item")
        values_2d = data.get("values")
        if item_key is None or values_2d is None:
            logger.info("APW response has no usable fields/values.")
            return None
        fields = {str(item_key): values_2d}

    first_key = next(iter(fields.keys()))
    first_values_2d = fields[first_key]
    if not first_values_2d:
        return None

    xs: list[int] = []
    ys: list[int] = []
    for row_idx, row in enumerate(first_values_2d):
        ty = ty_max - row_idx  # field の行は南→北
        for col_idx, _ in enumerate(row):
            tx = tx_min + col_idx
            xs.append(tx)
            ys.append(ty)

    tiles_xy = np.column_stack([xs, ys])
    lonlats = tile.lonlat(zoom=zoom, xy=tiles_xy)
    table = pd.DataFrame()
    table["lon"] = lonlats[:, 0]
    table["lat"] = lonlats[:, 1]
    table["X"] = tiles_xy[:, 0]
    table["Y"] = tiles_xy[:, 1]
    table["Z"] = zoom
    dt_index = snapshot_dt or datetime.datetime.fromisoformat(datestr)
    table["timestamp"] = dt_index
    table = table.set_index("timestamp")

    for pollutant in requested_pollutants:
        values_2d = fields.get(pollutant)
        if values_2d is None:
            logger.info(f"Missing pollutant in APW response fields: {pollutant}")
            table[pollutant.upper()] = np.nan
            continue

        vals: list[float | None] = []
        for row in values_2d:
            for v in row:
                vals.append(v)

        if len(vals) != len(table):
            logger.info(
                f"Field size mismatch for {pollutant}: "
                f"values={len(vals)} vs tiles={len(table)}"
            )
            table[pollutant.upper()] = np.nan
            continue

        table[pollutant.upper()] = np.array(vals, dtype=float)

    # 風向・風速 → WX/WY（AMeDAS を使う場合は APW から WD/WS を取らない想定）
    if (not use_amedas) and need_wind and "WD" in table.columns and "WS" in table.columns:
        wdws = table[["WD", "WS"]].to_numpy().astype(float)
        wx, wy = wdws2wxwy(wdws)
        table["WX"] = wx
        table["WY"] = wy
        # WD/WS は items に含まれていなければ削除
        if "WD" not in items:
            table.drop(columns=["WD"], inplace=True, errors="ignore")
        if "WS" not in items:
            table.drop(columns=["WS"], inplace=True, errors="ignore")

    # use_amedas が有効なら、TEMP/WX/WY について AMeDAS から再補間して上書きする
    if use_amedas:
        amedas_df = amedas.retrieve(datestr)
        amedas_df = amedas_df.replace({pd.NA: None})
        # WX/WY を事前に計算
        if "WD" in amedas_df.columns and "WS" in amedas_df.columns:
            amedas_df["WX"], amedas_df["WY"] = wdws2wxwy(
                amedas_df[["WD", "WS"]].to_numpy().astype(float)
            )

        lonlats_tiles = table[["lon", "lat"]].to_numpy()

        for item in ("TEMP", "WX", "WY"):
            if item not in items:
                continue
            if item not in amedas_df.columns:
                continue
            series2 = amedas_df[["lon", "lat", item]].dropna()
            if series2.empty:
                continue
            tri = DelaunayE(series2[["lon", "lat"]])
            values = []
            for lonlat in lonlats_tiles:
                v, mix = tri.mixratio(lonlat)
                if np.all(mix > 0):
                    values.append(mix @ series2.iloc[v][item])
                else:
                    values.append(np.nan)
            table[item] = np.array(values)

    # 呼び出し元が欲しい items 列のみ最低限存在するようにする（欠損時は NaN）
    for item in items:
        if item not in table.columns:
            table[item] = np.nan

    return table


def tiles(
    target_prefecture: str,
    isodate: str,
    zoom: int,
    use_amedas=True,
    items=["NMHC", "OX", "NOX", "TEMP", "WX", "WY"],  # order in datatype3
    *,
    max_retries: int = 3,
):  # ここで、isodateに時刻が含まれる場合に日付と時だけに修正する。
    dt = datetime.datetime.fromisoformat(isodate)
    datestr = dt.strftime("%Y-%m-%dT%H:00:00+09:00")

    if dt < datetime.datetime.fromisoformat("2021-04-04T00:00:00+09:00"):
        # use archived data of air monitor, which is provided by archive/airmonitor.py
        return archive.tiles_(
            target_prefecture, datestr, zoom, items=items
        )

    # それ以降は airpollutionwatch API ベースのタイルを利用
    return apw_tiles_(
        target_prefecture,
        datestr,
        zoom,
        use_amedas=use_amedas,
        items=items,
        max_retries=max_retries,
    )


def test():
    basicConfig(level=DEBUG)
    logger = getLogger()
    # logger.info(
    #     tiles("kanagawa", "2025-02-20T22:00+09:00", zoom=12, use_amedas=True)
    # )  # worked on 2025-02-20
    # logger.info(
    #     tiles("kanagawa", "2025-02-20T22:00+09:00", zoom=12, use_amedas=False)
    # )  # worked on 2025-02-20
    # logger.info(
    #     tiles("kanagawa", "2015-02-20T22:00+09:00", zoom=12)
    # )
    # logger.info(
    #     tiles("kanagawa", "2026-03-15T09:00+09:00", zoom=12, use_amedas=False)
    # )
    logger.info(
        tiles("kanagawa", "2026-03-18T09:00+09:00", zoom=12, use_amedas=False)
    )
    logger.info(
        tiles("kanagawa", "2026-03-18T09:00+09:00", zoom=12, use_amedas=True)
    )


if __name__ == "__main__":
    test()
