from functools import lru_cache
from logging import getLogger, basicConfig, DEBUG
import datetime
import os
from typing import Iterable

import numpy as np
import pandas as pd

from andersan import tile
from andersan.tile_utils import filter_table_by_tiles, normalize_tiles

try:
    from .sqlitedictcache import sqlitedict_cache
    from .__init__ import Neighbors, prefecture_ranges
except:
    # for test()
    from andersan.sqlitedictcache import sqlitedict_cache
    from andersan import Neighbors, prefecture_ranges


DEFAULT_ITEMS = ["NMHC", "OX", "NOX", "TEMP", "WX", "WY"]
DEFAULT_IDW_BASEPATH = os.getenv("ARCHIVE_AIRMONITOR_IDW_BASEPATH", "/AIR/andersan-train/idw12")


def _csv_path(basepath: str, item: str, x: int, y: int) -> str:
    return f"{basepath}/{item}/{x}/{y}.csv"


@lru_cache(maxsize=1024)
def _read_tile_csv(basepath: str, item: str, x: int, y: int) -> pd.DataFrame:
    path = _csv_path(basepath, item, x, y)
    if not os.path.exists(path):
        return pd.DataFrame(columns=["unixtime", f"{x}/{y}"])
    df = pd.read_csv(path)
    if "unixtime" not in df.columns:
        return pd.DataFrame(columns=["unixtime", f"{x}/{y}"])
    value_col = f"{x}/{y}"
    if value_col not in df.columns:
        return pd.DataFrame(columns=["unixtime", value_col])
    return df[["unixtime", value_col]]


@lru_cache(maxsize=2048)
def _series_by_unixtime(basepath: str, item: str, x: int, y: int) -> pd.Series:
    df = _read_tile_csv(basepath, item, x, y)
    if len(df) == 0:
        return pd.Series(dtype=float)
    value_col = f"{x}/{y}"
    return df.set_index("unixtime")[value_col]


def _lookup_value(basepath: str, item: str, x: int, y: int, unixtime: int) -> float:
    series = _series_by_unixtime(basepath, item, x, y)
    if len(series) == 0:
        return np.nan
    value = series.get(unixtime, np.nan)
    if pd.isna(value):
        return np.nan
    return float(value)


# @lru_cache(maxsize=9999)
# @shelf_cache("airmonitor")
@sqlitedict_cache("archive_airmonitor_idw12_v1")  # 実装刷新後の結果と旧キャッシュを分離
def tiles_(
    target_prefecture: str,
    isodate: str,
    zoom: int,
    items=DEFAULT_ITEMS,
):
    """idw12 のタイル化済み観測データから、指定時刻の値を返す。"""
    logger = getLogger(__name__)

    if target_prefecture not in Neighbors:
        return None

    if zoom != 12:
        raise ValueError(f"archive.airmonitor currently supports only zoom=12, got {zoom}")

    basepath = DEFAULT_IDW_BASEPATH
    if not os.path.exists(basepath):
        logger.warning(f"idw basepath does not exist: {basepath}")
        return None

    pref_range = np.array(prefecture_ranges[target_prefecture])
    tiles_xy, _ = tile.tiles(zoom, pref_range)
    lonlats = tile.lonlat(zoom=zoom, xy=tiles_xy)

    unixtime = int(datetime.datetime.fromisoformat(isodate).timestamp())

    table = pd.DataFrame(
        {
            "lon": lonlats[:, 0],
            "lat": lonlats[:, 1],
            "X": tiles_xy[:, 0],
            "Y": tiles_xy[:, 1],
        }
    )
    table["Z"] = zoom
    table["timestamp"] = datetime.datetime.fromisoformat(isodate)
    table = table.set_index("timestamp")

    for item in items:
        values = np.full(len(tiles_xy), np.nan, dtype=float)
        for i, (x, y) in enumerate(tiles_xy):
            values[i] = _lookup_value(basepath, item, int(x), int(y), unixtime)
        table[item] = values

    return table


def tiles(
    target_prefecture: str,
    isodate: str,
    zoom: int,
    items=DEFAULT_ITEMS,
):  # ここで、isodateに時刻が含まれる場合に日付と時だけに修正する。
    dt = datetime.datetime.fromisoformat(isodate)
    datestr = dt.strftime("%Y-%m-%dT%H:00:00+09:00")
    return tiles_(
        target_prefecture, datestr, zoom, items=items
    )


def tiles_by_tiles(
    target_tiles: Iterable[Iterable[int]],
    isodate: str,
    zoom: int,
    items=DEFAULT_ITEMS,
    *,
    target_prefecture: str = "kanagawa",
):
    """タイル集合指定で archived airmonitor を返す（内部は県単位取得+タイル絞り込み）。"""
    table = tiles(target_prefecture, isodate, zoom, items=items)
    tiles_xy = normalize_tiles(target_tiles)
    return filter_table_by_tiles(table, tiles_xy)


def test():
    basicConfig(level=DEBUG)
    logger = getLogger()
    logger.info(
        tiles("kanagawa", "2015-02-20T22:00+09:00", zoom=12)
    )


if __name__ == "__main__":
    test()
