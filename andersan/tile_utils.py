from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from andersan import tile


def normalize_tiles(
    tiles: Iterable[Iterable[int]], *, keep_order: bool = False
) -> np.ndarray:
    """タイル集合を (N,2) の int ndarray に正規化する。"""
    arr = np.asarray(list(tiles), dtype=int)
    if arr.ndim != 2 or arr.shape[1] != 2:
        raise ValueError("tiles must be a 2D iterable of [X, Y]")

    if not keep_order:
        return np.unique(arr, axis=0)

    seen = set()
    uniq = []
    for x, y in arr.tolist():
        key = (int(x), int(y))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(key)
    return np.asarray(uniq, dtype=int)


def tiles_to_key(tiles_xy: np.ndarray) -> tuple[tuple[int, int], ...]:
    return tuple((int(x), int(y)) for x, y in tiles_xy.tolist())


def bbox_str_from_tiles(tiles_xy: np.ndarray, zoom: int) -> str:
    lonlats = tile.lonlat(zoom=zoom, xy=tiles_xy)
    min_lon = float(np.min(lonlats[:, 0]))
    max_lon = float(np.max(lonlats[:, 0]))
    min_lat = float(np.min(lonlats[:, 1]))
    max_lat = float(np.max(lonlats[:, 1]))
    return f"{min_lon},{min_lat},{max_lon},{max_lat}"


def filter_table_by_tiles(table: pd.DataFrame, tiles_xy: np.ndarray) -> pd.DataFrame:
    keep = {(int(x), int(y)) for x, y in tiles_xy.tolist()}
    return table[
        table.apply(lambda r: (int(r["X"]), int(r["Y"])) in keep, axis=1)
    ]
