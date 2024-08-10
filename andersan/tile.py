"""
地理院タイルの操作。
"""

import numpy as np
from logging import getLogger, DEBUG, basicConfig


# backward compat


def tile_num(lat, lon, zoom):
    logger = getLogger()
    logger.warning("tile_num(lat, lon, zoom) is deprecated. Use code(zoom, lon, lat).")
    return code(zoom, lon, lat)


def num2deg(xtile, ytile, zoom):
    logger = getLogger()
    logger.warning("num2deg(xtile, ytile, zoom) is deprecated. Use lonlat(zoom, ...).")
    return lonlat(zoom, x=xtile, y=ytile)


def get_tile_bbox(z, x, y):
    logger = getLogger()
    logger.warning("get_tile_bbox(z, x, y) is deprecated. Use bounding_box(zoom, ...).")
    return bounding_box(zoom=z, x=x, y=y)


# deprecated


def get_tile_approximate_lonlats(z, x, y):
    """
    タイルの各ピクセルの左上隅の経度緯度を取得する（簡易版）
    Parameters
    ----------
    z : int
        タイルのズーム率
    x : int
        タイルのX座標
    y : int
        タイルのY座標
    Returns
    -------
    lonlats: ndarray
        タイルの各ピクセルの経度緯度
        256*256*2のnumpy配列
        経度、緯度の順
    """
    logger = getLogger()
    logger.warning("get_tile_approximate_lonlats(z, x, y) is deprecated.")

    # https://sorabatake.jp/7325/
    bbox = get_tile_bbox(z, x, y)
    width = abs(bbox[2] - bbox[0])
    height = abs(bbox[3] - bbox[1])
    width_per_px = width / 256
    height_per_px = height / 256

    lonlats = np.zeros((256, 256, 2))
    lat = bbox[3]
    for i in range(256):
        lon = bbox[0]
        for j in range(256):
            lonlats[i, j, :] = [lon, lat]
            lon += width_per_px
        lat -= height_per_px
    return lonlats


# functions


def code(zoom, lon=None, lat=None, lonlats=None):
    """
    緯度経度からタイル座標を取得する
    Parameters
    ----------
    lat : number
        タイル座標を取得したい地点の緯度(deg)
    lon : number
        タイル座標を取得したい地点の経度(deg)
    lonlats:
        array of shape[:,2]
    zoom : int
        タイルのズーム率
    Returns
    -------
    xtile : int
        タイルのX座標
    ytile : int
        タイルのY座標
    """
    # https://sorabatake.jp/7325/
    # https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Python

    assert not (lon is None and lonlats is None)

    if lon is None:
        lon, lat = lonlats[:, 0], lonlats[:, 1]

    lat_rad = np.radians(lat)
    n = 2.0**zoom
    xtile = np.floor((lon + 180.0) / 360.0 * n).astype(int)
    ytile = np.floor(
        (1.0 - np.log(np.tan(lat_rad) + (1 / np.cos(lat_rad))) / np.pi) / 2.0 * n
    ).astype(int)

    if lonlats is not None:
        return np.array([xtile, ytile], dtype=int).T

    return xtile, ytile


def lonlat(zoom: int, x: int = None, y: int = None, xy=None):
    """タイル番号から、タイルの左上角の緯度経度を計算する。

    Args:
        zoom (int): タイルのズーム率
        x (int, optional): タイルのx. Defaults to None. (Deprecated)
        y (int, optional): タイルのy. Defaults to None. (Deprecated)
        xy (_type_, optional): 2次元numpy array. Defaults to None.

    Returns:
        x, yが指定された場合は緯度と経度。
        xyが指定された場合は緯度経度の配列
    """
    # https://sorabatake.jp/7325/
    # https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Python

    # タイル番号から、タイルの左上角の緯度経度を計算します。
    # 従来は1つの(x,y)対に対して1つの緯度しか返せませんでしたが、[:,2]形状のarrayを渡すと結果も経度緯度のarrayで返すようになりました。
    # 1対だけ与えるときはパラメータx,yを指定し、arrayを渡す時はパラメータxyを指定します。同時に指定することはできません。

    # 同時に指定したらエラーを吐いて止まる。
    assert not (x is None and xy is None)

    # zoom値を、実際の比率に変換します。換算に必要です。
    n = 2.0**zoom

    # 対が与えられた場合とarrayが与えられた場合で、あとの換算処理が同じになるように、データ形式を加工します。

    logger = getLogger()
    if x is None:
        if type(xy) == np.ndarray:
            # arrayが与えられた場合
            # 1次元整数なら、x,yを分離する
            if len(xy.shape) == 1:
                logger.warning("andersan.tile.lonlat(): 8-digit code is deprecated.")
                xy = np.array([(x // 10000, x % 10000) for x in xy])
            # 2次元ならx,yとする
            x, y = np.floor(xy[:, 0]), np.floor(xy[:, 1])
        else:
            xy = np.array(xy)
            x, y = np.floor(xy[:, 0]), np.floor(xy[:, 1])
    else:
        logger.warning("andersan.tile.lonlat(): 'x=' is deprecated. Use 'xy='.")
        # 対が与えられた場合、小数点以下を切りすてます。
        x, y = np.floor(x), np.floor(y)

    # 換算計算。これは上のリンクのどちらかからもってきただけ。
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = np.arctan(np.sinh(np.pi * (1 - 2 * y / n)))
    lat_deg = np.degrees(lat_rad)

    # 返り値の返し方も、対とアレイで別になります。
    if xy is not None:
        # アレイの場合には、緯度と経度を束ね、[:,2]形状になるように転置します。
        return np.array([lon_deg, lat_deg]).T

    # 対の場合には、対を返します。
    return lon_deg, lat_deg


def bounding_box(zoom, x, y):
    """
    タイル座標からバウンディングボックスを取得する
    https://tools.ietf.org/html/rfc7946#section-5
    Parameters
    ----------
    z : int
        タイルのズーム率
    x : int, optional
        タイルのX座標
    y : int, optional
        タイルのY座標

    Returns
    -------
    bbox: tuple of number
        タイルのバウンディングボックス
        (左下経度, 左下緯度, 右上経度, 右上緯度)
    """
    # https://sorabatake.jp/7325/

    x = np.floor(x)
    y = np.floor(y)
    corners = np.array([[x, y + 1], [x + 1, y]])
    lonlats = lonlat(zoom, xy=corners)
    return (lonlats[0, 0], lonlats[0, 1], lonlats[1, 0], lonlats[1, 1])


def make_grid(zoom, lonlat_range):
    codes = code(zoom, lonlats=lonlat_range)
    topleft, bottomright = codes.astype(int)
    Xt, Yt = np.mgrid[topleft[0] : bottomright[0], topleft[1] : bottomright[1]]
    XY = np.vstack([Xt.ravel(), Yt.ravel()])
    lon, lat = lonlat(zoom=zoom, xy=XY)
    return np.array([lon, lat]).T


def test():
    #
    # 関数を改造してもちゃんと動くかどうかをいつも確認する。
    #

    basicConfig(level=DEBUG)
    # 平塚市の中心部のタイルは 13/7266/3235
    lon, lat = lonlat(zoom=13, x=7266, y=3235)
    print(lon, lat)
    x, y = code(zoom=13, lon=lon + 0.00001, lat=lat - 0.00001)
    print(x, y)
    print(bounding_box(zoom=13, x=7266, y=3235))
    # こんな書きかたができればいいなあ。
    # lon, lat = lonlat(zoom=13, xy=(7266, 3235))
    print(lon, lat)

    xy = np.array([[7266, 3235], [7267, 3235], [7267, 3236], [7266, 3236]])
    lonlats = lonlat(zoom=13, xy=xy)
    print(lonlats)
    xy = code(zoom=13, lonlats=lonlats)
    print(xy)


# test
if __name__ == "__main__":
    test()
