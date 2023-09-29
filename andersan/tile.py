"""
地理院タイルの操作。
"""
import numpy as np


def get_tile_num(lat, lon, zoom):
    """
    緯度経度からタイル座標を取得する
    Parameters
    ----------
    lat : number
        タイル座標を取得したい地点の緯度(deg)
    lon : number
        タイル座標を取得したい地点の経度(deg)
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
    lat_rad = np.radians(lat)
    n = 2.0**zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int(
        (1.0 - np.log(np.tan(lat_rad) + (1 / np.cos(lat_rad))) / np.pi) / 2.0 * n
    )
    return (xtile, ytile)


def num2deg(xtile, ytile, zoom):
    # https://sorabatake.jp/7325/
    # https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Python
    n = 2.0**zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = np.arctan(np.sinh(np.pi * (1 - 2 * ytile / n)))
    lat_deg = np.degrees(lat_rad)
    return (lon_deg, lat_deg)


def get_tile_bbox(z, x, y):
    """
    タイル座標からバウンディングボックスを取得する
    https://tools.ietf.org/html/rfc7946#section-5
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
    bbox: tuple of number
        タイルのバウンディングボックス
        (左下経度, 左下緯度, 右上経度, 右上緯度)
    """
    # https://sorabatake.jp/7325/

    right_top = num2deg(x + 1, y, z)
    left_bottom = num2deg(x, y + 1, z)
    return (left_bottom[0], left_bottom[1], right_top[0], right_top[1])


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


def test():
    # 平塚市の中心部のタイルは 13/7266/3235
    lon, lat = num2deg(7266, 3235, zoom=13)
    print(lon, lat)
    x, y = get_tile_num(lat + 0.00001, lon - 0.00001, zoom=13)
    print(x, y)


# test
if __name__ == "__main__":
    test()
