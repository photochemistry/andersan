import numpy as np
from scipy.spatial import Delaunay


def interpolate_(point, vertices):
    # last point is the origin
    o = vertices[2]
    ab = vertices[:2] - o
    c = point - o
    p, q = c @ np.linalg.inv(ab)
    r = 1 - p - q
    return p, q, r


def interpolate(stations: dict, grids: np.ndarray, extrapolate: bool = False):
    """_summary_

    Args:
        stations (dict): キーが局番号、値がlonlat
        grids (np.ndarray): 内挿したい格子点のlonlat

    Yields:
        label1, composition1, label2, composition2, label3, composition3:
            grid点を含むDelaunay3角形の3頂点と混合比。

    """

    # 内挿する
    # 順番が変わると困るので、測定局のリストをさきに作る。
    st = list(stations)
    # 内挿のためのデータ列を整形
    locations = np.array([stations[x] for x in st])

    # 三角形分割
    tri = Delaunay(locations)

    triangles = tri.find_simplex(grids)

    for i, (gridpoint, triangle) in enumerate(zip(grids, triangles)):
        # a, b, cは順序
        a, b, c = tri.simplices[triangle]
        # A, B, Cは測定局ラベル
        A, B, C = st[a], st[b], st[c]
        p, q, r = interpolate_(gridpoint, locations[tri.simplices[triangle]])
        if 0 <= p <= 1 and 0 <= q <= 1 and 0 <= r <= 1:
            yield A, p, B, q, C, r
        elif extrapolate:
            pqr = np.array([p, q, r])
            pqr[pqr < 0] = 0
            pqr /= np.sum(pqr)
            p, q, r = pqr
            yield A, p, B, q, C, r
        else:
            yield A, None, B, None, C, None
