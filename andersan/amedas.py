import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime

# import requests
import requests_cache
import pandas as pd
import numpy as np
from logging import basicConfig, getLogger, INFO, DEBUG
from airpollutionwatch.convert import TEMP, HUM, CODE, LON, LAT, WD, WS

# apparent nameと内部標準名(そらまめ名)の変換
converters = {
    # # "地域",
    # "測定局": lambda x: STATION(x, aliases=aliases),
    # # "測定局",
    # # "種別",
    # "SO2 ppm": lambda x: SO2(x, unit="ppm"),
    # "NO ppm": lambda x: NO(x, unit="ppm"),
    # "NO2 ppm": lambda x: NO2(x, unit="ppm"),
    # "NOX ppm": lambda x: NOX(x, unit="ppm"),
    # "OX ppm": lambda x: OX(x, unit="ppm"),
    # "SPM mg/m3": lambda x: SPM(x, unit="mg/m3"),
    # "PM2.5 ug/m3": lambda x: PM25(x, unit="ug/m3"),
    # "NMHC ppmC": lambda x: NMHC(x, unit="ppmC"),
    # "CH4 ppmC": lambda x: CH4(x, unit="ppmC"),
    # "THC ppmC": lambda x: THC(x, unit="ppmC"),
    # "CO ppm": lambda x: CO(x, unit="ppm"),
    "windDirection": lambda x: WD(x, unit="16dirc"),
    "wind": lambda x: WS(x, unit="m/s"),
    "temp": lambda x: TEMP(x, unit="celsius"),
    "humidity": lambda x: HUM(x, unit="%"),
    "lon": lambda x: LON(x, unit="degree"),
    "lat": lambda x: LAT(x, unit="degree"),
    "code": lambda x: CODE(x),
}


def retrieve_raw(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
    dt = datetime.datetime.fromisoformat(isotime)
    date_time = dt.strftime("%Y%m%d%H0000")

    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        f"https://www.jma.go.jp/bosai/amedas/data/map/{date_time}.json",
    )
    # これがないと文字化けする
    # response.encoding = response.apparent_encoding

    dfs = pd.read_json(io.StringIO(response.text), orient="index")
    return dfs


def retrieve(isotime):
    """指定された日時のデータを入手する。index名とcolumn名をつけなおし、単位をそらまめにあわせる。"""
    logger = getLogger()

    df = retrieve_raw(isotime)
    # print(df.iloc[0])
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        f"https://www.jma.go.jp/bosai/amedas/const/amedastable.json",
    )
    amedas = pd.read_json(io.StringIO(response.text), orient="index")

    df = pd.merge(df, amedas, left_index=True, right_index=True, how="left").dropna()
    # 度分を度に変換
    df["lon"] = [x[0] + x[1] / 60 for x in df["lon"]]
    df["lat"] = [x[0] + x[1] / 60 for x in df["lat"]]
    # 第2項目の意味がわからない。
    # print(df.iloc[0]["temp"][0])
    for item in ("temp", "humidity", "wind", "windDirection"):
        temp = []
        for t in df[item]:
            if type(t) is list:
                temp.append(t[0])
                # 2番目の項目の意味不明。単位指定か?
            else:
                temp.append(t)
        df[item] = temp
    df["code"] = df.index

    # print(df.columns)
    # print(df["windDirection"])
    cols = []
    for col in df.columns:
        if col in converters:
            cols.append(converters[col](df[col]))
    return pd.concat(cols, axis=1).set_index("code")
    # return df


def test():
    basicConfig(level=DEBUG)
    logger = getLogger()
    logger.info(retrieve("2025-02-20T22:00+09:00"))  # worked on 2025-02-20
    # 温度が10倍になっている? 注意


if __name__ == "__main__":
    test()
