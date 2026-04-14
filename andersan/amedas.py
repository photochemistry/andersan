import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime
import json
import sqlite3
import pickle
import time
import random

# import requests
import requests_cache
import requests
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


def validate_amedas_response(response_text, is_retry=False):
    """Amedasレスポンスのvalidationを行う"""
    try:
        data = json.loads(response_text)
        if not isinstance(data, dict):
            return False, "データが辞書形式ではありません"
        
        # リトライ時はより緩い条件でvalidation
        min_stations = 800 if is_retry else 1200
        min_temp_stations = 50 if is_retry else 100
        
        # JSONのkeyの個数が基準未満なら失敗
        if len(data) < min_stations:
            return False, f"観測所数が不足しています: {len(data)} (期待値: {min_stations}以上)"
        
        # 温度データの存在確認
        temp_count = 0
        for station_data in data.values():
            if isinstance(station_data, dict) and 'temp' in station_data:
                temp_count += 1
        
        if temp_count < min_temp_stations:  # 温度データを持つ観測所が基準未満なら失敗
            return False, f"温度データを持つ観測所が不足しています: {temp_count} (期待値: {min_temp_stations}以上)"
        
        return True, f"正常: 観測所数={len(data)}, 温度データあり観測所数={temp_count}"
    
    except json.JSONDecodeError as e:
        return False, f"JSON解析エラー: {e}"
    except Exception as e:
        return False, f"予期しないエラー: {e}"


def delete_invalid_cache(url):
    """無効なキャッシュを削除する"""
    logger = getLogger(__name__)
    try:
        conn = sqlite3.connect("airpollution.sqlite")
        cursor = conn.cursor()
        
        # URLに対応するキャッシュを検索
        cursor.execute("SELECT key, value FROM responses")
        for key, value in cursor.fetchall():
            try:
                response_data = pickle.loads(value) if isinstance(value, bytes) else value
                if isinstance(response_data, dict) and response_data.get('url') == url:
                    cursor.execute("DELETE FROM responses WHERE key = ?", (key,))
                    logger.info(f"無効なキャッシュを削除しました: {url}")
                    break
            except Exception:
                continue
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"キャッシュ削除エラー: {e}")


def retrieve_raw_single(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
    logger = getLogger(__name__)
    dt = datetime.datetime.fromisoformat(isotime)
    date_time = dt.strftime("%Y%m%d%H0000")
    url = f"https://www.jma.go.jp/bosai/amedas/data/map/{date_time}.json"

    session = requests_cache.CachedSession("airpollution")
    try:
        response = session.get(url)
    except requests.RequestException as e:
        raise ValueError(f"ネットワークエラー: {e}")

    if response.status_code == 404:
        raise ValueError(f"データが利用できません: {url} (404エラー)")

    is_valid, message = validate_amedas_response(response.text, is_retry=False)
    if not is_valid:
        logger.info(f"Validation失敗: {message}")
        logger.info(f"URL: {url}")
        logger.info(f"レスポンスサイズ: {len(response.text)} bytes")
        logger.info(f"HTTPステータス: {response.status_code}")
        # 無効データは残さない
        delete_invalid_cache(url)
        raise ValueError(f"validation失敗: {message}")
    
    # これがないと文字化けする
    # response.encoding = response.apparent_encoding

    dfs = pd.read_json(io.StringIO(response.text), orient="index")
    return dfs


def retrieve_raw(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
    return retrieve_raw_single(isotime)


def retrieve(isotime):
    """指定された日時のデータを入手する。index名とcolumn名をつけなおし、単位をそらまめにあわせる。"""
    logger = getLogger()

    df = retrieve_raw(isotime)
    # print(df.iloc[0])
    session = requests_cache.CachedSession("airpollution")
    # session = requests.Session()
    response = session.get(
        f"https://www.jma.go.jp/bosai/amedas/const/amedastable.json",
    )
    with open("amedastable.json", "w") as f:
        f.write(response.text)
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
