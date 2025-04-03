# Andersan プロジェクト

## 概要

Andersan プロジェクトは、大気汚染に関するデータを収集、処理、可視化するためのツールとライブラリを提供します。このプロジェクトは、気象データ、大気監視データ、地理情報などを統合し、ユーザーが環境問題についてより深く理解し、分析できるようにすることを目的としています。

## 主な機能

-   **データ収集**:
    -   Open-Meteo API からの気象データの取得
    -   OpenWeatherMap API からの気象データの取得
    -   AMeDAS (アメダス) からの気象データの取得
    -   各都道府県の大気監視ウェブサイトからの大気汚染データの取得
-   **データ処理**:
    -   地理院タイルを用いたデータのグリッド化
    -   Delaunay 三角形分割を用いたデータの補間
    -   風向きと風速から風のベクトルへの変換
-   **データキャッシュ**:
    -   `sqlitedict` を用いたデータキャッシュによる高速化
-   **データ変換**:
    -   測定値の単位変換
    -   風向きをベクトルに変換
-   **アーカイブデータの利用**:
    -   過去の気象データや大気監視データの利用
- **近隣県データの利用**:
    - 複数の県にまたがるデータを統合して利用

## 依存関係

-   `pandas`
-   `numpy`
-   `datetime`
-   `requests_cache`
-   `retry_requests`
-   `pytz`
-   `sqlitedict`
-   `scipy`
-   `toml`
-   `delaunayextrapolation`
-   `airpollutionwatch`

## インストール方法

1.  必要なライブラリをインストールします。

    ```bash
    pip install pandas numpy requests_cache retry_requests pytz sqlitedict scipy toml
    ```
    ```bash
    pip install git+https://github.com/t-sagara/delaunayextrapolation.git
    ```
    ```bash
    pip install git+https://github.com/t-sagara/airpollutionwatch.git
    ```

2.  このリポジトリをクローンします。

    ```bash
    git clone <リポジトリのURL>
    ```

## 使用方法

-   各モジュール（`openmeteo.py`, `openweathermap.py`, `amedas.py`, `airmonitor.py` など）には、データの取得や処理を行うための関数が含まれています。
-   `test()` 関数を実行することで、各モジュールの動作を確認できます。

## ファイル構成

-   `openmeteo.py`: Open-Meteo API からの気象データ取得
-   `openweathermap.py`: OpenWeatherMap API からの気象データ取得
-   `amedas.py`: AMeDAS からの気象データ取得
-   `airmonitor.py`: 各都道府県の大気監視ウェブサイトからの大気汚染データ取得
-   `tile.py`: 地理院タイルの操作
-   `__init__.py`: 近隣県の情報や補間関数
-   `sqlitedictcache.py`: `sqlitedict` を用いたキャッシュ機能
-   `archive/`: 過去のデータのアーカイブ
-   `api_keys.toml`: APIキーを保管するファイル

## テスト

各モジュールには `test()` 関数が含まれており、これを使って動作確認ができます。

```bash
python openmeteo.py
python openweathermap.py
python amedas.py
python airmonitor.py
python tile.py
```

## 今後の展望
- データの可視化機能の追加
- より多くのデータソースのサポート
- ユーザーインターフェースの改善
## 貢献
バグ報告、機能要望、プルリクエストなど、貢献は大歓迎です。

## ライセンス
このプロジェクトは MIT ライセンスの下で公開されています。

## 連絡先
質問や提案があれば、お気軽にご連絡ください。

