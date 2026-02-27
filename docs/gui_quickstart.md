# GUI クイックスタート

## 概要

`gui/main.py` は PySide6 製の最小 GUI で、以下の操作を画面から実行できます。

| ボタン | 実行内容 |
|--------|----------|
| **Update (RACE)** | JV-Link から過去データを取得 (32-bit Python) → 派生テーブルを更新 (64-bit) |
| **Suggest** | 複勝買い目提案を一括生成 (64-bit) |

---

## 前提条件

| 要件 | 備考 |
|------|------|
| Windows | JV-Link COM コンポーネントが必要 |
| Python 32-bit | `jv_ingest_raw.py` は 32-bit で実行する必要あり |
| Python 64-bit venv | `pandas` / `catboost` 等のライブラリが入っている venv |
| JV-Link | `JVDTLab.JVLink` COM コンポーネントが登録済みであること |

---

## セットアップ

### 1. 64-bit venv に PySide6 をインストール

```powershell
# 64-bit venv をアクティブにしてから
pip install PySide6
```

依存関係は `requirements.txt` からまとめてインストールできます:

```powershell
pip install -r requirements.txt
```

### 2. 32-bit Python に pywin32 をインストール

```powershell
# 32-bit の python.exe を使って
C:\Python32\python.exe -m pip install pywin32
```

---

## 起動方法

```powershell
# リポジトリのルートディレクトリで
# 64-bit venv をアクティブにしてから
python gui/main.py
```

---

## 各入力項目

| 項目 | デフォルト | 説明 |
|------|-----------|------|
| DB パス | `jv_data.db` | SQLite DB ファイルのパス |
| 取込開始日 | (空) | `YYYYMMDD` 形式 (例: `20240101`)。Update ボタン用 |
| 出力ディレクトリ | (空・必須) | Suggest の出力先ディレクトリ |
| モデルパス | `models/place_model.cbm` | 学習済み CatBoost モデル |
| 32-bit Python 実行ファイル | (空・必須) | 32-bit `python.exe` のフルパス |
| レースキー | (空) | スペース区切りのレースキー。Suggest ボタン用 |

`…` ボタンを押すとファイル/フォルダ選択ダイアログが開きます。

---

## Update (RACE) の動作

1. **Step 1/2** — `scripts/jv_ingest_raw.py` を **32-bit Python** で実行:
   ```
   <python32> scripts/jv_ingest_raw.py --from-date <日付> --dataspec RACE --db <DB>
   ```
2. **Step 2/2** — `scripts/update_db_from_raw.py` を **64-bit Python (現在の venv)** で実行:
   ```
   python scripts/update_db_from_raw.py --db <DB> --skip-masters
   ```

Step 1 が失敗した場合、Step 2 は実行されません。

---

## Suggest の動作

`scripts/batch_suggest_place_bets.py` を **64-bit Python** で実行:

```
python scripts/batch_suggest_place_bets.py \
    --db <DB> --model <モデル> --out-dir <出力ディレクトリ> \
    --race-keys <レースキー1> <レースキー2> ...
```

出力 CSV は `<出力ディレクトリ>/summary.csv` に生成されます。

---

## ログ

ウィンドウ下部のログエリアに各コマンドの標準出力がリアルタイムで表示されます。  
「ログをクリア」ボタンでログを消去できます。
