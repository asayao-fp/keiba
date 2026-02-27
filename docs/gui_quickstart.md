# GUI クイックスタート

## 概要

`gui/main.py` は PySide6 製の GUI で、以下の操作を画面から実行できます。

| ボタン | 実行内容 |
|--------|----------|
| **Update (RACE)** | JV-Link から過去データを取得 (32-bit Python) → 派生テーブルを更新 (64-bit) |
| **Suggest** | 複勝買い目提案を一括生成 (64-bit) |
| **Update + Suggest** | Update → Suggest を連続して実行 |
| **キャンセル** | 実行中のプロセスを停止してキューをクリア |

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

## 自動設定とデフォルト値

初回起動時 (設定ファイルが存在しない場合) に以下を自動で検出・設定します:

| 項目 | 自動設定内容 |
|------|-------------|
| DB パス | `<リポジトリルート>/jv_data.db` |
| モデルパス | `<リポジトリルート>/models/place_model.cbm` |
| 出力ディレクトリ | `<リポジトリルート>/out` (存在しない場合は自動作成) |
| 32-bit Python | `py` ランチャー または 一般的なインストールパスを自動検出 |

32-bit Python が自動検出できない場合は警告が表示されます。「検出」ボタンで再検出を試みるか、`…` ボタンで手動選択してください。

---

## 設定の永続化

設定はリポジトリルートの `.keiba_gui_config.json` に自動保存され、次回起動時に復元されます。  
このファイルはローカルの設定のみを含むため、`.gitignore` に登録されておりバージョン管理の対象外です。

保存される設定:
- DB パス / モデルパス / 出力ディレクトリ
- 32-bit Python コマンド
- 最後に使用した取込開始日
- レースキー
- ウィンドウの位置とサイズ

---

## 各入力項目

| 項目 | デフォルト | 説明 |
|------|-----------|------|
| DB パス | `<repo>/jv_data.db` | SQLite DB ファイルのパス |
| 取込開始日 | 今日の日付 | カレンダーから選択。`YYYYMMDD` 形式で渡される。Update ボタン用 |
| 出力ディレクトリ | `<repo>/out` | Suggest の出力先ディレクトリ |
| モデルパス | `<repo>/models/place_model.cbm` | 学習済み CatBoost モデル |
| 32-bit Python | (自動検出) | 32-bit Python の実行コマンド。`py -3.11-32` または `python.exe` フルパス |
| レースキー | (空) | スペース区切りのレースキー。Suggest ボタン用 |

`…` ボタンを押すとファイル/フォルダ選択ダイアログが開きます。  
取込開始日はカレンダーアイコンをクリックしてカレンダーから選択できます。

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
