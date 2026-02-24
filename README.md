# keiba

## JV-Link 過去データ取得スクリプト (MVP)

Windows + Python 32bit + JV-Link (COM: JVDTLab.JVLink) を使って過去データを取得し、SQLite に生レコードとして保存するスクリプトです。

### 前提条件

- Windows 環境
- **Python 32bit** (JV-Link の COM コンポーネントが 32bit のため)
  - 例: [Python 3.x Windows x86 installer](https://www.python.org/downloads/windows/)
- JV-Link (JVDTLab.JVLink) がインストール・COM 登録済みであること

### セットアップ

```bat
rem Python 32bit 環境で実行する
pip install -r requirements.txt
```

### 実行例

```bat
rem 単一 DataSpec
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE

rem 複数 DataSpec (カンマ区切り)
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE,TOKU

rem オプション全指定
python scripts/jv_ingest_raw.py --from-date 20240101000000 --dataspec RACE --data-option 1 --db jv_data.db
```

### オプション

| オプション        | 必須 | 説明                                                                  |
|-----------------|------|-----------------------------------------------------------------------|
| `--from-date`   | ✓    | データ提供開始日時。`YYYYMMDD` または `YYYYMMDD000000` 形式             |
| `--dataspec`    | ✓    | DataSpec をカンマ区切りで指定。例: `RACE` / `RACE,TOKU`               |
| `--data-option` |      | 1=通常データ (デフォルト), 2=今週データ, 3=セットアップデータ           |
| `--db`          |      | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                      |
| `--sid`         |      | JVInit に渡すサービスID (デフォルト: `UNKNOWN`。空文字で -101 が返る環境では `UNKNOWN` が必要) |

利用可能な DataSpec: `TOKU`, `RACE`, `DIFF`, `BLOD`, `SNAP`, `SLOP`, `WOOD`, `YSCH`, `HOSE`, `HOYU`, `COMM`, `MING`

> **注意**: `DIFF` など差分系の DataSpec は、契約プランや提供対象範囲によっては `JVOpen` が `-1` で失敗することがあります。
> その場合、該当 DataSpec はスキップされ、他の DataSpec の処理は継続されます。

### トラブルシューティング

| 症状 | 対処 |
|------|------|
| `JVInit` が `-101` で失敗する | `--sid ""` のように空文字を渡していた場合は引数を省略するか `--sid UNKNOWN` を指定してください (デフォルトは `UNKNOWN`) |
| `JVOpen` が `-1` で失敗する | 契約・提供対象外の DataSpec の可能性があります。`--dataspec` から該当 DataSpec を除外してください |

### 保存先 SQLite テーブル

`raw_jv_records` テーブルに以下のカラムで保存されます:

| カラム          | 型      | 説明                         |
|---------------|---------|------------------------------|
| id            | INTEGER | 主キー (自動採番)              |
| dataspec      | TEXT    | 取得時の DataSpec             |
| buffname      | TEXT    | JVRead が返すファイル名        |
| payload_text  | TEXT    | cp932 デコード済みの生レコード  |
| payload_size  | INTEGER | レコードサイズ (bytes)         |
| fetched_at    | TEXT    | 取得日時 (ISO 8601)           |

スキーマ定義: `db/schema.sql`
