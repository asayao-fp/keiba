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

---

## 正規化テーブル生成スクリプト

`raw_jv_records` テーブルに取り込んだレコードを固定長パースして、正規化テーブル (`races` / `entries` / `jockeys` / `trainers`) を生成します。

> **斤量と馬体重の違い**
> - **斤量** (負担重量): 騎手・鞍・装備を含む負担重量 (kg)。レース条件として定義される。
> - **馬体重**: 出走馬自身の体重 (kg)。SE レコード (馬毎レース情報) から取得。
>
> 現状 `entries.body_weight` には SE レコード由来の **馬体重** が格納されます。

### 実行手順

```bat
rem 1. raw ingest (RACE DataSpec + マスタ DataSpec)
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE,MING

rem 2. マスタテーブル生成 (jockeys 等)
python scripts/build_masters_from_raw.py --db jv_data.db

rem 3. レース・出走テーブル生成
python scripts/build_tables_from_raw.py --db jv_data.db

rem 重賞レース (grade_code が空白以外) のみを出力テーブルに残す場合
python scripts/build_tables_from_raw.py --db jv_data.db --graded-only
```

### オプション

#### `build_masters_from_raw.py`

| オプション | 説明                                                              |
|----------|-------------------------------------------------------------------|
| `--db`   | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                  |

#### `build_tables_from_raw.py`

| オプション       | 説明                                                                 |
|----------------|----------------------------------------------------------------------|
| `--db`         | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                     |
| `--graded-only`| 重賞レース (grade_code が空白以外) のレース・出走のみを出力テーブルに残す |

### 出力テーブル

#### `races` テーブル

| カラム           | 型      | 説明                                       |
|----------------|---------|--------------------------------------------|
| race_key       | TEXT    | 主キー (`yyyymmddcoursekaidayraceno`)       |
| yyyymmdd       | TEXT    | 開催年月日 (8桁)                            |
| course_code    | TEXT    | 競馬場コード (2桁)                          |
| kai            | TEXT    | 開催回 (2桁)                               |
| day            | TEXT    | 開催日目 (2桁)                             |
| race_no        | TEXT    | レース番号 (2桁)                           |
| grade_code     | TEXT    | グレードコード (空白=平場, A/B/C等=重賞)    |
| race_name_short| TEXT    | 競走名略称 (全角3文字)                      |
| created_at     | TEXT    | レコード生成日時 (ISO 8601)                 |

#### `entries` テーブル

| カラム       | 型      | 説明                                        |
|------------|---------|---------------------------------------------|
| entry_key  | TEXT    | 主キー (`race_key` + `horse_no`)             |
| race_key   | TEXT    | レースキー (`races.race_key` 参照)           |
| horse_no   | TEXT    | 馬番 (2桁)                                  |
| horse_id   | TEXT    | 血統登録番号 (10桁)                          |
| finish_pos | INTEGER | 確定着順 (欠場・非完走等は NULL)              |
| is_place   | INTEGER | 3着以内なら 1、4着以下なら 0、NULL=着順不明  |
| jockey_code| TEXT    | 騎手コード (5桁, SE レコード由来)            |
| trainer_code| TEXT   | 調教師コード (5桁, SE レコード由来)          |
| body_weight| INTEGER | 馬体重 (kg, 取得不可の場合は NULL)           |

#### `jockeys` テーブル

| カラム       | 型      | 説明                                        |
|------------|---------|---------------------------------------------|
| jockey_code| TEXT    | 主キー (5桁)                                |
| jockey_name| TEXT    | 騎手名 (全角, 姓+空白+名。外国人は連続)      |
| updated_at | TEXT    | レコード更新日時 (ISO 8601)                  |

#### `trainers` テーブル

| カラム        | 型      | 説明                                        |
|-------------|---------|---------------------------------------------|
| trainer_code| TEXT    | 主キー (5桁)                                |
| trainer_name| TEXT    | 調教師名 (NULL 可)                           |
| updated_at  | TEXT    | レコード更新日時 (ISO 8601)                  |

### 簡易検証 (件数集計)

```bat
rem races テーブルの件数
sqlite3 jv_data.db "SELECT COUNT(*) FROM races;"

rem entries テーブルの件数
sqlite3 jv_data.db "SELECT COUNT(*) FROM entries;"

rem jockeys テーブルの件数
sqlite3 jv_data.db "SELECT COUNT(*) FROM jockeys;"

rem 重賞レースのみ集計
sqlite3 jv_data.db "SELECT COUNT(*) FROM races WHERE TRIM(grade_code) != '';"

rem 3着以内入着の出走数
sqlite3 jv_data.db "SELECT COUNT(*) FROM entries WHERE is_place = 1;"

rem 馬体重が記録された出走数
sqlite3 jv_data.db "SELECT COUNT(*) FROM entries WHERE body_weight IS NOT NULL;"
```
