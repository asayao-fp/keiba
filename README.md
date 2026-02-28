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
| `--from-date`     | ✓    | データ提供開始日時。`YYYYMMDD` または `YYYYMMDD000000` 形式             |
| `--dataspec`      | ✓    | DataSpec をカンマ区切りで指定。例: `RACE` / `RACE,TOKU`               |
| `--data-option`   |      | 1=通常データ (デフォルト), 2=今週データ, 3=セットアップデータ           |
| `--db`            |      | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                      |
| `--sid`           |      | JVInit に渡すサービスID (デフォルト: `UNKNOWN`。空文字で -101 が返る環境では `UNKNOWN` が必要) |
| `--allow-no-data` |      | `JVOpen` が `-111` (データなし) を返した場合をエラーとして扱わず警告のみ表示して続行する。定期実行でオッズ等のデータが存在しない時間帯にも終了コード 0 で完了させたい場合に使用する。 |

利用可能な DataSpec: `TOKU`, `RACE`, `DIFF`, `BLOD`, `SNAP`, `SLOP`, `WOOD`, `YSCH`, `HOSE`, `HOYU`, `COMM`, `MING`, `ODDS` (O1 オッズ)

> **注意**: `DIFF` など差分系の DataSpec は、契約プランや提供対象範囲によっては `JVOpen` が `-1` で失敗することがあります。
> その場合、該当 DataSpec はスキップされ、他の DataSpec の処理は継続されます。

> **ODDS / O1 について**: オッズ系 DataSpec (`ODDS` / `O1`) は、レース当日・発走前後などの限られた時間帯のみデータが提供されます。
> データが存在しない時間帯に実行すると `JVOpen` が `-111` を返します。
> 定期実行スクリプトではデータなしを非致命的として扱いたい場合は `--allow-no-data` フラグを追加してください:
>
> ```bat
> python scripts/jv_ingest_raw.py --from-date 20260222 --dataspec O1 --db jv_data.db --allow-no-data
> ```

### トラブルシューティング

| 症状 | 対処 |
|------|------|
| `JVInit` が `-101` で失敗する | `--sid ""` のように空文字を渡していた場合は引数を省略するか `--sid UNKNOWN` を指定してください (デフォルトは `UNKNOWN`) |
| `JVOpen` が `-1` で失敗する | 契約・提供対象外の DataSpec の可能性があります。`--dataspec` から該当 DataSpec を除外してください |
| `JVOpen` が `-111` で失敗する (ODDS/O1 など) | データが現時点で提供されていません。オッズ系はレース当日のみ取得可能です。定期実行では `--allow-no-data` を付けて非致命的エラーとして扱ってください |
| `build_tables_from_raw.py` で `MemoryError` が発生する | `build_tables_from_raw.py` はストリーミング処理 (カーソルイテレーション) を採用しており、全件を一度にメモリに読み込まないため MemoryError は発生しません。旧バージョンをお使いの場合は最新版に更新してください。 |

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

> **既存 DB への列追加 (冪等)**: `build_tables_from_raw.py` を再実行すると、`races` テーブルに `distance_m` / `track_code` / `surface` 列が存在しない場合は自動的に追加されます。既に列が存在する場合はスキップされます。

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
| distance_m     | INTEGER | 距離 (メートル, 取得不可の場合は NULL)       |
| track_code     | TEXT    | トラックコード2009 (2桁, 取得不可の場合は NULL) |
| surface        | TEXT    | 馬場種別 (芝/ダート/サンド/障害/不明, track_code から導出) |
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
| handicap_weight_x10| INTEGER | 斤量=負担重量 (単位 0.1kg, 例: 550=55.0kg, 取得不可の場合は NULL) |

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

rem 斤量 (負担重量) が記録された出走数
sqlite3 jv_data.db "SELECT COUNT(*) FROM entries WHERE handicap_weight_x10 IS NOT NULL;"

rem 馬体重の min/max/avg
sqlite3 jv_data.db "SELECT MIN(body_weight), MAX(body_weight), AVG(body_weight) FROM entries WHERE body_weight IS NOT NULL;"

rem 斤量の min/max/avg (単位 0.1kg)
sqlite3 jv_data.db "SELECT MIN(handicap_weight_x10), MAX(handicap_weight_x10), AVG(handicap_weight_x10) FROM entries WHERE handicap_weight_x10 IS NOT NULL;"

rem 距離の件数・min/max/avg
sqlite3 jv_data.db "SELECT COUNT(*), MIN(distance_m), MAX(distance_m), AVG(distance_m) FROM races WHERE distance_m IS NOT NULL;"

rem トラックコードごとのレース数
sqlite3 jv_data.db "SELECT track_code, COUNT(*) FROM races WHERE track_code IS NOT NULL GROUP BY track_code ORDER BY COUNT(*) DESC;"

rem 馬場種別ごとのレース数
sqlite3 jv_data.db "SELECT surface, COUNT(*) FROM races GROUP BY surface ORDER BY COUNT(*) DESC;"
```

---

## 予測モデル (複勝圏)

`races.distance_m` (距離, メートル)、`races.track_code` (トラックコード2009)、および `races.surface` (馬場種別: 芝/ダート/サンド/障害/不明) が学習・推論の特徴量として使用されます。

| 特徴量カテゴリ  | 列名                                                                        |
|--------------|-----------------------------------------------------------------------------|
| 数値特徴量     | `body_weight`, `handicap_weight_x10`, `distance_m`                         |
| カテゴリ特徴量 | `jockey_code`, `trainer_code`, `course_code`, `grade_code`, `track_code`, `surface` |
| 通過順特徴量   | `avg_pos_1c_last3`, `avg_pos_4c_last3`, `avg_gain_last3`, `front_rate_last3`, `avg_pos_1c_pct_last3`, `avg_pos_4c_pct_last3`, `n_past` |

### 学習前の事前準備 (特徴量テーブル構築)

通過順特徴量を使用するため、学習データ生成の前に以下の 2 スクリプトを実行してください。

**ステップ 1: RA7 通過順テーブルの構築**

`raw_jv_records` の RA7 レコードを解析して `race_passing_positions` テーブルを作成します。

```bat
python scripts/build_race_passing_positions_from_ra7.py --db jv_data.db
```

| オプション      | デフォルト | 説明                                      |
|--------------|---------|------------------------------------------|
| `--db`       | `jv_data.db` | SQLite DB ファイルパス                  |
| `--tail-len` | `900`   | RA7 レコード末尾から参照する文字数            |

**ステップ 2: 馬別過去通過順特徴量テーブルの構築**

`race_passing_positions` を元に `horse_past_passing_features` テーブルを作成します。

```bat
python scripts/build_horse_past_passing_features.py --db jv_data.db --n-last 3
```

| オプション    | デフォルト | 説明                                   |
|------------|---------|---------------------------------------|
| `--db`     | `jv_data.db` | SQLite DB ファイルパス               |
| `--n-last` | `3`     | 集計対象の過去レース数                    |

### 学習データ生成

```bat
python scripts/build_place_training_data.py --db jv_data.db --out data/place_train.csv
```

### 学習

```bat
python scripts/train_place_model.py --train-csv data/place_train.csv --model-out models/place_model.cbm
```

### 推論

```bat
python scripts/predict_place.py --db jv_data.db --race-key <RACE_KEY> --model models/place_model.cbm
```

---

## ワイド (Wide) モデル

同一レース内の全馬ペアに対して、両馬とも複勝圏 (is_place=1) なら target=1 とするペア分類モデルです。

### 学習データ生成

```bat
python scripts/build_wide_training_data.py --db jv_data.db --out data/wide_train.csv
```

オプション:

| オプション              | デフォルト             | 説明                                     |
|----------------------|----------------------|------------------------------------------|
| `--db`               | `jv_data.db`         | SQLite DB ファイルパス                    |
| `--out`              | `data/wide_train.csv`| 出力 CSV パス                             |
| `--from`             | (なし)               | 取得開始日 (例: `20200101`)               |
| `--to`               | (なし)               | 取得終了日 (例: `20231231`)               |
| `--neg-sample-per-pos`| `10`                | 陽性 1 件あたり陰性ダウンサンプル数         |
| `--seed`             | `42`                 | 乱数シード                                |

### 学習

```bat
python scripts/train_wide_model.py --train-csv data/wide_train.csv --model-out models/wide_model.cbm
```

出力: `models/wide_model.cbm`

### 推論

```bat
python scripts/predict_wide.py --db jv_data.db --race-key <RACE_KEY> --model models/wide_model.cbm --topn 10
```

---

## 3連複 (Sanrenpuku) モデル

同一レース内の全馬トリプルに対して、3頭全てが複勝圏 (is_place=1) なら target=1 とするトリプル分類モデルです。

### 学習データ生成

```bat
python scripts/build_sanrenpuku_training_data.py --db jv_data.db --out data/sanrenpuku_train.csv
```

オプション:

| オプション              | デフォルト                    | 説明                                     |
|----------------------|------------------------------|------------------------------------------|
| `--db`               | `jv_data.db`                 | SQLite DB ファイルパス                    |
| `--out`              | `data/sanrenpuku_train.csv`  | 出力 CSV パス                             |
| `--from`             | (なし)                       | 取得開始日 (例: `20200101`)               |
| `--to`               | (なし)                       | 取得終了日 (例: `20231231`)               |
| `--neg-sample-per-pos`| `20`                        | 陽性 1 件あたり陰性ダウンサンプル数         |
| `--seed`             | `42`                         | 乱数シード                                |

### 学習

```bat
python scripts/train_sanrenpuku_model.py --train-csv data/sanrenpuku_train.csv --model-out models/sanrenpuku_model.cbm
```

出力: `models/sanrenpuku_model.cbm`

### 推論

```bat
python scripts/predict_sanrenpuku.py --db jv_data.db --race-key <RACE_KEY> --model models/sanrenpuku_model.cbm --topn 10
```

---

## 複勝予測スクリプト

学習済みモデルを使って指定レースの複勝圏確率 p_place を推論します。

### 実行例

```bat
rem 推論 (JSON 出力)
python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm --format json > pred.json

rem 推論 (JSONL 出力、デフォルト)
python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm

rem 推論 (テーブル表示)
python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm --format table
```

### オプション

| オプション     | 必須 | 説明                                                                      |
|--------------|------|---------------------------------------------------------------------------|
| `--db`       |      | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                          |
| `--race-key` | ✓    | レースキー (例: `202401010102010101`)                                      |
| `--model`    |      | 学習済みモデルパス (デフォルト: `models/place_model.cbm`)                  |
| `--format`   |      | 出力フォーマット: `jsonl` / `json` / `table` (デフォルト: `jsonl`)         |

---

## 買い目提案スクリプト

予測 JSON とオッズ CSV (または DB) を突合して期待値を計算し、複勝買い目候補を出力します。

### オッズ CSV 仕様

`data/sample_place_odds.csv` を参照してください。ヘッダ必須、追加列は無視されます。

| 列名              | 説明               |
|-----------------|--------------------|
| `horse_no`      | 馬番               |
| `place_odds_min`| 複勝オッズ 最小値  |
| `place_odds_max`| 複勝オッズ 最大値  |

```csv
horse_no,place_odds_min,place_odds_max
01,2.5,3.1
02,4.0,5.2
03,1.8,2.3
```

### 予測→オッズ CSV 用意→買い目提案 の実行例

> **PowerShell をお使いの場合**: PowerShell の `>` リダイレクトは UTF-16LE または UTF-8 BOM 付きでファイルを保存することがあります。
> `suggest_place_bets.py` はこれらのエンコーディングを自動検知して読み込めます。
> より確実な方法として、以下のように明示的に UTF-8 (BOM なし) で保存することも可能です:
> ```powershell
> python scripts/predict_place.py ... --format json | Out-File -Encoding utf8NoBOM pred.json
> ```

```bat
rem 1. 複勝圏確率を推論して JSON に保存
python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm --format json > pred.json

rem 2. オッズ CSV を用意 (手入力 or 別途取得)
rem    data/sample_place_odds.csv を参考に作成してください。

rem 3. 買い目候補を JSON で出力 (デフォルト: min オッズ, 期待値 >= 0.0, 上位 3 点, 100 円賭け)
python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv

rem 4. CSV 出力、オッズは中央値、期待値しきい値 0.05、1点 500 円、最大 5 点
python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv --format csv --odds-use mid --min-ev 0.05 --stake 500 --max-bets 5

rem 5. 当たりやすさ優先: p_place 降順, 複勝確率 0.22 以上, オッズ 12 以下, min オッズ使用
python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv --rank-by p --min-p-place 0.22 --max-odds-used 12 --odds-use min

rem 6. 収益性を維持しつつ当たりやすさにも配慮 (balance プリセット)
python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv --mode balance

rem    balance プリセットと同等の明示的な指定
python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv --rank-by ev_then_p --min-p-place 0.20 --max-odds-used 15 --min-ev 0
```

### オプション

| オプション      | 必須        | 説明                                                                                 |
|---------------|-------------|--------------------------------------------------------------------------------------|
| `--pred-json` | ✓           | `predict_place.py` が出力した JSON ファイルパス                                      |
| `--odds-csv`  |             | オッズ CSV ファイルパス。省略時は `--db` / `--race-key` から DB を参照              |
| `--db`        | ※CSV省略時✓ | SQLite DB ファイルパス (`--odds-csv` 省略時に使用)                                   |
| `--race-key`  | ※CSV省略時✓ | レースキー (`--odds-csv` 省略時に使用)                                               |
| `--format`    |             | 出力フォーマット: `json` / `csv` (デフォルト: `json`)                                |
| `--odds-use`  |             | 使用するオッズ: `min` / `max` / `mid` (デフォルト: `min`)                            |
| `--min-ev`    |             | 期待値しきい値 (デフォルト: `0.0`)。これ以上の期待値の馬のみを候補とする              |
| `--stake`     |             | 1点あたり賭け金・円 (デフォルト: `100`)                                              |
| `--max-bets`  |             | 最大購入点数 (デフォルト: `3`)                                                       |
| `--rank-by`   |             | ランキング基準: `ev`=期待値降順 (デフォルト) / `p`=確率降順 / `ev_then_p`=期待値→確率 |
| `--min-p-place` |           | 複勝圏確率の下限 (デフォルト: `0.0`、balance モード時: `0.20`)。これ未満の候補は除外  |
| `--max-odds-used` |         | 使用オッズの上限 (デフォルト: なし、balance モード時: `15`)。これを超える候補は除外   |
| `--mode`      |             | 運用プリセット: `balance`=収益性を維持しつつ当たりやすさにも配慮。明示指定した引数は優先 |

### 出力フィールド

| フィールド           | 説明                                           |
|--------------------|------------------------------------------------|
| `horse_no`         | 馬番                                           |
| `horse_id`         | 血統登録番号                                   |
| `p_place`          | 複勝圏確率                                     |
| `place_odds_min`   | 複勝オッズ 最小値                              |
| `place_odds_max`   | 複勝オッズ 最大値                              |
| `place_odds_used`  | 期待値計算に使用したオッズ                     |
| `ev_per_1unit`     | 1単位賭けあたりの期待値 (`p_place * odds - 1`) |
| `stake`            | 賭け金 (円)                                    |
| `expected_value_yen` | 期待値 (円) = `ev_per_1unit * stake`         |

---

## 複勝オッズ自動取得 (O1 レコード)

JV-Link から O1 レコード (単複オッズ) を取得して DB に格納することで、CSVを手入力せずにオッズを自動取得できます。

### オッズ取得〜買い目提案 の手順

```bat
rem 1. O1 レコードを含む DataSpec (例: ODDS) で ingest
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE,ODDS

rem 2. O1 レコードをパースして place_odds テーブルを生成
python scripts/build_place_odds_from_raw.py --db jv_data.db

rem    特定の dataspec に絞る場合
python scripts/build_place_odds_from_raw.py --db jv_data.db --dataspec ODDS

rem 3. 推論
python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm --format json > pred.json

rem 4. CSV なしで DB からオッズを自動取得して買い目提案
python scripts/suggest_place_bets.py --pred-json pred.json --db jv_data.db --race-key 202401010102010101
```

### `build_place_odds_from_raw.py` オプション

| オプション    | 説明                                                                       |
|-------------|----------------------------------------------------------------------------|
| `--db`      | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                           |
| `--dataspec`| dataspec で絞り込む場合に指定 (省略時は全レコードから O1 を検索)            |

### `place_odds` テーブル

| カラム           | 型      | 説明                                       |
|----------------|---------|--------------------------------------------|
| race_key       | TEXT    | レースキー (主キーの一部)                    |
| horse_no       | TEXT    | 馬番 (主キーの一部)                          |
| place_odds_min | REAL    | 複勝オッズ 最小値 (4桁整数÷10)              |
| place_odds_max | REAL    | 複勝オッズ 最大値 (4桁整数÷10)              |
| announced_at   | TEXT    | 発表日時 (yyyy + mmddHHMM)                  |
| updated_at     | TEXT    | 処理日時 (ISO 8601)                         |

---

## 複数レース一括買い目提案

`batch_suggest_place_bets.py` を使うと、複数レースの予測・買い目提案を一括で処理し、集計 CSV を出力できます。
オッズは DB (`place_odds` テーブル) から自動取得します。

### 実行例

```bat
rem 2レースをスペース区切りで指定
python scripts/batch_suggest_place_bets.py ^
    --race-keys 202401010102010101 202401010102010102 ^
    --db jv_data.db --model models/place_model.cbm ^
    --out-dir out/

rem レースキーをファイルで指定 (race_keys.txt: 1行1キー)
python scripts/batch_suggest_place_bets.py ^
    --race-keys-file race_keys.txt ^
    --db jv_data.db --model models/place_model.cbm ^
    --out-dir out/ --summary-csv out/summary.csv

rem balance プリセット + 期待値しきい値 0.05 を適用
python scripts/batch_suggest_place_bets.py ^
    --race-keys 202401010102010101 202401010102010102 ^
    --db jv_data.db --model models/place_model.cbm ^
    --out-dir out/ --mode balance --min-ev 0.05

rem 既存の pred_<race_key>.json を再利用して買い目提案のみ実行 (モデル不要)
python scripts/batch_suggest_place_bets.py ^
    --race-keys 202401010102010101 202401010102010102 ^
    --db jv_data.db ^
    --out-dir out/ ^
    --skip-predict --pred-dir out/
```

`race_keys.txt` の例:

```
202401010102010101
202401010102010102
202401010103010101
```

### 出力ファイル

| ファイル                    | 説明                                         |
|---------------------------|----------------------------------------------|
| `pred_<race_key>.json`    | 各レースの複勝圏確率 (馬ごとのリスト)          |
| `bets_<race_key>.json`    | 各レースの買い目候補リスト                     |
| `summary.csv`             | 全レース集計 (デフォルト: `<out-dir>/summary.csv`) |

### `summary.csv` 列

| 列名                      | 説明                                              |
|--------------------------|---------------------------------------------------|
| `race_key`               | レースキー                                        |
| `status`                 | 処理結果: `ok` または `failed`                    |
| `n_bets`                 | 買い目点数                                        |
| `total_stake`            | 合計賭け金 (円)                                   |
| `sum_expected_value_yen` | 買い目全体の期待値合計 (円)                        |
| `avg_p_place`            | 買い目馬の平均複勝圏確率                           |
| `avg_odds_used`          | 買い目馬の平均使用オッズ                           |
| `max_p_place`            | 買い目馬の最大複勝圏確率                           |
| `max_ev_per_1unit`       | 買い目馬の最大期待値 (1単位賭けあたり)              |
| `error`                  | エラーメッセージ (`status=failed` の場合に設定)    |

### オプション

| オプション           | 必須                    | 説明                                                                   |
|--------------------|-------------------------|------------------------------------------------------------------------|
| `--race-keys`      | ※どちらか一方           | レースキー (スペース区切りで複数)                                        |
| `--race-keys-file` | ※どちらか一方           | レースキーを1行1件で記載したテキストファイル                             |
| `--db`             |                         | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                       |
| `--model`          |                         | 学習済みモデルパス (デフォルト: `models/place_model.cbm`)               |
| `--out-dir`        | ✓                       | 出力ディレクトリ                                                        |
| `--summary-csv`    |                         | 集計CSVパス (デフォルト: `<out-dir>/summary.csv`)                       |
| `--mode`           |                         | 運用プリセット: `balance` (`suggest_place_bets.py` と同等)              |
| `--rank-by`        |                         | ランキング基準: `ev` / `p` / `ev_then_p` (デフォルト: `ev`)             |
| `--min-p-place`    |                         | 複勝圏確率の下限しきい値 (デフォルト: `0.0`)                            |
| `--max-odds-used`  |                         | 使用オッズの上限 (デフォルト: なし)                                     |
| `--min-ev`         |                         | 期待値しきい値 (デフォルト: `0.0`)                                      |
| `--odds-use`       |                         | 使用するオッズ: `min` / `max` / `mid` (デフォルト: `min`)               |
| `--stake`          |                         | 1点あたり賭け金・円 (デフォルト: `100`)                                 |
| `--max-bets`       |                         | 最大購入点数 (デフォルト: `3`)                                          |
| `--fail-fast`      |                         | エラー発生時に即座に終了する (デフォルト: 他レースは続行)                |
| `--skip-predict`   |                         | 予測をスキップし、既存の `pred_<race_key>.json` を再利用する (モデル不要) |
| `--pred-dir`       |                         | `--skip-predict` 時に pred JSON を読み込むディレクトリ (デフォルト: `--out-dir` と同じ) |

---

## 直近の重賞レース一覧 (`list_races.py`)

`list_races.py` を使うと、`races` テーブルから日付・グレード・競馬場などの条件でレースを絞り込み、`race_key` 一覧や CSV/JSON 形式で出力できます。出力した `race_key` をそのまま `batch_suggest_place_bets.py` に渡すことで、直近の重賞レースを簡単に一括処理できます。

### 実行例

```bat
rem 直近30日の重賞レース (grade_code=C) の race_key を1行ずつ出力
python scripts/list_races.py --db jv_data.db --days 30 --grade-code C --format keys

rem 複数グレードを指定
python scripts/list_races.py --db jv_data.db --days 30 --grade-code C --grade-code D --format keys

rem 期間指定 + CSV 形式で出力
python scripts/list_races.py --db jv_data.db --from 20240101 --to 20241231 --grade-code C --format csv

rem JSON 形式で出力
python scripts/list_races.py --db jv_data.db --days 14 --format json

rem 競馬場コードで絞り込む (05=東京, 06=中山)
python scripts/list_races.py --db jv_data.db --days 30 --course-code 05 --course-code 06 --format keys

rem レース名の部分一致
python scripts/list_races.py --db jv_data.db --days 60 --name-contains 皐月 --format keys

rem place_odds が存在するレースのみ出力 (バッチ実行前のフィルタリングに便利)
python scripts/list_races.py --db jv_data.db --days 30 --require-place-odds --format keys
```

### `list_races.py` → `batch_suggest_place_bets.py` パイプライン

```bat
rem (bat/cmd) race_key 一覧をファイルに保存してからバッチ実行
python scripts/list_races.py --db jv_data.db --days 30 --grade-code C --format keys > race_keys.txt
python scripts/batch_suggest_place_bets.py ^
    --race-keys-file race_keys.txt ^
    --db jv_data.db --model models/place_model.cbm ^
    --out-dir out/ --summary-csv out/summary.csv
```

```powershell
# (PowerShell) race_key 一覧をファイルに保存してからバッチ実行
python scripts/list_races.py --db jv_data.db --days 30 --grade-code C --format keys |
    Out-File -Encoding utf8NoBOM race_keys.txt
python scripts/batch_suggest_place_bets.py `
    --race-keys-file race_keys.txt `
    --db jv_data.db --model models/place_model.cbm `
    --out-dir out/ --summary-csv out/summary.csv
```

### オプション

| オプション          | 説明                                                                                         |
|-------------------|----------------------------------------------------------------------------------------------|
| `--db`            | SQLite DB ファイルパス (デフォルト: `jv_data.db`)                                             |
| `--days N`        | 今日から遡る日数 (例: `--days 30` → 直近30日)。`--from` と同時指定不可                        |
| `--from YYYYMMDD` | 検索開始日。`--days` と同時指定不可                                                           |
| `--to YYYYMMDD`   | 検索終了日 (省略時: 制限なし)。`--from` と組み合わせて使用                                     |
| `--grade-code`    | グレードコードで絞り込む (複数指定可。例: `-g C -g D`)                                        |
| `--name-contains` | `race_name_short` の部分一致フィルタ                                                          |
| `--course-code`   | 競馬場コードで絞り込む (複数指定可)                                                            |
| `--require-place-odds` | `place_odds` テーブルに `place_odds_min` および `place_odds_max` が NULL でないレコードが存在するレースのみ出力する (NULL のみのレコードは除外) |
| `--format`        | 出力フォーマット: `keys`=race_keyを1行ずつ / `csv` / `json` (デフォルト: `keys`)              |

---

## 自動化ワークフロー (前日準備 + 当日複数回更新)

`update_db_from_raw.py` と `make_today_race_keys.py` を組み合わせることで、**前日夜の準備** と **当日の差分更新** を少ないコマンドで実現できます。

### スクリプト概要

| スクリプト | 説明 |
|---|---|
| `update_db_from_raw.py` | `raw_jv_records` から `races` / `entries` / `jockeys` / `trainers` / `place_odds` テーブルを再構築する (冪等) |
| `make_today_race_keys.py` | 指定日 (デフォルト: 今日) の重賞レース `race_key` を生成してファイルまたは標準出力に書き出す |

### 推奨ワークフロー

#### 前日夜 (night-before prep)

```powershell
# 1. JV-Link から最新の生データを取得 (RACE + ODDS DataSpec)
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE,ODDS --db jv_data.db

# 2. 派生テーブルを一括更新 (races / entries / masters / place_odds)
python scripts/update_db_from_raw.py --db jv_data.db

# 3. 今日の重賞レースキーを生成してファイルに保存
python scripts/make_today_race_keys.py --db jv_data.db --out race_keys.txt

# 4. 買い目提案を一括実行
python scripts/batch_suggest_place_bets.py `
    --race-keys-file race_keys.txt `
    --db jv_data.db --model models/place_model.cbm `
    --out-dir out/ --summary-csv out/summary.csv
```

#### 当日の差分更新 (intraday refresh)

```powershell
# 1. 最新オッズのみ再取得・再構築 (ODDS DataSpec のみ)
# --allow-no-data: レース前後などオッズが提供されていない時間帯でも終了コード 0 で続行する
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec ODDS --db jv_data.db --allow-no-data
python scripts/update_db_from_raw.py --db jv_data.db --skip-masters

# 2. オッズ必須フィルタで今日のレースキーを再生成
python scripts/make_today_race_keys.py --db jv_data.db --require-place-odds --out race_keys.txt

# 3. 買い目提案を再実行
python scripts/batch_suggest_place_bets.py `
    --race-keys-file race_keys.txt `
    --db jv_data.db --model models/place_model.cbm `
    --out-dir out/ --summary-csv out/summary.csv
```

### `update_db_from_raw.py` オプション

| オプション | 説明 |
|---|---|
| `--db` | SQLite DB ファイルパス (デフォルト: `jv_data.db`) |
| `--skip-masters` | `jockeys` / `trainers` マスタテーブルの更新をスキップする |
| `--skip-place-odds` | `place_odds` テーブルの更新をスキップする |

### `make_today_race_keys.py` オプション

| オプション | 説明 |
|---|---|
| `--db` | SQLite DB ファイルパス (デフォルト: `jv_data.db`) |
| `--date YYYYMMDD` | 対象日 (デフォルト: 今日のローカル日付) |
| `--grade-codes CODE ...` | グレードコードで絞り込む (デフォルト: `A B C`) |
| `--require-place-odds` | `place_odds` に `place_odds_min` / `place_odds_max` が NULL でないレコードが存在するレースのみ出力する |
| `--out FILE` | 出力ファイルパス (省略時: 標準出力) |
