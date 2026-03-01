# raw レコードレイアウト調査ワークフロー

`raw_jv_records` に取り込まれた固定長 JV レコード (JG1 / HR2 / H15 / WF7 など)
のレイアウトを安全に調査し、正規化パイプラインを拡張するための手順書です。

---

## 全体の流れ

```
1. jv_ingest_raw.py      — 生データを SQLite に取り込む
2. summarize_raw_prefix_counts.py  — プレフィックス別件数を確認する
3. inspect_raw_layouts.py          — サンプルを目視確認・日付スライス候補を検証する
4. build_tables_from_raw.py        — 検証済みのパース関数で正規化テーブルを生成する
5. update_db_from_raw.py           — 派生テーブルを一括再構築する
```

---

## Step 1: 生データの取り込み

```bat
python scripts/jv_ingest_raw.py --from-date 20240101 --dataspec RACE --db jv_data.db
```

`raw_jv_records` テーブルに `payload_text` / `payload_size` / `dataspec` などが保存されます。

---

## Step 2: プレフィックス別件数の確認

```bat
python scripts/summarize_raw_prefix_counts.py --db jv_data.db
```

**オプション**

| オプション    | デフォルト | 説明                              |
|------------|-----------|----------------------------------|
| `--db`     | `jv_data.db` | SQLite DB ファイルパス          |
| `--dataspec` | `RACE`   | 集計対象の dataspec              |
| `--limit`  | `30`      | 各 prefix_len で上位 N 件を表示  |

**出力例**

```
[INFO] DB: jv_data.db
[INFO] dataspec='RACE'  総レコード数: 2,234,567

[INFO] prefix_len=2 TOP 30
  PREFIX          COUNT
  ----------  ----------
  JG        692,345
  SE        614,210
  HR         44,012
  ...

[INFO] prefix_len=3 TOP 30
  PREFIX          COUNT
  ----------  ----------
  JG1       692,345
  SE7       614,210
  HR2        22,456
  H15        21,556
  WF7           838
  ...
```

ここで各プレフィックスの件数を把握し、次の Step で優先的に調査するレコード種別を決定します。

---

## Step 3: レコードサンプルと日付スライス候補の確認

```bat
python scripts/inspect_raw_layouts.py --db jv_data.db --prefix JG1 HR2 H15 WF7
```

デフォルトで以下の位置 (1-始まりバイト位置, バイト長) を日付候補として試みます:

| pos | length | 説明                          |
|-----|--------|-------------------------------|
| 4   | 8      | RA/SE 系以外で多い先頭付近    |
| 9   | 8      | 一部レコード形式              |
| 11  | 8      | 一部レコード形式              |
| 12  | 8      | RA/SE 系の標準位置            |

**オプション**

| オプション       | デフォルト    | 説明                                              |
|---------------|-------------|--------------------------------------------------|
| `--db`        | `jv_data.db` | SQLite DB ファイルパス                           |
| `--dataspec`  | `RACE`       | 対象の dataspec                                 |
| `--prefix`    | (必須)       | 調査するプレフィックス (複数指定可)               |
| `--limit`     | `1000`       | 各プレフィックスのサンプル上限                    |
| `--samples`   | `5`          | テキスト先頭を表示するサンプル件数               |
| `--chars`     | `120`        | テキスト先頭の表示文字数                         |
| `--date-slice`| (デフォルト候補) | 試みる日付スライス `pos,length` (複数指定可)  |

**スライスを明示する例** (Unix/macOS)

```bash
python scripts/inspect_raw_layouts.py --db jv_data.db --prefix JG1 \
    --date-slice 12,8 --date-slice 4,8
```

**スライスを明示する例** (Windows)

```bat
python scripts/inspect_raw_layouts.py --db jv_data.db --prefix JG1 ^
    --date-slice 12,8 --date-slice 4,8
```

**出力例**

```
[INFO] DB: jv_data.db
[INFO] dataspec: RACE
[INFO] prefixes: ['JG1', 'HR2']
[INFO] date-slices: [(4, 8), (9, 8), (11, 8), (12, 8)]

============================================================
[INFO] prefix='JG1'  dataspec='RACE'  サンプル数: 1,000 / limit=1000
[INFO] payload_size (stored)  min=600  median=600  max=600
[INFO] len(payload_text)      min=598  median=599  max=600
[INFO] date-slice pos=4,len=8   hit=0/1000 (0.0%)  — YYYYMMDD パターンなし
[INFO] date-slice pos=12,len=8  hit=1000/1000 (100.0%)  min=20200101  max=20260228
[INFO] payload_text 先頭 120 文字 サンプル (最大 5 件):
  [1] 'JG120200101...'
  ...
```

`hit` が高い (例: 100%) スライス位置が実際の日付フィールドの位置候補です。
`min` / `max` 値が実際のレース日付範囲と一致するかを目視で確認します。

---

## Step 4: 正規化テーブルの生成

調査結果をもとに `scripts/build_tables_from_raw.py` にパース関数を追加・検証した後:

```bat
python scripts/build_tables_from_raw.py --db jv_data.db
```

---

## Step 5: 派生テーブルの一括再構築

```bat
python scripts/update_db_from_raw.py --db jv_data.db
```

---

## 注意事項

- `inspect_raw_layouts.py` および `summarize_raw_prefix_counts.py` は **読み取り専用** です。
  DB への書き込みは一切行いません。
- `payload_text` は cp932 デコード済みの文字列として保存されています。
  `inspect_raw_layouts.py` の `--date-slice` は ASCII 部分のバイト位置と文字位置が一致することを前提としています。
- スクリプトは Python 標準ライブラリのみ使用しており、追加パッケージは不要です。
