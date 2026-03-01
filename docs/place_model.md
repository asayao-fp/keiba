# is_place 予測パイプライン (LightGBM)

CatBoost モデル (`train_place_model.py`) とは別に、LightGBM を使った
**学習 → スコアリング → 推薦 CSV 生成** のエンドツーエンドパイプラインです。

`surface` や `grade_code` などのカテゴリ列を **ドロップせず** LightGBM の
`categorical_feature` 機能で扱うことで、モデル精度の改善を図っています。

---

## 必要な Python パッケージ

```bash
pip install pandas scikit-learn lightgbm joblib
```

---

## エンドツーエンド手順

### Step 1. combined CSV 生成

`build_place_training_data.py` に `--include-unlabeled` を付けると、
ラベル済み行 (`is_place` に値あり) と未ラベル行 (`is_place` が NULL) を
**両方含む** CSV を出力します。

```bat
rem Windows (cmd)
python scripts/build_place_training_data.py ^
    --db jv_data.db ^
    --out data/place_combined.csv ^
    --include-unlabeled
```

```powershell
# PowerShell
python scripts/build_place_training_data.py `
    --db jv_data.db `
    --out data/place_combined.csv `
    --include-unlabeled
```

### Step 2. ラベル済み / 未ラベルに分割

```bat
python scripts/split_labeled_unlabeled_csv.py ^
    --in data/place_combined.csv ^
    --labeled data/place_labeled.csv ^
    --unlabeled data/place_unlabeled.csv
```

```powershell
python scripts/split_labeled_unlabeled_csv.py `
    --in data/place_combined.csv `
    --labeled data/place_labeled.csv `
    --unlabeled data/place_unlabeled.csv
```

出力例:

```
[INFO] 読み込み: data/place_combined.csv
[INFO] 合計: 12000 件
[INFO]   ラベル済み: 10000 件 → data/place_labeled.csv
[INFO]   未ラベル:   2000 件 → data/place_unlabeled.csv
```

### Step 3. モデル学習

```bat
python scripts/train_place_model_lgbm.py ^
    --train-csv data/place_labeled.csv ^
    --model-out models/place_lgbm.pkl
```

```powershell
python scripts/train_place_model_lgbm.py `
    --train-csv data/place_labeled.csv `
    --model-out models/place_lgbm.pkl
```

主なオプション:

| オプション        | デフォルト | 説明                            |
|-----------------|----------|---------------------------------|
| `--train-csv`   | `data/place_labeled.csv` | 学習データ CSV       |
| `--model-out`   | `models/place_lgbm.pkl`  | joblib 形式モデル出力パス |
| `--n-estimators`| `500`    | ブースティング回数               |
| `--learning-rate`| `0.05`  | 学習率                          |
| `--num-leaves`  | `31`     | 葉ノード数                      |

出力例:

```
[INFO] 学習データ読み込み: data/place_labeled.csv
[INFO] 学習開始 (train=8000, val=2000)
[INFO] Val AUC: 0.7312
[INFO] モデルを保存しました: models/place_lgbm.pkl
```

### Step 4. 未ラベルデータにスコアリング

```bat
python scripts/predict_place_model_lgbm.py ^
    --in data/place_unlabeled.csv ^
    --model models/place_lgbm.pkl ^
    --out data/place_scored.csv
```

```powershell
python scripts/predict_place_model_lgbm.py `
    --in data/place_unlabeled.csv `
    --model models/place_lgbm.pkl `
    --out data/place_scored.csv
```

出力 CSV には元の列に加えて `pred_is_place_proba` 列が追加されます。

### Step 5. 推薦 CSV 生成 (DB メタデータ結合)

```bat
python scripts/make_place_recommendations_rich.py ^
    --scored-csv data/place_scored.csv ^
    --db jv_data.db ^
    --out data/place_recommendations.csv ^
    --topn 3
```

```powershell
python scripts/make_place_recommendations_rich.py `
    --scored-csv data/place_scored.csv `
    --db jv_data.db `
    --out data/place_recommendations.csv `
    --topn 3
```

主なオプション:

| オプション      | デフォルト | 説明                                        |
|---------------|----------|--------------------------------------------|
| `--scored-csv`| `data/place_scored.csv`   | スコア済み CSV パス     |
| `--db`        | `jv_data.db`              | SQLite DB パス          |
| `--out`       | `data/place_recommendations.csv` | 出力 CSV パス  |
| `--topn`      | `3`                       | レースごとの上位 N 頭数  |

#### 出力 CSV 列 (優先順)

| 列名                    | 説明                              |
|------------------------|----------------------------------|
| `race_date`            | レース日 (yyyymmdd)               |
| `race_key`             | レースキー                        |
| `course_code`          | 競馬場コード                      |
| `race_no`              | レース番号                        |
| `distance_m`           | 距離 (m)                          |
| `surface`              | 馬場種別 (芝/ダート 等)           |
| `grade_code`           | グレードコード                    |
| `race_name_short`      | レース名略称                      |
| `rank_in_race`         | レース内ランク (1=最高スコア)      |
| `horse_no`             | 馬番                              |
| `horse_id`             | 馬 ID                             |
| `horse_name`           | 馬名 (horses テーブルが必要)      |
| `jockey_name_short`    | 騎手名略称 (jockey_aliases が必要) |
| `trainer_name_short`   | 調教師名略称 (trainer_aliases が必要) |
| `body_weight`          | 馬体重 (kg)                       |
| `handicap_weight_x10`  | 斤量 × 10                        |
| `pred_is_place_proba`  | 複勝圏確率 (モデルスコア)         |

---

## スクリプト一覧

| スクリプト                           | 説明                                                    |
|------------------------------------|---------------------------------------------------------|
| `build_place_training_data.py`     | DB から combined CSV 生成 (`--include-unlabeled` 対応)   |
| `split_labeled_unlabeled_csv.py`   | combined CSV をラベル済み / 未ラベルに分割               |
| `train_place_model_lgbm.py`        | LightGBM で is_place モデルを学習 (joblib 保存)          |
| `predict_place_model_lgbm.py`      | 学習済みモデルで CSV にスコアリング                      |
| `make_place_recommendations_rich.py` | スコア済み CSV + DB を結合してレースごと上位 N 推薦を出力 |

---

## 特徴量

### 数値特徴量

| 列名                    | 説明                             |
|------------------------|----------------------------------|
| `body_weight`          | 馬体重                           |
| `handicap_weight_x10`  | 斤量 × 10                        |
| `distance_m`           | 距離 (m)                          |
| `avg_pos_1c_last3`     | 直近3走の1コーナー平均通過順位    |
| `avg_pos_4c_last3`     | 直近3走の4コーナー平均通過順位    |
| `avg_gain_last3`       | 直近3走の平均順位変動             |
| `front_rate_last3`     | 直近3走の先行率                  |
| `avg_pos_1c_pct_last3` | 直近3走の1コーナー相対位置        |
| `avg_pos_4c_pct_last3` | 直近3走の4コーナー相対位置        |
| `n_past`               | 過去出走数                       |

### カテゴリ特徴量 (LightGBM categorical_feature)

| 列名           | 説明              |
|--------------|------------------|
| `surface`    | 馬場種別          |
| `grade_code` | グレードコード    |
| `course_code`| 競馬場コード      |
| `track_code` | トラックコード    |
| `jockey_code`| 騎手コード        |
| `trainer_code`| 調教師コード     |
