"""
train_place_model_lgbm.py
=========================
ラベル済み CSV を読み込んで LightGBM バイナリ分類器で is_place モデルを学習する。
カテゴリ列 (surface, grade_code 等) は pandas category + LightGBM categorical_feature で処理し、
ドロップしない。

使用例:
  python scripts/train_place_model_lgbm.py \
      --train-csv data/place_labeled.csv \
      --model-out models/place_lgbm.pkl
"""

import argparse
import os
import sys

import joblib
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split


DEFAULT_TRAIN_CSV = "data/place_labeled.csv"
DEFAULT_MODEL_OUT = "models/place_lgbm.pkl"

# 数値特徴量
NUMERIC_FEATURES = [
    "body_weight",
    "handicap_weight_x10",
    "distance_m",
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
]

# カテゴリ特徴量 (LightGBM categorical_feature として扱う)
CATEGORICAL_FEATURES = [
    "surface",
    "grade_code",
    "course_code",
    "track_code",
    "jockey_code",
    "trainer_code",
]

FEATURE_COLS = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COL = "is_place"

# 識別用列 (特徴量に含めない)
ID_COLS = ["race_key", "entry_key", "horse_id", "horse_no", "yyyymmdd"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="is_place 予測モデルを LightGBM で学習する"
    )
    parser.add_argument(
        "--train-csv",
        default=DEFAULT_TRAIN_CSV,
        metavar="PATH",
        help=f"学習データ CSV パス (デフォルト: {DEFAULT_TRAIN_CSV})",
    )
    parser.add_argument(
        "--model-out",
        default=DEFAULT_MODEL_OUT,
        metavar="PATH",
        help=f"モデル出力パス (joblib .pkl) (デフォルト: {DEFAULT_MODEL_OUT})",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=500,
        metavar="N",
        help="ブースティング回数 (デフォルト: 500)",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.05,
        metavar="LR",
        help="学習率 (デフォルト: 0.05)",
    )
    parser.add_argument(
        "--num-leaves",
        type=int,
        default=31,
        metavar="N",
        help="葉ノード数 (デフォルト: 31)",
    )
    return parser.parse_args()


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame から特徴量列を取り出し、型を整える。
    - 数値列: pd.to_numeric で変換 (変換不能は NaN)
    - カテゴリ列: 存在する列のみ category 型に変換。欠損は NaN のまま保持
      (LightGBM は category 型の NaN を内部で処理できる)
    - 存在しない列は NaN 列として追加
    """
    result = pd.DataFrame(index=df.index)

    for col in NUMERIC_FEATURES:
        if col in df.columns:
            result[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            result[col] = float("nan")

    for col in CATEGORICAL_FEATURES:
        if col in df.columns:
            result[col] = df[col].astype("category")
        else:
            result[col] = pd.Categorical([None] * len(df))

    return result


def main():
    args = parse_args()

    print(f"[INFO] 学習データ読み込み: {args.train_csv}")
    try:
        df = pd.read_csv(args.train_csv, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] ファイルが見つかりません: {args.train_csv}", file=sys.stderr)
        sys.exit(1)

    # is_place を数値に変換し、ラベルなし行を除外
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce")
    before = len(df)
    df = df.dropna(subset=[TARGET_COL])
    after = len(df)
    if before != after:
        print(f"[INFO] is_place 欠損行を除外: {before - after} 件 (残 {after} 件)")

    if df.empty:
        print("[ERROR] 有効な学習データがありません。", file=sys.stderr)
        sys.exit(1)

    # 使用する列のうち存在しない列を確認
    missing_cols = [c for c in FEATURE_COLS if c not in df.columns]
    if missing_cols:
        print(f"[WARN] 以下の特徴量列が CSV に存在しないため NaN で補完します: {missing_cols}")

    X = prepare_features(df)
    y = df[TARGET_COL].astype(int)

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    cat_cols_present = [c for c in CATEGORICAL_FEATURES if c in X.columns]

    model = LGBMClassifier(
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )

    print(f"[INFO] 学習開始 (train={len(X_train)}, val={len(X_val)})")
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        categorical_feature=cat_cols_present,
    )

    y_pred_proba = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, y_pred_proba)
    print(f"[INFO] Val AUC: {auc:.4f}")

    # モデルバンドル: モデル本体 + 特徴スキーマを保存
    bundle = {
        "model": model,
        "feature_cols": FEATURE_COLS,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
    }

    out_dir = os.path.dirname(args.model_out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    joblib.dump(bundle, args.model_out)
    print(f"[INFO] モデルを保存しました: {args.model_out}")


if __name__ == "__main__":
    main()
