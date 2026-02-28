"""
train_place_model.py
====================
学習データ CSV を読み込んで CatBoostClassifier で is_place モデルを学習する。

使用例:
  python scripts/train_place_model.py --train-csv data/place_train.csv --model-out models/place_model.cbm
"""

import argparse
import os
import sys

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split


DEFAULT_TRAIN_CSV = "data/place_train.csv"
DEFAULT_MODEL_OUT = "models/place_model.cbm"

CATEGORICAL_FEATURES = ["jockey_code", "trainer_code", "course_code", "grade_code", "track_code", "surface"]
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
FEATURE_COLS = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COL = "is_place"

# 識別用列 (特徴量に含めない)
ID_COLS = ["race_key", "entry_key", "horse_id", "horse_no", "yyyymmdd"]

# 通過順特徴量は NULL を許容するため dropna 対象から除外
PASSING_FEATURE_COLS = [
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
]
REQUIRED_FEATURE_COLS = [c for c in FEATURE_COLS if c not in PASSING_FEATURE_COLS]


def parse_args():
    parser = argparse.ArgumentParser(
        description="is_place 予測モデルを CatBoost で学習する"
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
        help=f"モデル出力パス (デフォルト: {DEFAULT_MODEL_OUT})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"[INFO] 学習データ読み込み: {args.train_csv}")
    try:
        df = pd.read_csv(args.train_csv, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] ファイルが見つかりません: {args.train_csv}", file=sys.stderr)
        sys.exit(1)

    # 数値列を変換
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # is_place を整数に変換
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce")

    # 欠損を除外 (通過順特徴量は任意のため除外対象に含めない)
    required = REQUIRED_FEATURE_COLS + [TARGET_COL]
    before = len(df)
    df = df.dropna(subset=required)
    after = len(df)
    if before != after:
        print(f"[INFO] 欠損行を除外: {before - after} 件 (残 {after} 件)")

    if df.empty:
        print("[ERROR] 有効な学習データがありません。", file=sys.stderr)
        sys.exit(1)

    X = df[FEATURE_COLS].copy()
    y = df[TARGET_COL].astype(int)

    # カテゴリ列の欠損を空文字で埋める (CatBoost は空文字を許容する)
    for col in CATEGORICAL_FEATURES:
        X[col] = X[col].fillna("")

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    cat_feature_indices = [FEATURE_COLS.index(c) for c in CATEGORICAL_FEATURES]

    model = CatBoostClassifier(
        iterations=500,
        learning_rate=0.05,
        depth=6,
        eval_metric="AUC",
        cat_features=cat_feature_indices,
        verbose=100,
        random_seed=42,
    )

    print(f"[INFO] 学習開始 (train={len(X_train)}, val={len(X_val)})")
    model.fit(X_train, y_train, eval_set=(X_val, y_val), use_best_model=True)

    # 評価
    y_pred_proba = model.predict_proba(X_val)[:, 1]
    y_pred = model.predict(X_val)
    auc = roc_auc_score(y_val, y_pred_proba)
    acc = accuracy_score(y_val, y_pred)
    print(f"[INFO] Val AUC: {auc:.4f}  Accuracy: {acc:.4f}")

    out_dir = os.path.dirname(args.model_out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    model.save_model(args.model_out)
    print(f"[INFO] モデルを保存しました: {args.model_out}")


if __name__ == "__main__":
    main()
