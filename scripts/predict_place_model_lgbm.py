"""
predict_place_model_lgbm.py
===========================
train_place_model_lgbm.py で保存したモデルバンドルを読み込み、
CSV に pred_is_place_proba 列を付与して出力する。

使用例:
  python scripts/predict_place_model_lgbm.py \
      --in data/place_unlabeled.csv \
      --model models/place_lgbm.pkl \
      --out data/place_scored.csv
"""

import argparse
import os
import sys

import joblib
import pandas as pd


DEFAULT_IN_PATH = "data/place_unlabeled.csv"
DEFAULT_MODEL_PATH = "models/place_lgbm.pkl"
DEFAULT_OUT_PATH = "data/place_scored.csv"

PROBA_COL = "pred_is_place_proba"


def parse_args():
    parser = argparse.ArgumentParser(
        description="LightGBM モデルで CSV にスコアリングし pred_is_place_proba を付与する"
    )
    parser.add_argument(
        "--in",
        dest="in_csv",
        default=DEFAULT_IN_PATH,
        metavar="PATH",
        help=f"入力 CSV パス (デフォルト: {DEFAULT_IN_PATH})",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL_PATH,
        metavar="PATH",
        help=f"モデルバンドルパス (.pkl) (デフォルト: {DEFAULT_MODEL_PATH})",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_OUT_PATH,
        metavar="PATH",
        help=f"出力 CSV パス (デフォルト: {DEFAULT_OUT_PATH})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"[INFO] モデル読み込み: {args.model}")
    try:
        bundle = joblib.load(args.model)
    except FileNotFoundError:
        print(f"[ERROR] モデルファイルが見つかりません: {args.model}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] モデル読み込み失敗: {e}", file=sys.stderr)
        sys.exit(1)

    model = bundle["model"]
    feature_cols = bundle["feature_cols"]
    numeric_features = bundle["numeric_features"]
    categorical_features = bundle["categorical_features"]

    print(f"[INFO] 入力 CSV 読み込み: {args.in_csv}")
    try:
        df = pd.read_csv(args.in_csv, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] ファイルが見つかりません: {args.in_csv}", file=sys.stderr)
        sys.exit(1)

    if "entry_key" not in df.columns:
        print("[ERROR] 入力 CSV に 'entry_key' 列が必要です。", file=sys.stderr)
        sys.exit(1)

    # 数値列を変換
    for col in numeric_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = float("nan")

    # カテゴリ列を変換 (訓練時と同じカテゴリ型にアライン)
    for col in categorical_features:
        if col in df.columns:
            df[col] = df[col].astype("category")
        else:
            df[col] = pd.Categorical([None] * len(df))

    # 訓練時の特徴スキーマにアライン: 不足列は NaN/None 補完
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        print(f"[WARN] 以下の特徴量列が存在しないため NaN で補完します: {missing}")

    X = pd.DataFrame(index=df.index)
    for col in feature_cols:
        if col in df.columns:
            X[col] = df[col]
        elif col in numeric_features:
            X[col] = float("nan")
        else:
            X[col] = pd.Categorical([None] * len(df))

    proba = model.predict_proba(X)[:, 1]
    df[PROBA_COL] = proba

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    df.to_csv(args.out, index=False)
    print(f"[INFO] {len(df)} 件スコアリング完了 → {args.out}")


if __name__ == "__main__":
    main()
