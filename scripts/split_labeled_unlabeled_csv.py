"""
split_labeled_unlabeled_csv.py
==============================
combined CSV (is_place が空の行を含む可能性あり) を
ラベル済み CSV と未ラベル CSV に分割する。

使用例:
  python scripts/split_labeled_unlabeled_csv.py \
      --in data/place_combined.csv \
      --labeled data/place_labeled.csv \
      --unlabeled data/place_unlabeled.csv
"""

import argparse
import os
import sys

import pandas as pd


DEFAULT_IN_PATH = "data/place_combined.csv"
DEFAULT_LABELED_PATH = "data/place_labeled.csv"
DEFAULT_UNLABELED_PATH = "data/place_unlabeled.csv"
TARGET_COL = "is_place"


def parse_args():
    parser = argparse.ArgumentParser(
        description="combined CSV を labeled / unlabeled に分割する"
    )
    parser.add_argument(
        "--in",
        dest="in_csv",
        default=DEFAULT_IN_PATH,
        metavar="PATH",
        help=f"入力 CSV パス (デフォルト: {DEFAULT_IN_PATH})",
    )
    parser.add_argument(
        "--labeled",
        default=DEFAULT_LABELED_PATH,
        metavar="PATH",
        help=f"ラベル済み出力 CSV パス (デフォルト: {DEFAULT_LABELED_PATH})",
    )
    parser.add_argument(
        "--unlabeled",
        default=DEFAULT_UNLABELED_PATH,
        metavar="PATH",
        help=f"未ラベル出力 CSV パス (デフォルト: {DEFAULT_UNLABELED_PATH})",
    )
    return parser.parse_args()


def make_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def main():
    args = parse_args()

    print(f"[INFO] 読み込み: {args.in_csv}")
    try:
        df = pd.read_csv(args.in_csv, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] ファイルが見つかりません: {args.in_csv}", file=sys.stderr)
        sys.exit(1)

    if TARGET_COL not in df.columns:
        print(
            f"[ERROR] 列 '{TARGET_COL}' が CSV に存在しません。列: {list(df.columns)}",
            file=sys.stderr,
        )
        sys.exit(1)

    # 空文字・"nan" は欠損扱い
    mask_labeled = df[TARGET_COL].notna() & (df[TARGET_COL].str.strip() != "")
    df_labeled = df[mask_labeled].copy()
    df_unlabeled = df[~mask_labeled].copy()

    make_parent(args.labeled)
    make_parent(args.unlabeled)

    df_labeled.to_csv(args.labeled, index=False)
    df_unlabeled.to_csv(args.unlabeled, index=False)

    total = len(df)
    n_labeled = len(df_labeled)
    n_unlabeled = len(df_unlabeled)
    print(f"[INFO] 合計: {total} 件")
    print(f"[INFO]   ラベル済み: {n_labeled} 件 → {args.labeled}")
    print(f"[INFO]   未ラベル:   {n_unlabeled} 件 → {args.unlabeled}")


if __name__ == "__main__":
    main()
