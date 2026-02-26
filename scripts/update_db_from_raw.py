"""
update_db_from_raw.py
=====================
raw_jv_records に取り込み済みの生データから派生テーブルを再構築する。

実行する手順:
  1. build_tables_from_raw.py  → races / entries テーブル
  2. build_masters_from_raw.py → jockeys / trainers テーブル
  3. build_place_odds_from_raw.py → place_odds テーブル

各ステップは冪等 (UPSERT / CREATE IF NOT EXISTS) なので何度でも安全に実行できる。

使用例:
  python scripts/update_db_from_raw.py --db jv_data.db
  python scripts/update_db_from_raw.py --db jv_data.db --skip-masters
  python scripts/update_db_from_raw.py --db jv_data.db --skip-place-odds
"""

import argparse
import os
import sys

# Ensure sibling scripts are importable when running from any working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from build_tables_from_raw import build_tables  # noqa: E402
from build_masters_from_raw import build_masters  # noqa: E402
from build_place_odds_from_raw import build_place_odds  # noqa: E402

DEFAULT_DB_PATH = "jv_data.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="raw_jv_records から派生テーブル (races/entries/masters/place_odds) を再構築する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--skip-masters",
        action="store_true",
        default=False,
        help="jockeys / trainers マスタテーブルの更新をスキップする",
    )
    parser.add_argument(
        "--skip-place-odds",
        action="store_true",
        default=False,
        help="place_odds テーブルの更新をスキップする",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"[INFO] DB: {args.db}")

    # Step 1: races / entries
    print("\n[STEP 1/3] races / entries テーブルを更新します ...")
    build_tables(args.db, graded_only=False)
    print("[STEP 1/3] 完了")

    # Step 2: masters (jockeys / trainers)
    if args.skip_masters:
        print("\n[STEP 2/3] --skip-masters が指定されたためスキップします")
    else:
        print("\n[STEP 2/3] jockeys / trainers マスタテーブルを更新します ...")
        build_masters(args.db)
        print("[STEP 2/3] 完了")

    # Step 3: place_odds
    if args.skip_place_odds:
        print("\n[STEP 3/3] --skip-place-odds が指定されたためスキップします")
    else:
        print("\n[STEP 3/3] place_odds テーブルを更新します ...")
        build_place_odds(args.db)
        print("[STEP 3/3] 完了")

    print("\n[INFO] すべての更新が完了しました")


if __name__ == "__main__":
    main()
