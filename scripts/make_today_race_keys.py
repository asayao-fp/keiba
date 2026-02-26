"""
make_today_race_keys.py
=======================
今日の重賞レースの race_key を生成して出力する。

使用例:
  python scripts/make_today_race_keys.py --db jv_data.db
  python scripts/make_today_race_keys.py --db jv_data.db --date 20240101
  python scripts/make_today_race_keys.py --db jv_data.db --grade-codes A B C
  python scripts/make_today_race_keys.py --db jv_data.db --require-place-odds
  python scripts/make_today_race_keys.py --db jv_data.db --out race_keys.txt
"""

import argparse
import datetime
import os
import sqlite3
import sys

# Ensure sibling scripts are importable when running from any working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from list_races import fetch_races  # noqa: E402

DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_GRADE_CODES = ["A", "B", "C"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="今日の重賞レース race_key を生成して出力する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--date",
        dest="date",
        default=None,
        metavar="YYYYMMDD",
        help="対象日 (デフォルト: 今日のローカル日付)",
    )
    parser.add_argument(
        "--grade-codes",
        dest="grade_codes",
        nargs="+",
        default=DEFAULT_GRADE_CODES,
        metavar="CODE",
        help=f"グレードコードで絞り込む (デフォルト: {' '.join(DEFAULT_GRADE_CODES)})",
    )
    parser.add_argument(
        "--require-place-odds",
        action="store_true",
        default=False,
        help="place_odds テーブルに place_odds_min と place_odds_max が NULL でないレコードが存在するレースのみ出力する",
    )
    parser.add_argument(
        "--out",
        dest="out",
        default=None,
        metavar="FILE",
        help="出力ファイルパス (省略時: 標準出力)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.date is not None:
        target_date = args.date
    else:
        target_date = datetime.date.today().strftime("%Y%m%d")

    print(f"[INFO] DB: {args.db}", file=sys.stderr)
    print(f"[INFO] 対象日: {target_date}", file=sys.stderr)
    print(f"[INFO] グレードコード: {args.grade_codes}", file=sys.stderr)
    print(
        f"[INFO] 複勝オッズ必須: {args.require_place_odds}",
        file=sys.stderr,
    )

    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = fetch_races(
            conn,
            from_date=target_date,
            to_date=target_date,
            grade_codes=args.grade_codes,
            name_contains=None,
            course_codes=None,
            require_place_odds=args.require_place_odds,
        )
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    # Stable ordering: yyyymmdd ASC, course_code ASC, race_no ASC; deduplicate
    seen: set[str] = set()
    ordered: list[str] = []
    for row in sorted(rows, key=lambda r: (r["yyyymmdd"], r["course_code"], r["race_no"])):
        key = row["race_key"]
        if key not in seen:
            seen.add(key)
            ordered.append(key)

    print(f"[INFO] 該当レース数: {len(ordered)}", file=sys.stderr)

    output = "\n".join(ordered) + ("\n" if ordered else "")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"[INFO] 出力ファイル: {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(output)


if __name__ == "__main__":
    main()
