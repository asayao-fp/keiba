"""
list_graded_races.py
====================
races テーブルから grade_code が非NULL/非空の重賞レースを検索して出力する。

使用例:
  python scripts/list_graded_races.py --db jv_data.db
  python scripts/list_graded_races.py --db jv_data.db --from 20240101
  python scripts/list_graded_races.py --db jv_data.db --from 20240101 --to 20241231
  python scripts/list_graded_races.py --db jv_data.db --from 20240101 --format table
"""

import argparse
import datetime
import sqlite3
import sys
from typing import Optional

from output_utils import print_rows


DEFAULT_DB_PATH = "jv_data.db"


def fetch_graded_races(conn: sqlite3.Connection, from_date: str, to_date: Optional[str]):
    query = """
        SELECT
            race_key,
            yyyymmdd,
            course_code,
            race_no,
            race_name_short,
            grade_code
        FROM races
        WHERE TRIM(grade_code) != ''
          AND yyyymmdd >= ?
    """
    params: list = [from_date]
    if to_date is not None:
        query += "  AND yyyymmdd <= ?\n"
        params.append(to_date)
    query += "ORDER BY yyyymmdd ASC, course_code ASC, race_no ASC"

    cur = conn.execute(query, params)
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def parse_args():
    today = datetime.date.today().strftime("%Y%m%d")
    parser = argparse.ArgumentParser(
        description="重賞レース一覧を出力する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default=today,
        metavar="YYYYMMDD",
        help="検索開始日 (デフォルト: 今日)",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default=None,
        metavar="YYYYMMDD",
        help="検索終了日 (省略時: 制限なし)",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["jsonl", "json", "table"],
        default="jsonl",
        help="出力フォーマット (デフォルト: jsonl)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = fetch_graded_races(conn, args.from_date, args.to_date)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    print_rows(rows, args.fmt)


if __name__ == "__main__":
    main()

