"""
show_race_entries.py
====================
指定レースの出走馬一覧を出力する。

使用例:
  python scripts/show_race_entries.py --db jv_data.db --race-key 202401010102010101
  python scripts/show_race_entries.py --db jv_data.db --race-key 202401010102010101 --format table
  python scripts/show_race_entries.py --db jv_data.db --race-key 202401010102010101 --format json
"""

import argparse
import sqlite3
import sys

from output_utils import print_rows


DEFAULT_DB_PATH = "jv_data.db"


def fetch_race_info(conn: sqlite3.Connection, race_key: str):
    cur = conn.execute(
        """
        SELECT race_key, yyyymmdd, course_code, race_no, race_name_short, grade_code
        FROM races
        WHERE race_key = ?
        """,
        (race_key,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    columns = [d[0] for d in cur.description]
    return dict(zip(columns, row))


def fetch_entries(conn: sqlite3.Connection, race_key: str):
    cur = conn.execute(
        """
        SELECT
            e.entry_key,
            e.race_key,
            e.horse_no,
            e.horse_id,
            e.finish_pos,
            e.is_place,
            e.jockey_code,
            j.jockey_name,
            e.trainer_code,
            t.trainer_name,
            e.body_weight,
            e.handicap_weight_x10
        FROM entries e
        LEFT JOIN jockeys  j ON j.jockey_code  = e.jockey_code
        LEFT JOIN trainers t ON t.trainer_code = e.trainer_code
        WHERE e.race_key = ?
        ORDER BY CAST(e.horse_no AS INTEGER) ASC
        """,
        (race_key,),
    )
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def parse_args():
    parser = argparse.ArgumentParser(
        description="指定レースの出走馬一覧を出力する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--race-key",
        required=True,
        metavar="RACE_KEY",
        help="レースキー (例: 202401010102010101)",
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
        race = fetch_race_info(conn, args.race_key)
        if race is None:
            print(f"[ERROR] レースが見つかりません: {args.race_key}", file=sys.stderr)
            sys.exit(1)

        rows = fetch_entries(conn, args.race_key)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    print_rows(rows, args.fmt)


if __name__ == "__main__":
    main()
