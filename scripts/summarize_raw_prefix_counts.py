"""
summarize_raw_prefix_counts.py
==============================
raw_jv_records テーブルの payload_text 先頭 2 文字・3 文字のプレフィックス別
レコード件数を集計して表示する調査ツール。

使用例:
  python scripts/summarize_raw_prefix_counts.py --db jv_data.db
  python scripts/summarize_raw_prefix_counts.py --db jv_data.db --dataspec RACE --limit 20
"""

import argparse
import sqlite3

DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_DATASPEC = "RACE"
DEFAULT_LIMIT = 30


def summarize(db_path: str, dataspec: str, limit: int) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    total = conn.execute(
        "SELECT COUNT(*) FROM raw_jv_records WHERE dataspec = ?",
        (dataspec,),
    ).fetchone()[0]
    print(f"[INFO] dataspec={dataspec!r}  総レコード数: {total:,}")

    for prefix_len in (2, 3):
        print(f"\n[INFO] prefix_len={prefix_len} TOP {limit}")
        print(f"  {'PREFIX':<10}  {'COUNT':>10}")
        print(f"  {'-'*10}  {'-'*10}")
        rows = conn.execute(
            """
            SELECT SUBSTR(payload_text, 1, ?) AS prefix,
                   COUNT(*)                   AS cnt
            FROM raw_jv_records
            WHERE dataspec = ?
            GROUP BY prefix
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (prefix_len, dataspec, limit),
        ).fetchall()
        for row in rows:
            print(f"  {(row['prefix'] or '(empty)'):<10}  {row['cnt']:>10,}")

    conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="raw_jv_records の payload_text 先頭プレフィックス別件数を集計する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--dataspec",
        default=DEFAULT_DATASPEC,
        metavar="SPEC",
        help=f"集計対象の dataspec (デフォルト: {DEFAULT_DATASPEC})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"各 prefix_len で上位 N 件を表示 (デフォルト: {DEFAULT_LIMIT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(f"[INFO] DB: {args.db}")
    summarize(args.db, args.dataspec, args.limit)


if __name__ == "__main__":
    main()
