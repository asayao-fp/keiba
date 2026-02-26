"""
list_races.py
=============
races テーブルからレースを検索して出力する。

使用例:
  python scripts/list_races.py --db jv_data.db --days 30
  python scripts/list_races.py --db jv_data.db --days 30 --grade-code C --grade-code D
  python scripts/list_races.py --db jv_data.db --from 20240101 --to 20241231
  python scripts/list_races.py --db jv_data.db --days 30 --grade-code C --format keys
  python scripts/list_races.py --db jv_data.db --days 30 --format csv
  python scripts/list_races.py --db jv_data.db --days 30 --format json
  python scripts/list_races.py --db jv_data.db --days 30 --require-place-odds --format keys
"""

import argparse
import csv
import datetime
import json
import sqlite3
import sys

DEFAULT_DB_PATH = "jv_data.db"

OUTPUT_FIELDS = [
    "race_key",
    "yyyymmdd",
    "course_code",
    "kai",
    "day",
    "race_no",
    "grade_code",
    "race_name_short",
    "distance_m",
    "track_code",
]


def fetch_races(
    conn: sqlite3.Connection,
    from_date: str,
    to_date: str | None,
    grade_codes: list[str] | None,
    name_contains: str | None,
    course_codes: list[str] | None,
    require_place_odds: bool = False,
) -> list[dict]:
    query = """
        SELECT
            race_key,
            yyyymmdd,
            course_code,
            kai,
            day,
            race_no,
            grade_code,
            race_name_short,
            distance_m,
            track_code
        FROM races
        WHERE yyyymmdd >= ?
    """
    params: list = [from_date]

    if to_date is not None:
        query += "  AND yyyymmdd <= ?\n"
        params.append(to_date)

    if grade_codes:
        placeholders = ", ".join("?" for _ in grade_codes)
        query += f"  AND TRIM(grade_code) IN ({placeholders})\n"
        params.extend(grade_codes)

    if name_contains is not None:
        query += "  AND race_name_short LIKE ?\n"
        params.append(f"%{name_contains}%")

    if course_codes:
        placeholders = ", ".join("?" for _ in course_codes)
        query += f"  AND course_code IN ({placeholders})\n"
        params.extend(course_codes)

    if require_place_odds:
        query += "  AND EXISTS (SELECT 1 FROM place_odds WHERE place_odds.race_key = races.race_key)\n"

    query += (
        "ORDER BY yyyymmdd DESC, course_code ASC, kai ASC, day ASC, race_no ASC"
    )

    cur = conn.execute(query, params)
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def output_keys(rows: list[dict]) -> None:
    for row in rows:
        print(row["race_key"])


def output_csv(rows: list[dict]) -> None:
    writer = csv.DictWriter(
        sys.stdout, fieldnames=OUTPUT_FIELDS, lineterminator="\n", extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)


def output_json(rows: list[dict]) -> None:
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="races テーブルからレース一覧を出力する"
    )

    # --- DB ---
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )

    # --- 日付フィルタ (--days OR --from / --to) ---
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="今日から遡る日数 (例: 30 → 直近30日)",
    )
    date_group.add_argument(
        "--from",
        dest="from_date",
        metavar="YYYYMMDD",
        help="検索開始日",
    )

    parser.add_argument(
        "--to",
        dest="to_date",
        default=None,
        metavar="YYYYMMDD",
        help="検索終了日 (--from と組み合わせて使用; 省略時: 制限なし)",
    )

    # --- 任意フィルタ ---
    parser.add_argument(
        "--grade-code",
        "-g",
        dest="grade_codes",
        action="append",
        metavar="CODE",
        help="グレードコードで絞り込む (複数指定可。例: -g C -g D)",
    )
    parser.add_argument(
        "--name-contains",
        metavar="TEXT",
        help="race_name_short の部分一致フィルタ",
    )
    parser.add_argument(
        "--course-code",
        dest="course_codes",
        action="append",
        metavar="CODE",
        help="競馬場コードで絞り込む (複数指定可)",
    )

    # --- オッズフィルタ ---
    parser.add_argument(
        "--require-place-odds",
        action="store_true",
        default=False,
        help="place_odds テーブルに対応レコードが存在するレースのみ出力する",
    )

    # --- 出力フォーマット ---
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["keys", "csv", "json"],
        default="keys",
        help="出力フォーマット: keys=race_keyを1行ずつ / csv / json (デフォルト: keys)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    today = datetime.date.today()

    if args.days is not None:
        from_date = (today - datetime.timedelta(days=args.days)).strftime("%Y%m%d")
        to_date = args.to_date  # typically None when --days is used
    elif args.from_date is not None:
        from_date = args.from_date
        to_date = args.to_date
    else:
        # デフォルト: 直近30日
        from_date = (today - datetime.timedelta(days=30)).strftime("%Y%m%d")
        to_date = None

    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = fetch_races(
            conn,
            from_date=from_date,
            to_date=to_date,
            grade_codes=args.grade_codes,
            name_contains=args.name_contains,
            course_codes=args.course_codes,
            require_place_odds=args.require_place_odds,
        )
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    if args.fmt == "keys":
        output_keys(rows)
    elif args.fmt == "csv":
        output_csv(rows)
    else:
        output_json(rows)


if __name__ == "__main__":
    main()
