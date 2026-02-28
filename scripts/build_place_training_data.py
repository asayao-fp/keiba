"""
build_place_training_data.py
============================
SQLite DB から学習データ CSV を生成する。

使用例:
  python scripts/build_place_training_data.py --db jv_data.db --out data/place_train.csv
  python scripts/build_place_training_data.py --db jv_data.db --out data/place_train.csv --from 20200101 --to 20231231
"""

import argparse
import csv
import os
import sqlite3
import sys


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_OUT_PATH = "data/place_train.csv"

COLUMNS = [
    "race_key",
    "entry_key",
    "horse_id",
    "horse_no",
    "yyyymmdd",
    "course_code",
    "grade_code",
    "jockey_code",
    "trainer_code",
    "body_weight",
    "handicap_weight_x10",
    "distance_m",
    "track_code",
    "surface",
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
    "is_place",
]

PASSING_FEATURE_COLS = [
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
]


def fetch_training_rows(conn: sqlite3.Connection, date_from: str | None, date_to: str | None) -> list:
    params = []
    date_filter = ""
    if date_from:
        date_filter += " AND r.yyyymmdd >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND r.yyyymmdd <= ?"
        params.append(date_to)

    query_with_passing = f"""
        SELECT
            e.race_key,
            e.entry_key,
            e.horse_id,
            e.horse_no,
            r.yyyymmdd,
            r.course_code,
            r.grade_code,
            e.jockey_code,
            e.trainer_code,
            e.body_weight,
            e.handicap_weight_x10,
            r.distance_m,
            r.track_code,
            r.surface,
            p.avg_pos_1c_last3,
            p.avg_pos_4c_last3,
            p.avg_gain_last3,
            p.front_rate_last3,
            p.avg_pos_1c_pct_last3,
            p.avg_pos_4c_pct_last3,
            p.n_past,
            e.is_place
        FROM entries e
        JOIN races r ON r.race_key = e.race_key
        LEFT JOIN horse_past_passing_features p ON p.race_key = e.race_key AND p.horse_id = e.horse_id
        WHERE e.is_place IS NOT NULL
          AND e.body_weight IS NOT NULL
          AND e.handicap_weight_x10 IS NOT NULL
          {date_filter}
        ORDER BY r.yyyymmdd, e.race_key, CAST(e.horse_no AS INTEGER)
    """

    query_without_passing = f"""
        SELECT
            e.race_key,
            e.entry_key,
            e.horse_id,
            e.horse_no,
            r.yyyymmdd,
            r.course_code,
            r.grade_code,
            e.jockey_code,
            e.trainer_code,
            e.body_weight,
            e.handicap_weight_x10,
            r.distance_m,
            r.track_code,
            r.surface,
            NULL AS avg_pos_1c_last3,
            NULL AS avg_pos_4c_last3,
            NULL AS avg_gain_last3,
            NULL AS front_rate_last3,
            NULL AS avg_pos_1c_pct_last3,
            NULL AS avg_pos_4c_pct_last3,
            NULL AS n_past,
            e.is_place
        FROM entries e
        JOIN races r ON r.race_key = e.race_key
        WHERE e.is_place IS NOT NULL
          AND e.body_weight IS NOT NULL
          AND e.handicap_weight_x10 IS NOT NULL
          {date_filter}
        ORDER BY r.yyyymmdd, e.race_key, CAST(e.horse_no AS INTEGER)
    """

    try:
        cur = conn.execute(query_with_passing, params)
    except sqlite3.OperationalError as e:
        print(
            f"[WARN] horse_past_passing_features テーブルが利用できません ({e})。通過順特徴量なしで続行します。",
            file=sys.stderr,
        )
        cur = conn.execute(query_without_passing, params)
    return cur.fetchall()


def parse_args():
    parser = argparse.ArgumentParser(
        description="SQLite DB から is_place 学習データ CSV を生成する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_OUT_PATH,
        metavar="PATH",
        help=f"出力 CSV ファイルパス (デフォルト: {DEFAULT_OUT_PATH})",
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        default=None,
        metavar="YYYYMMDD",
        help="取得開始日 (例: 20200101)",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        default=None,
        metavar="YYYYMMDD",
        help="取得終了日 (例: 20231231)",
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
        rows = fetch_training_rows(conn, args.date_from, args.date_to)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    if not rows:
        print("[WARN] 出力対象の行が 0 件でした。", file=sys.stderr)

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(rows)

    print(f"[INFO] {len(rows)} 件 → {args.out}")


if __name__ == "__main__":
    main()
