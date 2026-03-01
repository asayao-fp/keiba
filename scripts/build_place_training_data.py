"""
build_place_training_data.py
============================
SQLite DB から学習データ CSV を生成する。

使用例:
  python scripts/build_place_training_data.py --db jv_data.db --out data/place_train.csv
  python scripts/build_place_training_data.py --db jv_data.db --out data/place_train.csv --from 20200101 --to 20231231
  python scripts/build_place_training_data.py --db jv_data.db --out data/place_infer.csv --include-unlabeled
"""

import argparse
import csv
import math
import os
import sqlite3
import sys
from collections import defaultdict


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
    "body_weight_diff_mean",
    "handicap_weight_x10_diff_mean",
    "body_weight_z",
    "handicap_weight_x10_z",
    "is_place",
]

IS_PLACE_IDX = COLUMNS.index("is_place")


PASSING_FEATURE_COLS = [
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
]


def fetch_training_rows(
    conn: sqlite3.Connection,
    date_from: str | None,
    date_to: str | None,
    include_unlabeled: bool = False,
) -> list:
    params = []
    date_filter = ""
    if date_from:
        date_filter += " AND r.yyyymmdd >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND r.yyyymmdd <= ?"
        params.append(date_to)

    # When include_unlabeled is True, omit the filter so NULL rows are included.
    is_place_filter = "" if include_unlabeled else "AND e.is_place IS NOT NULL"

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
        WHERE e.body_weight IS NOT NULL
          AND e.handicap_weight_x10 IS NOT NULL
          {is_place_filter}
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
        WHERE e.body_weight IS NOT NULL
          AND e.handicap_weight_x10 IS NOT NULL
          {is_place_filter}
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


def add_race_relative_features(rows: list) -> list:
    """各 race_key グループ内で体重・斤量の平均差・z スコアを計算して付加する。

    追加列 (この順に各行末尾へ追記):
      body_weight_diff_mean         - body_weight - group mean
      handicap_weight_x10_diff_mean - handicap_weight_x10 - group mean
      body_weight_z                 - (body_weight - mean) / std; std==0 のときは 0.0
      handicap_weight_x10_z         - (handicap_weight_x10 - mean) / std; std==0 のときは 0.0

    グループ内で対象列が全 None の場合は派生列も None のままにする。
    """
    race_key_idx = COLUMNS.index("race_key")
    bw_idx = COLUMNS.index("body_weight")
    hw_idx = COLUMNS.index("handicap_weight_x10")

    # グループ化 (インデックスのみ保持してメモリ効率を上げる)
    group_indices: dict[str, list[int]] = defaultdict(list)
    rows = [list(r) for r in rows]
    for i, row in enumerate(rows):
        group_indices[row[race_key_idx]].append(i)

    for indices in group_indices.values():
        bw_vals = [rows[i][bw_idx] for i in indices if rows[i][bw_idx] is not None]
        hw_vals = [rows[i][hw_idx] for i in indices if rows[i][hw_idx] is not None]

        bw_mean = sum(bw_vals) / len(bw_vals) if bw_vals else None
        hw_mean = sum(hw_vals) / len(hw_vals) if hw_vals else None

        # 母集団標準偏差を使用: レース内全頭が母集団そのものであるため N 除算が適切
        bw_std: float | None = None
        if len(bw_vals) > 1 and bw_mean is not None:
            bw_std = math.sqrt(sum((v - bw_mean) ** 2 for v in bw_vals) / len(bw_vals))

        hw_std: float | None = None
        if len(hw_vals) > 1 and hw_mean is not None:
            hw_std = math.sqrt(sum((v - hw_mean) ** 2 for v in hw_vals) / len(hw_vals))

        for i in indices:
            bw = rows[i][bw_idx]
            hw = rows[i][hw_idx]

            bw_diff = (bw - bw_mean) if (bw is not None and bw_mean is not None) else None
            hw_diff = (hw - hw_mean) if (hw is not None and hw_mean is not None) else None

            if bw is not None and bw_mean is not None and bw_std is not None:
                bw_z: float | None = 0.0 if bw_std == 0.0 else (bw - bw_mean) / bw_std
            else:
                bw_z = None

            if hw is not None and hw_mean is not None and hw_std is not None:
                hw_z: float | None = 0.0 if hw_std == 0.0 else (hw - hw_mean) / hw_std
            else:
                hw_z = None

            # SQL クエリは常に is_place を末尾に返す前提で、新列をその直前に挿入する
            rows[i] = rows[i][:-1] + [bw_diff, hw_diff, bw_z, hw_z] + [rows[i][-1]]

    return rows


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
    parser.add_argument(
        "--include-unlabeled",
        dest="include_unlabeled",
        action="store_true",
        default=False,
        help="is_place が NULL の未ラベル行も出力に含める (デフォルト: False)",
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
        rows = fetch_training_rows(conn, args.date_from, args.date_to, args.include_unlabeled)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    if not rows:
        print("[WARN] 出力対象の行が 0 件でした。", file=sys.stderr)

    rows = add_race_relative_features(rows)

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(rows)

    print(f"[INFO] {len(rows)} 件 → {args.out}")
    if args.include_unlabeled:
        labeled = sum(1 for r in rows if r[IS_PLACE_IDX] is not None)
        unlabeled = len(rows) - labeled
        print(f"[INFO]   ラベル済み: {labeled} 件 / 未ラベル: {unlabeled} 件")


if __name__ == "__main__":
    main()
