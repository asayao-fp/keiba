"""
build_wide_training_data.py
===========================
SQLite DB からワイド (Wide) 学習データ CSV を生成する。

ワイドラベル: 同一レース内の馬 i, j に対して、両方が複勝圏 (is_place=1) なら target=1。

使用例:
  python scripts/build_wide_training_data.py --db jv_data.db --out data/wide_train.csv
  python scripts/build_wide_training_data.py --db jv_data.db --out data/wide_train.csv --from 20200101 --to 20231231
  python scripts/build_wide_training_data.py --db jv_data.db --out data/wide_train.csv --neg-sample-per-pos 5 --seed 0
"""

import argparse
import csv
import itertools
import os
import random
import sqlite3
import sys


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_OUT_PATH = "data/wide_train.csv"
DEFAULT_NEG_SAMPLE_PER_POS = 10
DEFAULT_SEED = 42

COLUMNS = [
    "race_key",
    "yyyymmdd",
    "course_code",
    "grade_code",
    "track_code",
    "surface",
    "distance_m",
    "horse_no_a",
    "body_weight_a",
    "handicap_weight_x10_a",
    "jockey_code_a",
    "trainer_code_a",
    "horse_no_b",
    "body_weight_b",
    "handicap_weight_x10_b",
    "jockey_code_b",
    "trainer_code_b",
    "abs_diff_body_weight",
    "abs_diff_handicap",
    "is_wide",
]


def fetch_races_entries(conn: sqlite3.Connection, date_from: str | None, date_to: str | None) -> list:
    params = []
    date_filter = ""
    if date_from:
        date_filter += " AND r.yyyymmdd >= ?"
        params.append(date_from)
    if date_to:
        date_filter += " AND r.yyyymmdd <= ?"
        params.append(date_to)

    query = f"""
        SELECT
            e.race_key,
            r.yyyymmdd,
            r.course_code,
            r.grade_code,
            r.track_code,
            r.surface,
            r.distance_m,
            e.horse_no,
            e.body_weight,
            e.handicap_weight_x10,
            e.jockey_code,
            e.trainer_code,
            e.is_place
        FROM entries e
        JOIN races r ON r.race_key = e.race_key
        WHERE e.is_place IS NOT NULL
          AND e.body_weight IS NOT NULL
          AND e.handicap_weight_x10 IS NOT NULL
          {date_filter}
        ORDER BY r.yyyymmdd, e.race_key, CAST(e.horse_no AS INTEGER)
    """
    cur = conn.execute(query, params)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def build_pairs(entries_by_race: dict, neg_sample_per_pos: int, rng: random.Random) -> list:
    """Enumerate all unordered pairs per race and downsample negatives."""
    positives = []
    negatives = []

    for race_key, entries in entries_by_race.items():
        race_info = {
            "race_key": entries[0]["race_key"],
            "yyyymmdd": entries[0]["yyyymmdd"],
            "course_code": entries[0]["course_code"],
            "grade_code": entries[0]["grade_code"],
            "track_code": entries[0]["track_code"],
            "surface": entries[0]["surface"],
            "distance_m": entries[0]["distance_m"],
        }

        for a, b in itertools.combinations(entries, 2):
            bw_a = a["body_weight"]
            bw_b = b["body_weight"]
            hc_a = a["handicap_weight_x10"]
            hc_b = b["handicap_weight_x10"]

            try:
                abs_diff_bw = abs(float(bw_a) - float(bw_b)) if bw_a is not None and bw_b is not None else None
                abs_diff_hc = abs(float(hc_a) - float(hc_b)) if hc_a is not None and hc_b is not None else None
            except (TypeError, ValueError):
                abs_diff_bw = None
                abs_diff_hc = None

            label = 1 if (a["is_place"] == 1 and b["is_place"] == 1) else 0

            row = [
                race_info["race_key"],
                race_info["yyyymmdd"],
                race_info["course_code"],
                race_info["grade_code"],
                race_info["track_code"],
                race_info["surface"],
                race_info["distance_m"],
                a["horse_no"],
                bw_a,
                hc_a,
                a["jockey_code"],
                a["trainer_code"],
                b["horse_no"],
                bw_b,
                hc_b,
                b["jockey_code"],
                b["trainer_code"],
                abs_diff_bw,
                abs_diff_hc,
                label,
            ]

            if label == 1:
                positives.append(row)
            else:
                negatives.append(row)

    n_neg = len(positives) * neg_sample_per_pos
    if len(negatives) > n_neg:
        negatives = rng.sample(negatives, n_neg)

    return positives + negatives


def group_by_race(rows: list) -> dict:
    result = {}
    for row in rows:
        key = row["race_key"]
        result.setdefault(key, []).append(row)
    return result


def parse_args():
    parser = argparse.ArgumentParser(
        description="SQLite DB からワイド学習データ CSV を生成する"
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
        "--neg-sample-per-pos",
        type=int,
        default=DEFAULT_NEG_SAMPLE_PER_POS,
        metavar="N",
        help=f"陽性 1 件につき陰性のダウンサンプル数 (デフォルト: {DEFAULT_NEG_SAMPLE_PER_POS})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        metavar="N",
        help=f"乱数シード (デフォルト: {DEFAULT_SEED})",
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
        rows = fetch_races_entries(conn, args.date_from, args.date_to)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    if not rows:
        print("[WARN] 出力対象の行が 0 件でした。", file=sys.stderr)

    entries_by_race = group_by_race(rows)
    rng = random.Random(args.seed)
    pairs = build_pairs(entries_by_race, args.neg_sample_per_pos, rng)

    rng.shuffle(pairs)

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(pairs)

    n_pos = sum(1 for p in pairs if p[-1] == 1)
    n_neg = len(pairs) - n_pos
    print(f"[INFO] {len(pairs)} 件 (陽性 {n_pos}, 陰性 {n_neg}) → {args.out}")


if __name__ == "__main__":
    main()
