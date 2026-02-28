"""
build_sanrenpuku_training_data.py
=================================
SQLite DB から 3連複 (Sanrenpuku) 学習データ CSV を生成する。

3連複ラベル: 同一レース内の馬 i, j, k に対して、3頭全てが複勝圏 (is_place=1) なら target=1。

使用例:
  python scripts/build_sanrenpuku_training_data.py --db jv_data.db --out data/sanrenpuku_train.csv
  python scripts/build_sanrenpuku_training_data.py --db jv_data.db --out data/sanrenpuku_train.csv --from 20200101 --to 20231231
  python scripts/build_sanrenpuku_training_data.py --db jv_data.db --out data/sanrenpuku_train.csv --neg-sample-per-pos 10 --seed 0
"""

import argparse
import csv
import itertools
import os
import random
import sqlite3
import sys


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_OUT_PATH = "data/sanrenpuku_train.csv"
DEFAULT_NEG_SAMPLE_PER_POS = 20
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
    "horse_no_c",
    "body_weight_c",
    "handicap_weight_x10_c",
    "jockey_code_c",
    "trainer_code_c",
    "maxmin_body_weight",
    "maxmin_handicap",
    "is_sanrenpuku",
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


def build_triples(entries_by_race: dict, neg_sample_per_pos: int, rng: random.Random) -> list:
    """Enumerate all unordered triples per race and downsample negatives."""
    positives = []
    negatives = []

    for race_key, entries in entries_by_race.items():
        if len(entries) < 3:
            continue

        race_info = {
            "race_key": entries[0]["race_key"],
            "yyyymmdd": entries[0]["yyyymmdd"],
            "course_code": entries[0]["course_code"],
            "grade_code": entries[0]["grade_code"],
            "track_code": entries[0]["track_code"],
            "surface": entries[0]["surface"],
            "distance_m": entries[0]["distance_m"],
        }

        for a, b, c in itertools.combinations(entries, 3):
            bw_vals = []
            hc_vals = []
            for horse in (a, b, c):
                try:
                    if horse["body_weight"] is not None:
                        bw_vals.append(float(horse["body_weight"]))
                    if horse["handicap_weight_x10"] is not None:
                        hc_vals.append(float(horse["handicap_weight_x10"]))
                except (TypeError, ValueError):
                    pass

            maxmin_bw = (max(bw_vals) - min(bw_vals)) if len(bw_vals) == 3 else None
            maxmin_hc = (max(hc_vals) - min(hc_vals)) if len(hc_vals) == 3 else None

            label = 1 if (a["is_place"] == 1 and b["is_place"] == 1 and c["is_place"] == 1) else 0

            row = [
                race_info["race_key"],
                race_info["yyyymmdd"],
                race_info["course_code"],
                race_info["grade_code"],
                race_info["track_code"],
                race_info["surface"],
                race_info["distance_m"],
                a["horse_no"],
                a["body_weight"],
                a["handicap_weight_x10"],
                a["jockey_code"],
                a["trainer_code"],
                b["horse_no"],
                b["body_weight"],
                b["handicap_weight_x10"],
                b["jockey_code"],
                b["trainer_code"],
                c["horse_no"],
                c["body_weight"],
                c["handicap_weight_x10"],
                c["jockey_code"],
                c["trainer_code"],
                maxmin_bw,
                maxmin_hc,
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
        description="SQLite DB から3連複学習データ CSV を生成する"
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
    triples = build_triples(entries_by_race, args.neg_sample_per_pos, rng)

    rng.shuffle(triples)

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(triples)

    n_pos = sum(1 for t in triples if t[-1] == 1)
    n_neg = len(triples) - n_pos
    print(f"[INFO] {len(triples)} 件 (陽性 {n_pos}, 陰性 {n_neg}) → {args.out}")


if __name__ == "__main__":
    main()
