"""
predict_sanrenpuku.py
=====================
学習済みモデルを使って、指定 race_key の全トリプルに3連複確率 p_sanrenpuku を付与して出力する。

使用例:
  python scripts/predict_sanrenpuku.py --db jv_data.db --race-key 202401010102010101 --model models/sanrenpuku_model.cbm
  python scripts/predict_sanrenpuku.py --db jv_data.db --race-key 202401010102010101 --model models/sanrenpuku_model.cbm --topn 10
  python scripts/predict_sanrenpuku.py --db jv_data.db --race-key 202401010102010101 --model models/sanrenpuku_model.cbm --format json
"""

import argparse
import itertools
import sqlite3
import sys

import pandas as pd
from catboost import CatBoostClassifier

from output_utils import print_rows


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_MODEL_PATH = "models/sanrenpuku_model.cbm"
DEFAULT_TOPN = 10

CATEGORICAL_FEATURES = [
    "course_code", "grade_code", "track_code", "surface",
    "jockey_code_a", "trainer_code_a",
    "jockey_code_b", "trainer_code_b",
    "jockey_code_c", "trainer_code_c",
]
NUMERIC_FEATURES = [
    "distance_m",
    "body_weight_a", "handicap_weight_x10_a",
    "body_weight_b", "handicap_weight_x10_b",
    "body_weight_c", "handicap_weight_x10_c",
    "maxmin_body_weight", "maxmin_handicap",
]
FEATURE_COLS = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def fetch_entries_for_race(conn: sqlite3.Connection, race_key: str) -> list[dict]:
    cur = conn.execute(
        """
        SELECT
            e.entry_key,
            e.race_key,
            e.horse_no,
            e.horse_id,
            r.yyyymmdd,
            r.course_code,
            r.grade_code,
            e.jockey_code,
            e.trainer_code,
            e.body_weight,
            e.handicap_weight_x10,
            r.distance_m,
            r.track_code,
            r.surface
        FROM entries e
        JOIN races r ON r.race_key = e.race_key
        WHERE e.race_key = ?
        ORDER BY CAST(e.horse_no AS INTEGER)
        """,
        (race_key,),
    )
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def build_triple_features(entries: list[dict]) -> pd.DataFrame:
    """全トリプルの特徴量 DataFrame を生成する。"""
    rows = []
    for a, b, c in itertools.combinations(entries, 3):
        bw_vals = []
        hc_vals = []
        for horse in (a, b, c):
            try:
                if horse.get("body_weight") is not None:
                    bw_vals.append(float(horse["body_weight"]))
                if horse.get("handicap_weight_x10") is not None:
                    hc_vals.append(float(horse["handicap_weight_x10"]))
            except (TypeError, ValueError):
                pass

        maxmin_bw = (max(bw_vals) - min(bw_vals)) if len(bw_vals) == 3 else None
        maxmin_hc = (max(hc_vals) - min(hc_vals)) if len(hc_vals) == 3 else None

        row = {
            "horse_no_a": a["horse_no"],
            "horse_no_b": b["horse_no"],
            "horse_no_c": c["horse_no"],
            "distance_m": a.get("distance_m"),
            "body_weight_a": a.get("body_weight"),
            "handicap_weight_x10_a": a.get("handicap_weight_x10"),
            "jockey_code_a": a.get("jockey_code"),
            "trainer_code_a": a.get("trainer_code"),
            "body_weight_b": b.get("body_weight"),
            "handicap_weight_x10_b": b.get("handicap_weight_x10"),
            "jockey_code_b": b.get("jockey_code"),
            "trainer_code_b": b.get("trainer_code"),
            "body_weight_c": c.get("body_weight"),
            "handicap_weight_x10_c": c.get("handicap_weight_x10"),
            "jockey_code_c": c.get("jockey_code"),
            "trainer_code_c": c.get("trainer_code"),
            "maxmin_body_weight": maxmin_bw,
            "maxmin_handicap": maxmin_hc,
            "course_code": a.get("course_code"),
            "grade_code": a.get("grade_code"),
            "track_code": a.get("track_code"),
            "surface": a.get("surface"),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def parse_args():
    parser = argparse.ArgumentParser(
        description="学習済みモデルで指定レースの3連複確率 p_sanrenpuku を推論する"
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
        "--model",
        default=DEFAULT_MODEL_PATH,
        metavar="PATH",
        help=f"学習済みモデルパス (デフォルト: {DEFAULT_MODEL_PATH})",
    )
    parser.add_argument(
        "--topn",
        type=int,
        default=DEFAULT_TOPN,
        metavar="N",
        help=f"上位 N 件を出力 (デフォルト: {DEFAULT_TOPN})",
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
        entries = fetch_entries_for_race(conn, args.race_key)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    if not entries:
        print(f"[ERROR] 出走馬が見つかりません: {args.race_key}", file=sys.stderr)
        sys.exit(1)

    if len(entries) < 3:
        print(f"[ERROR] トリプルを生成するには最低 3 頭必要です: {args.race_key}", file=sys.stderr)
        sys.exit(1)

    try:
        model = CatBoostClassifier()
        model.load_model(args.model)
    except Exception as e:
        print(f"[ERROR] モデル読み込み失敗: {e}", file=sys.stderr)
        sys.exit(1)

    df = build_triple_features(entries)

    # 数値列を変換
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # カテゴリ列の欠損を空文字で埋める
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].fillna("").astype(str)

    X = df[FEATURE_COLS].copy()
    proba = model.predict_proba(X)[:, 1]

    results = []
    triple_list = list(itertools.combinations(entries, 3))
    for i, (a, b, c) in enumerate(triple_list):
        row = {
            "horse_no_a": a["horse_no"],
            "horse_no_b": b["horse_no"],
            "horse_no_c": c["horse_no"],
            "race_key": a["race_key"],
            "p_sanrenpuku": round(float(proba[i]), 4),
        }
        results.append(row)

    # p_sanrenpuku 降順でソート
    results.sort(key=lambda r: r["p_sanrenpuku"], reverse=True)

    print_rows(results[: args.topn], args.fmt)


if __name__ == "__main__":
    main()
