"""
predict_place.py
================
学習済みモデルを使って、指定 race_key の出走馬に複勝圏確率 p_place を付与して出力する。

使用例:
  python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm
  python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm --format table
  python scripts/predict_place.py --db jv_data.db --race-key 202401010102010101 --model models/place_model.cbm --format json
"""

import argparse
import sqlite3
import sys

import pandas as pd
from catboost import CatBoostClassifier

from output_utils import print_rows


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_MODEL_PATH = "models/place_model.cbm"

CATEGORICAL_FEATURES = ["jockey_code", "trainer_code", "course_code", "grade_code", "track_code"]
NUMERIC_FEATURES = ["body_weight", "handicap_weight_x10", "distance_m"]
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
            r.track_code
        FROM entries e
        JOIN races r ON r.race_key = e.race_key
        WHERE e.race_key = ?
        ORDER BY CAST(e.horse_no AS INTEGER)
        """,
        (race_key,),
    )
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def parse_args():
    parser = argparse.ArgumentParser(
        description="学習済みモデルで指定レースの複勝圏確率 p_place を推論する"
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

    try:
        model = CatBoostClassifier()
        model.load_model(args.model)
    except Exception as e:
        print(f"[ERROR] モデル読み込み失敗: {e}", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(entries)

    # 数値列を変換
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # カテゴリ列の欠損を空文字で埋める
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].fillna("").astype(str)

    X = df[FEATURE_COLS].copy()

    proba = model.predict_proba(X)[:, 1]

    results = []
    for i, entry in enumerate(entries):
        row = {
            "horse_no": entry["horse_no"],
            "horse_id": entry["horse_id"],
            "entry_key": entry["entry_key"],
            "race_key": entry["race_key"],
            "p_place": round(float(proba[i]), 4),
        }
        results.append(row)

    # p_place 降順でソート
    results.sort(key=lambda r: r["p_place"], reverse=True)

    print_rows(results, args.fmt)


if __name__ == "__main__":
    main()
