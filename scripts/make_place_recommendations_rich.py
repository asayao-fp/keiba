"""
make_place_recommendations_rich.py
===================================
スコア済み CSV と SQLite DB を結合して、レースごとの上位 N 頭の推薦 CSV を作成する。

使用例:
  python scripts/make_place_recommendations_rich.py \
      --scored-csv data/place_scored.csv \
      --db jv_data.db \
      --out data/place_recommendations.csv \
      --topn 3
"""

import argparse
import os
import sqlite3
import sys

import pandas as pd


DEFAULT_SCORED_CSV = "data/place_scored.csv"
DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_OUT_PATH = "data/place_recommendations.csv"
DEFAULT_TOPN = 3

PROBA_COL = "pred_is_place_proba"

# 出力列の優先順序
OUTPUT_COLS = [
    "race_date",
    "race_key",
    "course_code",
    "race_no",
    "distance_m",
    "surface",
    "grade_code",
    "race_name_short",
    "rank_in_race",
    "horse_no",
    "horse_id",
    "horse_name",
    "jockey_name_short",
    "trainer_name_short",
    "body_weight",
    "handicap_weight_x10",
    PROBA_COL,
]


def load_table_if_exists(
    conn: sqlite3.Connection, table: str, columns: list[str] | None = None
) -> pd.DataFrame | None:
    """テーブルが存在すれば DataFrame を返す。存在しなければ None。
    columns を指定すると SELECT で絞り込む。"""
    try:
        if columns:
            col_sql = ", ".join(columns)
            return pd.read_sql_query(f"SELECT {col_sql} FROM {table}", conn)
        return pd.read_sql_query(f"SELECT * FROM {table}", conn)
    except Exception:
        return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="スコア済み CSV と DB を結合してレースごと上位 N 頭を推薦 CSV として出力する"
    )
    parser.add_argument(
        "--scored-csv",
        default=DEFAULT_SCORED_CSV,
        metavar="PATH",
        help=f"スコア済み CSV パス (デフォルト: {DEFAULT_SCORED_CSV})",
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
        help=f"出力 CSV パス (デフォルト: {DEFAULT_OUT_PATH})",
    )
    parser.add_argument(
        "--topn",
        type=int,
        default=DEFAULT_TOPN,
        metavar="N",
        help=f"レースごとに上位何頭を出力するか (デフォルト: {DEFAULT_TOPN})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # --- スコア済み CSV 読み込み ---
    print(f"[INFO] スコア済み CSV 読み込み: {args.scored_csv}")
    try:
        df_scored = pd.read_csv(args.scored_csv, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] ファイルが見つかりません: {args.scored_csv}", file=sys.stderr)
        sys.exit(1)

    for required in ("entry_key", PROBA_COL):
        if required not in df_scored.columns:
            print(
                f"[ERROR] 列 '{required}' がスコア済み CSV に存在しません。"
                f" 列: {list(df_scored.columns)}",
                file=sys.stderr,
            )
            sys.exit(1)

    df_scored[PROBA_COL] = pd.to_numeric(df_scored[PROBA_COL], errors="coerce")

    # --- DB 読み込み ---
    print(f"[INFO] DB 接続: {args.db}")
    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        # entries テーブル (必須)
        try:
            df_entries = pd.read_sql_query("SELECT * FROM entries", conn)
        except Exception as e:
            print(f"[ERROR] entries テーブル読み込み失敗: {e}", file=sys.stderr)
            sys.exit(1)

        # races テーブル (必須)
        try:
            df_races = pd.read_sql_query("SELECT * FROM races", conn)
        except Exception as e:
            print(f"[ERROR] races テーブル読み込み失敗: {e}", file=sys.stderr)
            sys.exit(1)

        # オプショナルテーブル
        df_horses = load_table_if_exists(conn, "horses")
        df_jockey_aliases = load_table_if_exists(conn, "jockey_aliases")
        df_trainer_aliases = load_table_if_exists(conn, "trainer_aliases")
    finally:
        conn.close()

    if df_horses is None:
        print("[WARN] horses テーブルが存在しません。horse_name は空になります。")
    if df_jockey_aliases is None:
        print("[WARN] jockey_aliases テーブルが存在しません。jockey_name_short は空になります。")
    if df_trainer_aliases is None:
        print("[WARN] trainer_aliases テーブルが存在しません。trainer_name_short は空になります。")

    # --- races テーブルの列名を整理 ---
    # yyyymmdd → race_date として扱う
    if "yyyymmdd" in df_races.columns and "race_date" not in df_races.columns:
        df_races = df_races.rename(columns={"yyyymmdd": "race_date"})

    # race_no が races テーブルに存在しない場合は race_key から取り出す (9-10 桁目)
    if "race_no" not in df_races.columns and "race_key" in df_races.columns:
        df_races["race_no"] = df_races["race_key"].str[8:10]

    # --- JOIN ---
    df = df_scored.copy()

    # entries join: scored CSV に race_key が無い場合のみ実施
    if "race_key" not in df.columns:
        n_before = len(df)
        # entries から race_key と必要な列のみ取得 (scored に存在しない列のみ)
        entry_extra = [c for c in df_entries.columns if c not in df.columns or c == "entry_key"]
        df = df.merge(
            df_entries[entry_extra].drop_duplicates("entry_key"),
            on="entry_key",
            how="left",
        )
        n_no_match = df["race_key"].isna().sum() if "race_key" in df.columns else 0
        print(f"[INFO] entries JOIN: {n_before - n_no_match} 件マッチ / {n_no_match} 件未マッチ")

    # races join: scored CSV に存在しない列のみ追加
    race_extra_cols = ["race_key"] + [
        c for c in df_races.columns if c != "race_key" and c not in df.columns
    ]
    n_before = len(df)
    df = df.merge(
        df_races[race_extra_cols].drop_duplicates("race_key"),
        on="race_key",
        how="left",
    )
    n_no_race = 0
    if "race_date" in df.columns:
        n_no_race = df["race_date"].isna().sum()
    elif "yyyymmdd" in df.columns:
        n_no_race = df["yyyymmdd"].isna().sum()
    print(f"[INFO] races JOIN: {n_before - n_no_race} 件マッチ / {n_no_race} 件未マッチ")

    # race_date が無ければ yyyymmdd を race_date として使う
    if "race_date" not in df.columns and "yyyymmdd" in df.columns:
        df["race_date"] = df["yyyymmdd"]

    # horses join (left) - horse_name のみ取得
    if df_horses is not None and "horse_id" in df.columns and "horse_id" in df_horses.columns:
        horse_cols = ["horse_id"]
        if "horse_name" in df_horses.columns:
            horse_cols.append("horse_name")
        df = df.merge(df_horses[horse_cols].drop_duplicates("horse_id"), on="horse_id", how="left")
        n_no_horse = df["horse_name"].isna().sum() if "horse_name" in df.columns else 0
        print(f"[INFO] horses JOIN: {len(df) - n_no_horse} 件マッチ / {n_no_horse} 件未マッチ")
    else:
        df["horse_name"] = None

    # jockey_aliases join (left) - jockey_name_short のみ取得
    if (
        df_jockey_aliases is not None
        and "jockey_code" in df.columns
        and "jockey_code" in df_jockey_aliases.columns
    ):
        alias_cols = ["jockey_code"]
        if "jockey_name_short" in df_jockey_aliases.columns:
            alias_cols.append("jockey_name_short")
        elif "name_short" in df_jockey_aliases.columns:
            df_jockey_aliases = df_jockey_aliases.rename(columns={"name_short": "jockey_name_short"})
            alias_cols.append("jockey_name_short")
        df = df.merge(
            df_jockey_aliases[alias_cols].drop_duplicates("jockey_code"),
            on="jockey_code",
            how="left",
        )
    else:
        df["jockey_name_short"] = None

    # trainer_aliases join (left) - trainer_name_short のみ取得
    if (
        df_trainer_aliases is not None
        and "trainer_code" in df.columns
        and "trainer_code" in df_trainer_aliases.columns
    ):
        alias_cols = ["trainer_code"]
        if "trainer_name_short" in df_trainer_aliases.columns:
            alias_cols.append("trainer_name_short")
        elif "name_short" in df_trainer_aliases.columns:
            df_trainer_aliases = df_trainer_aliases.rename(columns={"name_short": "trainer_name_short"})
            alias_cols.append("trainer_name_short")
        df = df.merge(
            df_trainer_aliases[alias_cols].drop_duplicates("trainer_code"),
            on="trainer_code",
            how="left",
        )
    else:
        df["trainer_name_short"] = None

    # --- レースごとランキング & 上位 N 抽出 ---
    race_key_col = "race_key" if "race_key" in df.columns else None
    if race_key_col is None:
        print("[ERROR] race_key 列が見つかりません。entries/scored CSV に race_key が必要です。", file=sys.stderr)
        sys.exit(1)
    df = df.sort_values([race_key_col, PROBA_COL], ascending=[True, False])
    df["rank_in_race"] = df.groupby(race_key_col)[PROBA_COL].rank(
        method="first", ascending=False
    ).astype(int)

    df_top = df[df["rank_in_race"] <= args.topn].copy()

    # --- 出力列を整理 ---
    # 存在する列だけを選択 (順序は OUTPUT_COLS に従う)
    out_cols = [c for c in OUTPUT_COLS if c in df_top.columns]
    # OUTPUT_COLS にない列も末尾に追加 (entry_key は常に含める)
    extra_cols = [c for c in df_top.columns if c not in out_cols]
    if "entry_key" not in out_cols and "entry_key" in extra_cols:
        out_cols = ["entry_key"] + out_cols
        extra_cols = [c for c in extra_cols if c != "entry_key"]
    out_cols = out_cols + extra_cols

    df_out = df_top[out_cols].reset_index(drop=True)

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    df_out.to_csv(args.out, index=False)
    n_races = df_out["race_key"].nunique() if "race_key" in df_out.columns else "?"
    print(f"[INFO] {len(df_out)} 行 ({n_races} レース, 上位 {args.topn} 頭) → {args.out}")


if __name__ == "__main__":
    main()
