"""
jv_build_latest_metrics.py
==========================
horse_latest_metrics テーブルを既存の entries テーブルから再構築する。

build_tables_from_raw.py の SE 処理では新規レコードを取り込みながら
horse_latest_metrics を逐次更新するが、このスクリプトは既存の entries
テーブルの全データを使ってテーブルを一から再構築する。

使用例:
  python scripts/jv_build_latest_metrics.py --db jv_data.db
"""

import argparse
import datetime
import sqlite3

DEFAULT_DB_PATH = "jv_data.db"


def rebuild_latest_metrics(db_path: str) -> None:
    conn = sqlite3.connect(db_path)

    # テーブルが存在しない場合は作成
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS horse_latest_metrics (
            horse_id             TEXT PRIMARY KEY,
            handicap_weight_x10  INTEGER,
            body_weight          INTEGER,
            race_key             TEXT,
            updated_at           TEXT NOT NULL
        )
        """
    )
    conn.commit()

    now = datetime.datetime.now().isoformat()

    # entries テーブルから最新 race_key のレコードを使って一括 upsert
    conn.execute(
        """
        INSERT INTO horse_latest_metrics
            (horse_id, handicap_weight_x10, body_weight, race_key, updated_at)
        SELECT
            horse_id,
            handicap_weight_x10,
            body_weight,
            race_key,
            ?
        FROM entries AS e1
        WHERE (handicap_weight_x10 IS NOT NULL OR body_weight IS NOT NULL)
          AND race_key = (
              SELECT MAX(race_key)
              FROM entries AS e2
              WHERE e2.horse_id = e1.horse_id
                AND (e2.handicap_weight_x10 IS NOT NULL OR e2.body_weight IS NOT NULL)
          )
        ON CONFLICT(horse_id) DO UPDATE SET
            handicap_weight_x10 = COALESCE(excluded.handicap_weight_x10, handicap_weight_x10),
            body_weight         = COALESCE(excluded.body_weight, body_weight),
            race_key            = excluded.race_key,
            updated_at          = excluded.updated_at
        """,
        (now,),
    )
    conn.commit()

    result = conn.execute("SELECT COUNT(*) FROM horse_latest_metrics").fetchone()
    total = result[0] if result else 0
    conn.close()

    print(f"[INFO] horse_latest_metrics 再構築完了: {total} 件")


def parse_args():
    parser = argparse.ArgumentParser(
        description="entries テーブルから horse_latest_metrics テーブルを再構築する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[INFO] DB: {args.db}")
    rebuild_latest_metrics(args.db)


if __name__ == "__main__":
    main()
