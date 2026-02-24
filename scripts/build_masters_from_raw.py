"""
build_masters_from_raw.py
=========================
raw_jv_records テーブルからマスタレコードをパースして、
正規化マスタテーブル (jockeys / trainers) を生成する。

使用例:
  python scripts/build_masters_from_raw.py --db jv_data.db
"""

import argparse
import datetime
import sqlite3


DEFAULT_DB_PATH = "jv_data.db"


def _s(text: str, pos: int, length: int) -> str:
    """1始まり pos から length 文字を切り出す (0始まりに変換)。"""
    return text[pos - 1 : pos - 1 + length]


def parse_ks(payload: str):
    """
    KS レコード (騎手マスタ) をパースして dict を返す。
    レコード種別識別子が 'KS' でない場合は None を返す。
    """
    if len(payload) < 75 or payload[:2] != "KS":
        return None

    jockey_code = _s(payload, 12, 5)
    jockey_name = _s(payload, 42, 34).strip()

    return {
        "jockey_code": jockey_code,
        "jockey_name": jockey_name,
    }


def init_master_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jockeys (
            jockey_code TEXT PRIMARY KEY,
            jockey_name TEXT,
            updated_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trainers (
            trainer_code TEXT PRIMARY KEY,
            trainer_name TEXT,
            updated_at   TEXT NOT NULL
        );
        """
    )
    conn.commit()


def build_masters(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    init_master_tables(conn)

    now = datetime.datetime.now().isoformat()

    rows = conn.execute(
        "SELECT payload_text FROM raw_jv_records WHERE SUBSTR(payload_text, 1, 2) = 'KS'"
    ).fetchall()

    ks_count = 0

    for (payload,) in rows:
        if not payload:
            continue
        rec = parse_ks(payload)
        if rec is None:
            continue
        conn.execute(
            """
            INSERT INTO jockeys (jockey_code, jockey_name, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(jockey_code) DO UPDATE SET
                jockey_name = excluded.jockey_name,
                updated_at  = excluded.updated_at
            """,
            (rec["jockey_code"], rec["jockey_name"], now),
        )
        ks_count += 1

    conn.commit()

    jockey_total  = conn.execute("SELECT COUNT(*) FROM jockeys").fetchone()[0]
    trainer_total = conn.execute("SELECT COUNT(*) FROM trainers").fetchone()[0]

    conn.close()

    print(f"[INFO] KS パース: {ks_count} 件 → jockeys テーブル: {jockey_total} 件")
    if trainer_total == 0:
        print("[INFO] trainers テーブルは空です (調教師マスタ (KY等) のレコードが raw_jv_records に見つかりません)")


def parse_args():
    parser = argparse.ArgumentParser(
        description="raw_jv_records からマスタレコードをパースして正規化マスタテーブルを生成する"
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
    build_masters(args.db)


if __name__ == "__main__":
    main()
