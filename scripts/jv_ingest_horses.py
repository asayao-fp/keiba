"""
jv_ingest_horses.py
===================
raw_jv_records テーブルの SE レコードをパースして、horses (馬名マスタ) テーブルを構築する。

SE レコード (馬毎レース情報) には血統登録番号 (KettoNum / horse_id) と
馬名 (Bamei / horse_name) が含まれているため、これを利用して馬名マスタを作成する。

使用例:
  python scripts/jv_ingest_horses.py --db jv_data.db
"""

import argparse
import datetime
import sqlite3


DEFAULT_DB_PATH = "jv_data.db"


def _sb(b: bytes, pos: int, length: int) -> str:
    """cp932 バイト列上で 1始まり pos から length バイトを切り出して文字列へ変換する。"""
    return b[pos - 1 : pos - 1 + length].decode("cp932", errors="ignore")


def parse_se_for_horse(payload: str):
    """
    SE レコードから horse_id と horse_name を抽出して dict を返す。
    SE レコードでない場合や長さが不足する場合は None を返す。

    SE レコード固定長レイアウト (JV-Data 仕様):
      pos 31, 10 byte : KettoNum (血統登録番号 = horse_id)
      pos 41, 36 byte : Bamei    (馬名、全角18文字 = 36バイト)
    """
    b = payload.encode("cp932")
    if len(b) < 76 or payload[:2] != "SE":
        return None

    horse_id   = _sb(b, 31, 10).strip()
    horse_name = _sb(b, 41, 36).strip(" \u3000")

    if not horse_id or not horse_name:
        return None

    return {"horse_id": horse_id, "horse_name": horse_name}


def init_horses_table(conn: sqlite3.Connection) -> None:
    """horses テーブルが存在しない場合は作成する。"""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS horses (
            horse_id    TEXT    PRIMARY KEY,
            horse_name  TEXT    NOT NULL,
            updated_at  TEXT    NOT NULL
        )
        """
    )
    conn.commit()


def build_horses(db_path: str) -> None:
    """raw_jv_records の SE レコードをパースして horses テーブルを更新する。"""
    conn = sqlite3.connect(db_path)
    init_horses_table(conn)

    now = datetime.datetime.now().isoformat()

    rows = conn.execute(
        "SELECT payload_text FROM raw_jv_records WHERE SUBSTR(payload_text, 1, 2) = 'SE'"
    ).fetchall()

    parsed_count = 0
    error_count = 0

    for (payload,) in rows:
        if not payload:
            continue
        try:
            rec = parse_se_for_horse(payload)
        except Exception as e:
            print(f"[WARN] SE パースエラー: {e}")
            error_count += 1
            continue

        if rec is None:
            continue

        conn.execute(
            """
            INSERT INTO horses (horse_id, horse_name, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(horse_id) DO UPDATE SET
                horse_name = excluded.horse_name,
                updated_at = excluded.updated_at
            """,
            (rec["horse_id"], rec["horse_name"], now),
        )
        parsed_count += 1

        if parsed_count % 10_000 == 0:
            conn.commit()
            print(f"  ... {parsed_count} 件処理済み", end="\r")

    conn.commit()

    horse_total = conn.execute("SELECT COUNT(*) FROM horses").fetchone()[0]
    conn.close()

    print(f"[INFO] SE パース: {parsed_count} 件 → horses テーブル: {horse_total} 件")
    if error_count:
        print(f"[WARN] パースエラー: {error_count} 件")


def parse_args():
    parser = argparse.ArgumentParser(
        description="raw_jv_records の SE レコードをパースして horses (馬名マスタ) テーブルを構築する"
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
    build_horses(args.db)


if __name__ == "__main__":
    main()
