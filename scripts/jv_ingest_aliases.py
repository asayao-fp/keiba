"""
jv_ingest_aliases.py
====================
raw_jv_records テーブルの SE レコードをパースして、
jockey_aliases (騎手略称マスタ) および trainer_aliases (調教師略称マスタ) テーブルを構築する。

SE レコードには騎手略称 (全角4文字 / 8バイト) と調教師略称 (全角4文字 / 8バイト) が
含まれているため、これを利用して略称マスタを作成する。

使用例:
  python scripts/jv_ingest_aliases.py --db jv_data.db
"""

import argparse
import datetime
import sqlite3


DEFAULT_DB_PATH = "jv_data.db"


def _sb(b: bytes, pos: int, length: int) -> str:
    """cp932 バイト列上で 1始まり pos から length バイトを切り出して文字列へ変換する。"""
    return b[pos - 1 : pos - 1 + length].decode("cp932", errors="ignore")


def parse_se_for_aliases(payload: str):
    """
    SE レコードから騎手コード/略称・調教師コード/略称を抽出して dict を返す。
    SE レコードでない場合や長さが不足する場合は None を返す。

    SE レコード固定長レイアウト (JV-Data 仕様):
      pos  86,  5 byte : TrainerCode        (調教師コード)
      pos  91,  8 byte : TanshukuTrainerMei (調教師略称、全角4文字)
      pos 297,  5 byte : KisyuCode          (騎手コード)
      pos 307,  8 byte : TanshukuKisyuMei   (騎手略称、全角4文字)
    """
    b = payload.encode("cp932")
    if len(b) < 314 or payload[:2] != "SE":
        return None

    trainer_code       = _sb(b, 86, 5).strip()
    trainer_name_short = _sb(b, 91, 8).strip(" \u3000")
    jockey_code        = _sb(b, 297, 5).strip()
    jockey_name_short  = _sb(b, 307, 8).strip(" \u3000")

    if not trainer_code and not jockey_code:
        return None

    return {
        "trainer_code":       trainer_code,
        "trainer_name_short": trainer_name_short,
        "jockey_code":        jockey_code,
        "jockey_name_short":  jockey_name_short,
    }


def init_alias_tables(conn: sqlite3.Connection) -> None:
    """jockey_aliases / trainer_aliases テーブルが存在しない場合は作成する。"""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jockey_aliases (
            jockey_code       TEXT PRIMARY KEY,
            jockey_name_short TEXT NOT NULL,
            updated_at        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trainer_aliases (
            trainer_code       TEXT PRIMARY KEY,
            trainer_name_short TEXT NOT NULL,
            updated_at         TEXT NOT NULL
        );
        """
    )
    conn.commit()


def build_aliases(db_path: str) -> None:
    """raw_jv_records の SE レコードをパースして jockey_aliases / trainer_aliases テーブルを更新する。"""
    conn = sqlite3.connect(db_path)
    init_alias_tables(conn)

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
            rec = parse_se_for_aliases(payload)
        except Exception as e:
            print(f"[WARN] SE パースエラー: {e}")
            error_count += 1
            continue

        if rec is None:
            continue

        if rec["jockey_code"] and rec["jockey_name_short"]:
            conn.execute(
                """
                INSERT INTO jockey_aliases (jockey_code, jockey_name_short, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(jockey_code) DO UPDATE SET
                    jockey_name_short = excluded.jockey_name_short,
                    updated_at        = excluded.updated_at
                """,
                (rec["jockey_code"], rec["jockey_name_short"], now),
            )

        if rec["trainer_code"] and rec["trainer_name_short"]:
            conn.execute(
                """
                INSERT INTO trainer_aliases (trainer_code, trainer_name_short, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(trainer_code) DO UPDATE SET
                    trainer_name_short = excluded.trainer_name_short,
                    updated_at         = excluded.updated_at
                """,
                (rec["trainer_code"], rec["trainer_name_short"], now),
            )

        parsed_count += 1

        if parsed_count % 10_000 == 0:
            conn.commit()
            print(f"  ... {parsed_count} 件処理済み", end="\r")

    conn.commit()

    jockey_total  = conn.execute("SELECT COUNT(*) FROM jockey_aliases").fetchone()[0]
    trainer_total = conn.execute("SELECT COUNT(*) FROM trainer_aliases").fetchone()[0]
    conn.close()

    print(f"[INFO] SE パース: {parsed_count} 件 → jockey_aliases: {jockey_total} 件, trainer_aliases: {trainer_total} 件")
    if error_count:
        print(f"[WARN] パースエラー: {error_count} 件")


def parse_args():
    parser = argparse.ArgumentParser(
        description="raw_jv_records の SE レコードをパースして jockey_aliases / trainer_aliases (略称マスタ) テーブルを構築する"
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
    build_aliases(args.db)


if __name__ == "__main__":
    main()
