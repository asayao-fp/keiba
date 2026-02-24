"""
build_tables_from_raw.py
========================
raw_jv_records テーブルの RACE DataSpec レコードを固定長パースして、
正規化テーブル (races / entries) を生成する。

使用例:
  python scripts/build_tables_from_raw.py --db jv_data.db
  python scripts/build_tables_from_raw.py --db jv_data.db --graded-only
"""

import argparse
import datetime
import sqlite3


DEFAULT_DB_PATH = "jv_data.db"


def _s(text: str, pos: int, length: int) -> str:
    """1始まり pos から length 文字を切り出す (0始まりに変換)。"""
    return text[pos - 1 : pos - 1 + length]


def parse_ra(payload: str):
    """
    RA レコード (レース詳細, 1272 byte) をパースして dict を返す。
    レコード種別識別子が 'RA' でない場合は None を返す。
    """
    if len(payload) < 615 or payload[:2] != "RA":
        return None

    yyyy        = _s(payload, 12, 4)
    mmdd        = _s(payload, 16, 4)
    course      = _s(payload, 20, 2)
    kai         = _s(payload, 22, 2)
    day         = _s(payload, 24, 2)
    raceno      = _s(payload, 26, 2)
    race_name_short = _s(payload, 605, 6)
    grade_code  = _s(payload, 615, 1)

    race_key = f"{yyyy}{mmdd}{course}{kai}{day}{raceno}"
    yyyymmdd = f"{yyyy}{mmdd}"

    return {
        "race_key":        race_key,
        "yyyymmdd":        yyyymmdd,
        "course_code":     course,
        "kai":             kai,
        "day":             day,
        "race_no":         raceno,
        "grade_code":      grade_code,
        "race_name_short": race_name_short,
    }


def parse_se(payload: str):
    """
    SE レコード (馬毎レース情報, 555 byte) をパースして dict を返す。
    レコード種別識別子が 'SE' でない場合は None を返す。
    """
    if len(payload) < 336 or payload[:2] != "SE":
        return None

    yyyy   = _s(payload, 12, 4)
    mmdd   = _s(payload, 16, 4)
    course = _s(payload, 20, 2)
    kai    = _s(payload, 22, 2)
    day    = _s(payload, 24, 2)
    raceno = _s(payload, 26, 2)
    horse_no  = _s(payload, 29, 2)
    horse_id  = _s(payload, 31, 10)
    trainer_code    = _s(payload, 86, 5)
    jockey_code     = _s(payload, 297, 5)
    body_weight_raw = _s(payload, 325, 3).strip()
    finish_raw      = _s(payload, 335, 2).strip()

    race_key  = f"{yyyy}{mmdd}{course}{kai}{day}{raceno}"
    entry_key = f"{race_key}{horse_no}"

    # 確定着順: 数値変換できない/0 の場合は NULL
    finish_pos = None
    is_place   = None
    if finish_raw and finish_raw.isdigit():
        fp = int(finish_raw)
        if fp > 0:
            finish_pos = fp
            is_place   = 1 if fp <= 3 else 0

    # 馬体重: 数値変換できない場合は NULL
    body_weight = None
    if body_weight_raw:
        try:
            bw = int(body_weight_raw)
            if bw > 0:
                body_weight = bw
        except ValueError:
            pass

    return {
        "entry_key":    entry_key,
        "race_key":     race_key,
        "horse_no":     horse_no,
        "horse_id":     horse_id,
        "jockey_code":  jockey_code,
        "trainer_code": trainer_code,
        "body_weight":  body_weight,
        "finish_pos":   finish_pos,
        "is_place":     is_place,
    }


def init_normalized_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS races (
            race_key        TEXT    PRIMARY KEY,
            yyyymmdd        TEXT    NOT NULL,
            course_code     TEXT    NOT NULL,
            kai             TEXT    NOT NULL,
            day             TEXT    NOT NULL,
            race_no         TEXT    NOT NULL,
            grade_code      TEXT    NOT NULL,
            race_name_short TEXT    NOT NULL,
            created_at      TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS entries (
            entry_key   TEXT    PRIMARY KEY,
            race_key    TEXT    NOT NULL REFERENCES races(race_key),
            horse_no    TEXT    NOT NULL,
            horse_id    TEXT    NOT NULL,
            finish_pos  INTEGER,
            is_place    INTEGER,
            UNIQUE (race_key, horse_no)
        );

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
    # entries に新規列を追加 (冪等: 既存の場合はスキップ)
    for col_def in [
        "jockey_code  TEXT",
        "trainer_code TEXT",
        "body_weight  INTEGER",
    ]:
        try:
            conn.execute(f"ALTER TABLE entries ADD COLUMN {col_def}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
    conn.commit()


def build_tables(db_path: str, graded_only: bool) -> None:
    conn = sqlite3.connect(db_path)
    init_normalized_tables(conn)

    now = datetime.datetime.now().isoformat()

    rows = conn.execute(
        "SELECT payload_text FROM raw_jv_records WHERE dataspec = 'RACE'"
    ).fetchall()

    ra_count = 0
    se_count = 0

    for (payload,) in rows:
        if not payload:
            continue
        record_type = payload[:2]

        if record_type == "RA":
            rec = parse_ra(payload)
            if rec is None:
                continue
            conn.execute(
                """
                INSERT INTO races
                    (race_key, yyyymmdd, course_code, kai, day, race_no,
                     grade_code, race_name_short, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_key) DO UPDATE SET
                    yyyymmdd        = excluded.yyyymmdd,
                    course_code     = excluded.course_code,
                    kai             = excluded.kai,
                    day             = excluded.day,
                    race_no         = excluded.race_no,
                    grade_code      = excluded.grade_code,
                    race_name_short = excluded.race_name_short,
                    created_at      = excluded.created_at
                """,
                (
                    rec["race_key"],
                    rec["yyyymmdd"],
                    rec["course_code"],
                    rec["kai"],
                    rec["day"],
                    rec["race_no"],
                    rec["grade_code"],
                    rec["race_name_short"],
                    now,
                ),
            )
            ra_count += 1

        elif record_type == "SE":
            rec = parse_se(payload)
            if rec is None:
                continue
            conn.execute(
                """
                INSERT INTO entries
                    (entry_key, race_key, horse_no, horse_id, finish_pos, is_place,
                     jockey_code, trainer_code, body_weight)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entry_key) DO UPDATE SET
                    race_key     = excluded.race_key,
                    horse_no     = excluded.horse_no,
                    horse_id     = excluded.horse_id,
                    finish_pos   = excluded.finish_pos,
                    is_place     = excluded.is_place,
                    jockey_code  = excluded.jockey_code,
                    trainer_code = excluded.trainer_code,
                    body_weight  = excluded.body_weight
                """,
                (
                    rec["entry_key"],
                    rec["race_key"],
                    rec["horse_no"],
                    rec["horse_id"],
                    rec["finish_pos"],
                    rec["is_place"],
                    rec["jockey_code"],
                    rec["trainer_code"],
                    rec["body_weight"],
                ),
            )
            se_count += 1

    conn.commit()

    if graded_only:
        print("[INFO] --graded-only: 重賞レース (grade_code が空白以外) のみを残します")
        conn.execute(
            "DELETE FROM entries WHERE race_key NOT IN "
            "(SELECT race_key FROM races WHERE TRIM(grade_code) != '')"
        )
        conn.execute(
            "DELETE FROM races WHERE TRIM(grade_code) = ''"
        )
        conn.commit()

    race_total   = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    entry_total  = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]

    conn.close()

    print(f"[INFO] RA パース: {ra_count} 件 → races テーブル: {race_total} 件")
    print(f"[INFO] SE パース: {se_count} 件 → entries テーブル: {entry_total} 件")


def parse_args():
    parser = argparse.ArgumentParser(
        description="raw_jv_records から RA/SE を固定長パースして正規化テーブルを生成する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--graded-only",
        action="store_true",
        help="grade_code が空白以外 (重賞) のレース・出走のみを出力テーブルに残す",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[INFO] DB: {args.db}")
    build_tables(args.db, args.graded_only)


if __name__ == "__main__":
    main()
