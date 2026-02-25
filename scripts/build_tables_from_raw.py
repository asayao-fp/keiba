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


def _sb(b: bytes, pos: int, length: int) -> str:
    """cp932 バイト列上で 1始まり pos から length バイトを切り出して文字列へ変換する。"""
    return b[pos - 1 : pos - 1 + length].decode("cp932", errors="ignore")


def parse_ra(payload: str):
    """
    RA レコード (レース詳細, 1272 byte) をパースして dict を返す。
    レコード種別識別子が 'RA' でない場合は None を返す。
    """
    b = payload.encode("cp932")
    if len(b) < 707 or payload[:2] != "RA":
        return None

    yyyy        = _sb(b, 12, 4)
    mmdd        = _sb(b, 16, 4)
    course      = _sb(b, 20, 2)
    kai         = _sb(b, 22, 2)
    day         = _sb(b, 24, 2)
    raceno      = _sb(b, 26, 2)
    race_name_short = _sb(b, 605, 6)
    grade_code  = _sb(b, 615, 1)

    # 距離 (メートル)
    distance_m = None
    distance_raw = _sb(b, 637, 4).strip()
    if distance_raw:
        try:
            dm = int(distance_raw)
            if dm > 0:
                distance_m = dm
        except ValueError:
            pass

    # トラックコード (2009)
    track_code = _sb(b, 706, 2).strip() or None

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
        "distance_m":      distance_m,
        "track_code":      track_code,
    }


def parse_se(payload: str):
    """
    SE レコード (馬毎レース情報, 555 byte) をパースして dict を返す。
    レコード種別識別子が 'SE' でない場合は None を返す。
    """
    b = payload.encode("cp932")
    if len(b) < 336 or payload[:2] != "SE":
        return None

    yyyy   = _sb(b, 12, 4)
    mmdd   = _sb(b, 16, 4)
    course = _sb(b, 20, 2)
    kai    = _sb(b, 22, 2)
    day    = _sb(b, 24, 2)
    raceno = _sb(b, 26, 2)
    horse_no  = _sb(b, 29, 2)
    horse_id  = _sb(b, 31, 10)
    trainer_code         = _sb(b, 86, 5)
    handicap_weight_raw  = _sb(b, 289, 3).strip()
    jockey_code          = _sb(b, 297, 5)
    body_weight_raw      = _sb(b, 325, 3).strip()
    finish_raw           = _sb(b, 335, 2).strip()

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

    # 負担重量 (斤量) 単位 0.1kg: 数値変換できない場合は NULL
    handicap_weight_x10 = None
    if handicap_weight_raw:
        try:
            hw = int(handicap_weight_raw)
            if hw > 0:
                handicap_weight_x10 = hw
        except ValueError:
            pass

    return {
        "entry_key":           entry_key,
        "race_key":            race_key,
        "horse_no":            horse_no,
        "horse_id":            horse_id,
        "jockey_code":         jockey_code,
        "trainer_code":        trainer_code,
        "body_weight":         body_weight,
        "handicap_weight_x10": handicap_weight_x10,
        "finish_pos":          finish_pos,
        "is_place":            is_place,
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
        "jockey_code         TEXT",
        "trainer_code        TEXT",
        "body_weight         INTEGER",
        "handicap_weight_x10 INTEGER",
    ]:
        try:
            conn.execute(f"ALTER TABLE entries ADD COLUMN {col_def}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
    # races に新規列を追加 (冪等: 既存の場合はスキップ)
    for col_def in [
        "distance_m INTEGER",
        "track_code  TEXT",
    ]:
        try:
            conn.execute(f"ALTER TABLE races ADD COLUMN {col_def}")
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
                     grade_code, race_name_short, distance_m, track_code, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_key) DO UPDATE SET
                    yyyymmdd        = excluded.yyyymmdd,
                    course_code     = excluded.course_code,
                    kai             = excluded.kai,
                    day             = excluded.day,
                    race_no         = excluded.race_no,
                    grade_code      = excluded.grade_code,
                    race_name_short = excluded.race_name_short,
                    distance_m      = excluded.distance_m,
                    track_code      = excluded.track_code,
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
                    rec["distance_m"],
                    rec["track_code"],
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
                     jockey_code, trainer_code, body_weight, handicap_weight_x10)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entry_key) DO UPDATE SET
                    race_key             = excluded.race_key,
                    horse_no             = excluded.horse_no,
                    horse_id             = excluded.horse_id,
                    finish_pos           = excluded.finish_pos,
                    is_place             = excluded.is_place,
                    jockey_code          = excluded.jockey_code,
                    trainer_code         = excluded.trainer_code,
                    body_weight          = excluded.body_weight,
                    handicap_weight_x10  = excluded.handicap_weight_x10
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
                    rec["handicap_weight_x10"],
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
