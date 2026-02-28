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

# トラックコード (コード表2009) → 馬場種別マッピング
# JRA コード: 10-19 = 芝, 20-28 = ダート, 29 = 障害
# 地方コード: 51-53 = ダート, 54-56 = サンド, 57-59 = 芝, 60 = 障害, 61-64 = サンド
_TRACK_CODE_SURFACE: dict[int, str] = {}
for _c in range(10, 20):
    _TRACK_CODE_SURFACE[_c] = "芝"
for _c in range(20, 29):
    _TRACK_CODE_SURFACE[_c] = "ダート"
_TRACK_CODE_SURFACE[29] = "障害"
for _c in [51, 52, 53]:
    _TRACK_CODE_SURFACE[_c] = "ダート"
for _c in [54, 55, 56]:
    _TRACK_CODE_SURFACE[_c] = "サンド"
for _c in [57, 58, 59]:
    _TRACK_CODE_SURFACE[_c] = "芝"
_TRACK_CODE_SURFACE[60] = "障害"
for _c in [61, 62, 63, 64]:
    _TRACK_CODE_SURFACE[_c] = "サンド"


def _track_code_to_surface(track_code: str | None) -> str:
    """トラックコード (文字列) を馬場種別 (芝/ダート/サンド/障害/不明) に変換する。"""
    if not track_code:
        return "不明"
    try:
        return _TRACK_CODE_SURFACE.get(int(track_code), "不明")
    except (ValueError, TypeError):
        return "不明"


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

    # 馬場種別
    surface = _track_code_to_surface(track_code)

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
        "surface":         surface,
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
    horse_no   = _sb(b, 29, 2)
    horse_id   = _sb(b, 31, 10)
    horse_name = _sb(b, 41, 36).strip(" \u3000")
    trainer_code         = _sb(b, 86, 5)
    trainer_name_short   = _sb(b, 91, 8).strip(" \u3000")
    handicap_weight_raw  = _sb(b, 289, 3).strip()
    jockey_code          = _sb(b, 297, 5)
    jockey_name_short    = _sb(b, 307, 8).strip(" \u3000")
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
        "horse_name":          horse_name,
        "jockey_code":         jockey_code,
        "jockey_name_short":   jockey_name_short,
        "trainer_code":        trainer_code,
        "trainer_name_short":  trainer_name_short,
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

        CREATE TABLE IF NOT EXISTS horse_latest_metrics (
            horse_id             TEXT PRIMARY KEY,
            handicap_weight_x10  INTEGER,
            body_weight          INTEGER,
            race_key             TEXT,
            updated_at           TEXT NOT NULL
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

        CREATE TABLE IF NOT EXISTS horses (
            horse_id    TEXT    PRIMARY KEY,
            horse_name  TEXT    NOT NULL,
            updated_at  TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jockey_aliases (
            jockey_code      TEXT PRIMARY KEY,
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
        "surface     TEXT",
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

    cursor = conn.execute(
        "SELECT payload_text FROM raw_jv_records"
        " WHERE dataspec = 'RACE'"
        " AND SUBSTR(payload_text, 1, 2) IN ('RA', 'SE')"
    )

    ra_count = 0
    se_count = 0
    processed = 0

    for (payload,) in cursor:
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
                     grade_code, race_name_short, distance_m, track_code, surface, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    surface         = excluded.surface,
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
                    rec["surface"],
                    now,
                ),
            )
            ra_count += 1
            processed += 1
            if processed % 50_000 == 0:
                print(f"[INFO] 処理済み: {processed} 件")

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
            if rec["horse_id"] and rec["horse_name"]:
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
            if rec["horse_id"] and rec["race_key"] and (
                rec["handicap_weight_x10"] is not None or rec["body_weight"] is not None
            ):
                conn.execute(
                    """
                    INSERT INTO horse_latest_metrics
                        (horse_id, handicap_weight_x10, body_weight, race_key, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(horse_id) DO UPDATE SET
                        handicap_weight_x10 = CASE
                            WHEN excluded.race_key >= race_key
                            THEN COALESCE(excluded.handicap_weight_x10, handicap_weight_x10)
                            ELSE handicap_weight_x10
                        END,
                        body_weight = CASE
                            WHEN excluded.race_key >= race_key
                            THEN COALESCE(excluded.body_weight, body_weight)
                            ELSE body_weight
                        END,
                        race_key = CASE
                            WHEN excluded.race_key >= race_key
                            THEN excluded.race_key
                            ELSE race_key
                        END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        rec["horse_id"],
                        rec["handicap_weight_x10"],
                        rec["body_weight"],
                        rec["race_key"],
                        now,
                    ),
                )
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
            se_count += 1
            processed += 1
            if processed % 50_000 == 0:
                print(f"[INFO] 処理済み: {processed} 件")

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
    horse_total  = conn.execute("SELECT COUNT(*) FROM horses").fetchone()[0]
    jockey_alias_total  = conn.execute("SELECT COUNT(*) FROM jockey_aliases").fetchone()[0]
    trainer_alias_total = conn.execute("SELECT COUNT(*) FROM trainer_aliases").fetchone()[0]
    metrics_total = conn.execute("SELECT COUNT(*) FROM horse_latest_metrics").fetchone()[0]

    conn.close()

    print(f"[INFO] RA パース: {ra_count} 件 → races テーブル: {race_total} 件")
    print(f"[INFO] SE パース: {se_count} 件 → entries テーブル: {entry_total} 件, horses テーブル: {horse_total} 件")
    print(f"[INFO] jockey_aliases: {jockey_alias_total} 件, trainer_aliases: {trainer_alias_total} 件")
    print(f"[INFO] horse_latest_metrics: {metrics_total} 件")


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
