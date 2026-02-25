"""
build_place_odds_from_raw.py
============================
raw_jv_records テーブルの O1 レコードを固定長パースして、
place_odds テーブルに複勝オッズ (最低/最高) を UPSERT する。

使用例:
  python scripts/build_place_odds_from_raw.py --db jv_data.db
  python scripts/build_place_odds_from_raw.py --db jv_data.db --dataspec ODDS
"""

import argparse
import datetime
import sqlite3


DEFAULT_DB_PATH = "jv_data.db"

# O1 固定長仕様 (位置は 1始まり)
_BLOCK_START = 268   # 複勝オッズブロック 先頭位置 (1始まり)
_BLOCK_LEN = 12      # 1頭あたりのブロック長
_BLOCK_COUNT = 28    # 最大頭数
_PROGRESS_LOG_INTERVAL = 10_000


def _sb(b: bytes, pos: int, length: int) -> str:
    """cp932 バイト列上で 1始まり pos から length バイトを切り出して文字列へ変換する。"""
    return b[pos - 1 : pos - 1 + length].decode("cp932", errors="ignore")


def _parse_odds(raw: str):
    """4桁オッズ文字列 → float (小数1桁) または None。"""
    s = raw.strip()
    if not s or not s.isdigit():
        return None
    return float(s) / 10.0


def parse_o1(payload: str):
    """
    O1 レコード (単複オッズ) をパースしてレース情報と複勝オッズリストを返す。
    レコード種別が 'O1' でない場合は None を返す。
    """
    if len(payload) < 2 or payload[:2] != "O1":
        return None

    b = payload.encode("cp932")

    # レコード最小長チェック: 最終ブロック末尾 = _BLOCK_START + _BLOCK_COUNT * _BLOCK_LEN - 1
    required = _BLOCK_START + _BLOCK_COUNT * _BLOCK_LEN - 1
    if len(b) < required:
        return None

    yyyy         = _sb(b, 12, 4)
    mmdd         = _sb(b, 16, 4)
    course       = _sb(b, 20, 2)
    kai          = _sb(b, 22, 2)
    day          = _sb(b, 24, 2)
    race_no      = _sb(b, 26, 2)
    announced_at = _sb(b, 28, 8)  # mmddHHMM

    race_key = f"{yyyy}{mmdd}{course}{kai}{day}{race_no}"

    # announced_at を「yyyy + mmddHHMM」形式で保存
    announced_at_str = f"{yyyy}{announced_at}" if announced_at.strip() else None

    horses = []
    for k in range(_BLOCK_COUNT):
        block_pos = _BLOCK_START + k * _BLOCK_LEN  # 1始まり
        horse_no_raw  = _sb(b, block_pos,     2)
        odds_min_raw  = _sb(b, block_pos + 2, 4)
        odds_max_raw  = _sb(b, block_pos + 6, 4)

        horse_no = horse_no_raw.strip()
        if not horse_no or not horse_no.isdigit() or int(horse_no) == 0:
            continue

        odds_min = _parse_odds(odds_min_raw)
        odds_max = _parse_odds(odds_max_raw)

        horses.append({
            "horse_no":      horse_no,
            "place_odds_min": odds_min,
            "place_odds_max": odds_max,
        })

    return {
        "race_key":      race_key,
        "announced_at":  announced_at_str,
        "horses":        horses,
    }


def init_place_odds_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS place_odds (
            race_key        TEXT    NOT NULL,
            horse_no        TEXT    NOT NULL,
            place_odds_min  REAL,
            place_odds_max  REAL,
            announced_at    TEXT,
            updated_at      TEXT    NOT NULL,
            PRIMARY KEY (race_key, horse_no)
        );
        """
    )
    conn.commit()


def build_place_odds(db_path: str, dataspec: str | None = None) -> None:
    conn = sqlite3.connect(db_path)
    init_place_odds_table(conn)

    now = datetime.datetime.now().isoformat()

    if dataspec:
        cursor = conn.execute(
            "SELECT payload_text FROM raw_jv_records"
            " WHERE SUBSTR(payload_text, 1, 2) = 'O1' AND dataspec = ?",
            (dataspec,),
        )
    else:
        cursor = conn.execute(
            "SELECT payload_text FROM raw_jv_records"
            " WHERE SUBSTR(payload_text, 1, 2) = 'O1'"
        )

    o1_count = 0
    upsert_count = 0

    for (payload,) in cursor:
        if not payload:
            continue
        rec = parse_o1(payload)
        if rec is None:
            continue
        o1_count += 1

        for h in rec["horses"]:
            conn.execute(
                """
                INSERT INTO place_odds
                    (race_key, horse_no, place_odds_min, place_odds_max,
                     announced_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_key, horse_no) DO UPDATE SET
                    place_odds_min = excluded.place_odds_min,
                    place_odds_max = excluded.place_odds_max,
                    announced_at   = excluded.announced_at,
                    updated_at     = excluded.updated_at
                """,
                (
                    rec["race_key"],
                    h["horse_no"],
                    h["place_odds_min"],
                    h["place_odds_max"],
                    rec["announced_at"],
                    now,
                ),
            )
            upsert_count += 1

        if o1_count % _PROGRESS_LOG_INTERVAL == 0:
            print(f"[INFO] 処理済み O1 レコード: {o1_count} 件")

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM place_odds").fetchone()[0]
    conn.close()

    print(f"[INFO] O1 パース: {o1_count} 件 → place_odds UPSERT: {upsert_count} 件 (テーブル合計: {total} 件)")


def parse_args():
    parser = argparse.ArgumentParser(
        description="raw_jv_records から O1 レコードを固定長パースして place_odds テーブルを生成する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--dataspec",
        default=None,
        metavar="SPEC",
        help="dataspec で絞り込む場合に指定 (省略時は全レコードから O1 を検索)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[INFO] DB: {args.db}")
    build_place_odds(args.db, args.dataspec)


if __name__ == "__main__":
    main()
