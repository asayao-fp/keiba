"""
build_race_passing_positions_from_ra7.py
========================================
raw_jv_records テーブルの RA7 レコードを解析して race_passing_positions テーブルを構築する。

使用例:
  python scripts/build_race_passing_positions_from_ra7.py --db jv_data.db
  python scripts/build_race_passing_positions_from_ra7.py --db jv_data.db --tail-len 900
"""

import argparse
import sqlite3
import sys


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_TAIL_LEN = 900

# RA7 レコード先頭の固定ヘッダ長 (RecordSpec=2, DataKubun=1, MakeDate=8 = 11)
# race_key は先頭 16 バイト (yyyymmdd=8 + JoCode=2 + Kai=2 + Nichime=2 + RaceNo=2)
# ただし RA7 では実際の race_key 位置が仕様書通りでない場合があるため
# guess_race_key() で既知の race_key セットから探索する
RA7_RECORD_SPEC = "RA"


def build_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS race_passing_positions")
    conn.execute("""
        CREATE TABLE race_passing_positions (
            race_key  TEXT NOT NULL,
            horse_no  TEXT NOT NULL,
            corner    INTEGER NOT NULL,
            pos       INTEGER NOT NULL,
            PRIMARY KEY (race_key, horse_no, corner)
        )
    """)
    conn.commit()


def load_known_race_keys(conn: sqlite3.Connection) -> set:
    cur = conn.execute("SELECT race_key FROM races")
    return {row[0] for row in cur.fetchall()}


def load_entries_horse_nos(conn: sqlite3.Connection) -> dict:
    """race_key -> set of integer horse_no"""
    cur = conn.execute("SELECT race_key, horse_no FROM entries")
    entries_by_race: dict = {}
    for race_key, horse_no in cur.fetchall():
        try:
            hn = int(horse_no)
        except (ValueError, TypeError):
            continue
        entries_by_race.setdefault(race_key, set()).add(hn)
    return entries_by_race


def load_field_sizes(conn: sqlite3.Connection) -> dict:
    """race_key -> int field size (count of entries)"""
    cur = conn.execute("SELECT race_key, COUNT(*) FROM entries GROUP BY race_key")
    return {row[0]: row[1] for row in cur.fetchall()}


def guess_race_key(text: str, known_keys: set, tail_len: int) -> str | None:
    """
    RA7 テキストの末尾 tail_len バイト相当の部分から既知の race_key (16文字) を探す。
    先頭から順に 16文字を切り出して既知セットと照合する。
    """
    # RA7 の race_key は先頭 2(RecordSpec) + 1(DataKubun) + 8(MakeDate) = 11 バイト後から
    # 実際には RecordSpec 含む先頭から 16 文字の範囲に race_key が存在する
    # RA7 仕様: RecordSpec(2) DataKubun(1) MakeDate(8) RaceID(16) ...
    # RaceID はオフセット 11 から 16 文字
    candidate = text[11:27]
    if candidate in known_keys:
        return candidate
    # フォールバック: 先頭から走査
    for i in range(0, min(50, len(text) - 16)):
        candidate = text[i : i + 16]
        if candidate in known_keys:
            return candidate
    return None


def extract_corner_positions(tail: str, field_size: int, valid_horse_nos: set) -> dict:
    """
    RA7 テキスト末尾から各コーナー通過順を抽出する。

    RA7 の通過順フォーマット (仕様書より):
    各コーナーは「馬番(2桁)」を連結したもの。
    コーナー数分のブロックが並ぶ。

    Returns: {corner: [horse_no_int, ...]} (corner = 1..4)
    """
    # RA7 末尾の通過順ブロック解析
    # 通過順は各コーナーあたり最大 field_size * 2 桁
    # 仕様: Lap=1 のみ対象 (コーナー通過順)
    # tail から "コーナー数" と通過順を読み取る
    # RA7 テキスト内での通過順領域:
    #   LapTimeInfo ブロックの後に KakuteiFlag + 各コーナー通過順が並ぶ
    # 実装上は tail 末尾から固定オフセットで各コーナーブロックを読む

    # 各コーナーブロックサイズ = field_size * 2 桁
    block = field_size * 2
    corners: dict = {}

    # tail の末尾側に 4 コーナー分のブロックが並んでいると仮定
    # コーナー4 が最末尾寄りに、コーナー1 が先頭寄りに配置
    # 十分な長さがあるか確認
    needed = block * 4
    if len(tail) < needed:
        return corners

    # 末尾から 4ブロック分を取り出す
    segment = tail[-needed:]

    for corner_idx in range(4):
        start = corner_idx * block
        chunk = segment[start : start + block]
        positions = []
        valid = True
        for i in range(0, len(chunk), 2):
            token = chunk[i : i + 2]
            try:
                hn = int(token)
            except ValueError:
                valid = False
                break
            if hn == 0:
                # ゼロパディング終端
                break
            positions.append(hn)
        if not valid:
            continue
        # 有効馬番のみに絞り込み
        filtered = [h for h in positions if h in valid_horse_nos]
        if filtered:
            corners[corner_idx + 1] = filtered

    return corners


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RA7 レコードから race_passing_positions テーブルを構築する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--tail-len",
        type=int,
        default=DEFAULT_TAIL_LEN,
        metavar="N",
        help=f"RA7 レコード末尾から参照する文字数 (デフォルト: {DEFAULT_TAIL_LEN})",
    )
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    print("[INFO] race_passing_positions テーブルを再構築します...")
    build_table(conn)

    known_keys = load_known_race_keys(conn)
    entries_horse_nos = load_entries_horse_nos(conn)
    field_sizes = load_field_sizes(conn)
    print(f"[INFO] 既知 race_key: {len(known_keys)} 件")

    # RA7 レコードを取得 (payload_text の先頭3文字で絞り込む)
    try:
        ra7_count = conn.execute(
            "SELECT COUNT(*) FROM raw_jv_records WHERE substr(payload_text, 1, 3) = 'RA7'"
        ).fetchone()[0]
        print(f"[INFO] RA7 レコード: {ra7_count} 件")
        if ra7_count == 0:
            print("[WARN] RA7 レコードが見つかりませんでした。payload_text カラムを確認してください。", file=sys.stderr)
        cur = conn.execute(
            "SELECT payload_text FROM raw_jv_records WHERE substr(payload_text, 1, 3) = 'RA7'"
        )
    except sqlite3.OperationalError as e:
        print(f"[ERROR] raw_jv_records クエリ失敗: {e}", file=sys.stderr)
        conn.close()
        sys.exit(1)

    rows_inserted = 0
    rows_skipped = 0
    bad_rows = 0
    race_key_counts: dict = {}

    batch: list = []
    BATCH_SIZE = 1000

    def flush_batch() -> None:
        nonlocal rows_inserted
        if not batch:
            return
        conn.executemany(
            "INSERT OR REPLACE INTO race_passing_positions (race_key, horse_no, corner, pos) VALUES (?, ?, ?, ?)",
            batch,
        )
        conn.commit()
        rows_inserted += len(batch)
        batch.clear()

    for (payload,) in cur:
        if not payload or len(payload) < 27:
            bad_rows += 1
            continue

        race_key = guess_race_key(payload, known_keys, args.tail_len)
        if race_key is None:
            rows_skipped += 1
            continue

        valid_horse_nos = entries_horse_nos.get(race_key, set())
        field_size = field_sizes.get(race_key, 0)
        if field_size == 0 or not valid_horse_nos:
            rows_skipped += 1
            continue

        tail = payload[-args.tail_len :]
        corners = extract_corner_positions(tail, field_size, valid_horse_nos)
        if not corners:
            rows_skipped += 1
            continue

        added = 0
        for corner, positions in corners.items():
            for pos_idx, horse_no_int in enumerate(positions, start=1):
                # pos が field_size を超えないようにする
                if pos_idx > field_size:
                    continue
                # horse_no は2桁ゼロパディング文字列で保存
                horse_no_str = f"{horse_no_int:02d}"
                batch.append((race_key, horse_no_str, corner, pos_idx))
                added += 1

        if added > 0:
            race_key_counts[race_key] = race_key_counts.get(race_key, 0) + added
            if len(batch) >= BATCH_SIZE:
                flush_batch()

    flush_batch()
    conn.close()

    max_per_race = max(race_key_counts.values()) if race_key_counts else 0
    print(f"[INFO] 挿入: {rows_inserted} 行, スキップ: {rows_skipped} 件, 不正レコード: {bad_rows} 件")
    print(f"[INFO] レース数: {len(race_key_counts)}, race_key 最大行数: {max_per_race}")


if __name__ == "__main__":
    main()
