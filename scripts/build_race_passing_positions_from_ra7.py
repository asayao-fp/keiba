"""
build_race_passing_positions_from_ra7.py
========================================
raw_jv_records テーブルの RA7 レコードを解析して race_passing_positions テーブルを構築する。

使用例:
  python scripts/build_race_passing_positions_from_ra7.py --db jv_data.db
  python scripts/build_race_passing_positions_from_ra7.py --db jv_data.db --tail-len 900
"""

import argparse
import re
import sqlite3
import sys


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_TAIL_LEN = 900
DEBUG_TAIL_PREVIEW_MAX_LEN = 200

# RA7 レコード先頭の固定ヘッダ長 (RecordSpec=2, DataKubun=1, MakeDate=8 = 11)
# race_key は先頭 16 バイト (yyyymmdd=8 + JoCode=2 + Kai=2 + Nichime=2 + RaceNo=2)
# ただし RA7 では実際の race_key 位置が仕様書通りでない場合があるため
# guess_race_key() で既知の race_key セットから探索する
RA7_RECORD_SPEC = "RA"
MAX_HORSE_NO = 28  # JRA 規定の1レース最大頭数


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


def guess_race_key(text: str, known_keys: set) -> str | None:
    """
    RA7 テキストの先頭 120 文字から既知の race_key を探す。
    数字のみ抽出して既知キーとのマッチングを行い、最長一致を返す。
    """
    head = text[:120]
    digits = ''.join(re.findall(r'\d', head))
    best: str | None = None
    for rk in known_keys:
        if rk in digits:
            # race_key はすべて同一長 (16桁) のため、複数ヒット時は先頭に近い位置の
            # ものを優先する。同長の場合は最初に見つかったものを保持。
            if best is None or len(rk) > len(best):
                best = rk
    return best


def extract_corner_positions(
    text: str, field_size: int, valid_horse_nos: set, tail_len: int = DEFAULT_TAIL_LEN
) -> dict:
    """
    RA7 テキストから各コーナー通過順を抽出する。

    全角スペースを半角に正規化してスペースを折り畳み、末尾 tail_len 文字を
    空白区切りのブロックに分割する。各ブロックの先頭文字がコーナー番号 (1–4) で
    あれば、残りのテキストから re.findall(r'\\d{1,2}') で馬番を抽出する。
    同じコーナー番号のブロックが複数ある場合は '=4' を含むものを優先する。

    Returns: {corner: [horse_no_int, ...]} (corner = 1..4)
    """
    if field_size <= 0:
        return {}

    # 全角スペース → 半角スペース、連続空白を折り畳む
    normalized = text.replace('\u3000', ' ')
    tail = normalized[-tail_len:]

    # コーナーごとの候補セグメントを収集
    # 空白で区切られた各トークン (ブロック) を走査し、先頭文字がコーナー番号かどうかを確認
    corner_candidates: dict[int, str] = {}
    for seg in re.split(r'\s+', tail):
        seg = seg.strip()
        if not seg:
            continue
        m = re.match(r'^([1-4])', seg)
        if not m:
            continue
        corner = int(m.group(1))
        # '=4' を含むセグメントを優先し、同優先度では長いセグメントを選ぶ
        existing = corner_candidates.get(corner)
        if existing is None:
            corner_candidates[corner] = seg
        elif '=4' in seg and '=4' not in existing:
            corner_candidates[corner] = seg
        elif '=4' not in seg and '=4' not in existing and len(seg) > len(existing):
            corner_candidates[corner] = seg

    corners: dict = {}
    for corner, seg in corner_candidates.items():
        # 先頭のコーナー番号文字を除いた残りから馬番を抽出
        rest = seg[1:]
        positions: list = []
        seen: set = set()
        for num_str in re.findall(r'\d{1,2}', rest):
            n = int(num_str)
            if 1 <= n <= MAX_HORSE_NO and n in valid_horse_nos and n not in seen:
                positions.append(n)
                seen.add(n)
                if len(positions) >= field_size:
                    break
        if positions:
            corners[corner] = positions

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
    parser.add_argument(
        "--debug-sample",
        type=int,
        default=0,
        metavar="N",
        help="コーナー情報なしでスキップされた際、末尾セグメントを最大 N 件表示する",
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
    skipped_no_race_key = 0
    skipped_no_entries = 0
    skipped_no_corners = 0
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

        race_key = guess_race_key(payload, known_keys)
        if race_key is None:
            skipped_no_race_key += 1
            continue

        valid_horse_nos = entries_horse_nos.get(race_key, set())
        field_size = field_sizes.get(race_key, 0)
        if field_size == 0 or not valid_horse_nos:
            skipped_no_entries += 1
            continue

        corners = extract_corner_positions(payload, field_size, valid_horse_nos, args.tail_len)
        if not corners:
            skipped_no_corners += 1
            if args.debug_sample > 0 and skipped_no_corners <= args.debug_sample:
                tail_preview = repr(payload[-(min(args.tail_len, DEBUG_TAIL_PREVIEW_MAX_LEN)):])
                print(f"[DEBUG] コーナー情報なし (race_key={race_key}): tail={tail_preview}", file=sys.stderr)
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
    rows_skipped = skipped_no_race_key + skipped_no_entries + skipped_no_corners
    print(f"[INFO] 挿入: {rows_inserted} 行, スキップ合計: {rows_skipped} 件, 不正レコード: {bad_rows} 件")
    print(f"[INFO]   スキップ内訳 -- race_key 未発見: {skipped_no_race_key}, エントリなし: {skipped_no_entries}, コーナー情報なし: {skipped_no_corners}")
    print(f"[INFO] レース数: {len(race_key_counts)}, race_key 最大行数: {max_per_race}")


if __name__ == "__main__":
    main()
