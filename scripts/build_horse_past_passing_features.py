"""
build_horse_past_passing_features.py
=====================================
race_passing_positions テーブルを元に horse_past_passing_features テーブルを構築する。

各 (race_key, horse_id) について過去 n_last レースの通過順ローリング集計を行う。

使用例:
  python scripts/build_horse_past_passing_features.py --db jv_data.db
  python scripts/build_horse_past_passing_features.py --db jv_data.db --n-last 3
"""

import argparse
import sqlite3
import sys


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_N_LAST = 3


def build_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS horse_past_passing_features")
    conn.execute("""
        CREATE TABLE horse_past_passing_features (
            race_key           TEXT NOT NULL,
            horse_id           TEXT NOT NULL,
            n_past             INTEGER,
            avg_pos_1c_last3   REAL,
            avg_pos_4c_last3   REAL,
            avg_gain_last3     REAL,
            front_rate_last3   REAL,
            avg_pos_1c_pct_last3 REAL,
            avg_pos_4c_pct_last3 REAL,
            PRIMARY KEY (race_key, horse_id)
        )
    """)
    conn.commit()


def fetch_passing_history(conn: sqlite3.Connection) -> list:
    """
    各馬について (race_key, horse_id, yyyymmdd, horse_no, corner1_pos, corner4_pos, field_size) を
    yyyymmdd 昇順で取得する。
    """
    query = """
        SELECT
            e.horse_id,
            r.yyyymmdd,
            e.race_key,
            e.horse_no,
            p1.pos   AS pos_1c,
            p4.pos   AS pos_4c,
            COUNT(*) OVER (PARTITION BY e.race_key) AS field_size
        FROM entries e
        JOIN races r ON r.race_key = e.race_key
        LEFT JOIN race_passing_positions p1
            ON p1.race_key = e.race_key
           AND p1.horse_no = PRINTF('%02d', CAST(e.horse_no AS INTEGER))
           AND p1.corner = 1
        LEFT JOIN race_passing_positions p4
            ON p4.race_key = e.race_key
           AND p4.horse_no = PRINTF('%02d', CAST(e.horse_no AS INTEGER))
           AND p4.corner = 4
        ORDER BY e.horse_id, r.yyyymmdd, e.race_key
    """
    cur = conn.execute(query)
    return cur.fetchall()


def compute_features(
    history: list[tuple],
    n_last: int,
) -> list[tuple]:
    """
    history: list of (horse_id, yyyymmdd, race_key, horse_no, pos_1c, pos_4c, field_size)
    各 (race_key, horse_id) に対して過去 n_last 件の集計を計算して返す。
    Returns: list of (race_key, horse_id, n_past, avg_pos_1c_last3, avg_pos_4c_last3,
                       avg_gain_last3, front_rate_last3, avg_pos_1c_pct_last3, avg_pos_4c_pct_last3)
    """
    # horse_id ごとにグループ化
    from collections import defaultdict
    races_by_horse: dict = defaultdict(list)
    for row in history:
        horse_id, yyyymmdd, race_key, horse_no, pos_1c, pos_4c, field_size = row
        races_by_horse[horse_id].append((yyyymmdd, race_key, pos_1c, pos_4c, field_size))

    results = []
    for horse_id, race_list in races_by_horse.items():
        # yyyymmdd 昇順はクエリ側でソート済みだが念のため
        race_list.sort(key=lambda x: (x[0], x[1]))

        for i, (yyyymmdd, race_key, pos_1c, pos_4c, field_size) in enumerate(race_list):
            # 過去 n_last 件 (現在レースは含めない)
            past = race_list[max(0, i - n_last) : i]
            n_past = len(past)

            # 有効な通過順を持つ過去レースのみ集計
            valid_1c = [(p[2], p[4]) for p in past if p[2] is not None and p[4] and p[4] > 0]
            valid_4c = [(p[3], p[4]) for p in past if p[3] is not None and p[4] and p[4] > 0]

            if valid_1c:
                avg_pos_1c = sum(p for p, _ in valid_1c) / len(valid_1c)
                avg_pos_1c_pct = sum(p / fs for p, fs in valid_1c) / len(valid_1c)
                front_rate = sum(1 for p, fs in valid_1c if p <= max(1, fs * 0.3)) / len(valid_1c)
            else:
                avg_pos_1c = None
                avg_pos_1c_pct = None
                front_rate = None

            if valid_4c:
                avg_pos_4c = sum(p for p, _ in valid_4c) / len(valid_4c)
                avg_pos_4c_pct = sum(p / fs for p, fs in valid_4c) / len(valid_4c)
            else:
                avg_pos_4c = None
                avg_pos_4c_pct = None

            # 平均ゲイン: コーナー1 からコーナー4 にかけて何ポジション上がったか
            # 両方有効なレースのみ
            valid_gain = [
                (p[2], p[3]) for p in past
                if p[2] is not None and p[3] is not None
            ]
            if valid_gain:
                avg_gain = sum(p1c - p4c for p1c, p4c in valid_gain) / len(valid_gain)
            else:
                avg_gain = None

            results.append((
                race_key,
                horse_id,
                n_past,
                avg_pos_1c,
                avg_pos_4c,
                avg_gain,
                front_rate,
                avg_pos_1c_pct,
                avg_pos_4c_pct,
            ))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="horse_past_passing_features テーブルを構築する"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--n-last",
        type=int,
        default=DEFAULT_N_LAST,
        metavar="N",
        help=f"集計対象の過去レース数 (デフォルト: {DEFAULT_N_LAST})",
    )
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    print("[INFO] horse_past_passing_features テーブルを再構築します...")
    build_table(conn)

    # race_passing_positions が存在するか確認
    try:
        conn.execute("SELECT 1 FROM race_passing_positions LIMIT 1")
    except sqlite3.OperationalError:
        print(
            "[WARN] race_passing_positions テーブルが存在しません。"
            "先に build_race_passing_positions_from_ra7.py を実行してください。",
            file=sys.stderr,
        )
        conn.close()
        sys.exit(1)

    print("[INFO] 通過順履歴を取得しています...")
    try:
        history = fetch_passing_history(conn)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] クエリ失敗: {e}", file=sys.stderr)
        conn.close()
        sys.exit(1)

    print(f"[INFO] {len(history)} 行を取得しました。特徴量を計算しています...")
    features = compute_features(history, args.n_last)

    if features:
        conn.executemany(
            """
            INSERT OR REPLACE INTO horse_past_passing_features
            (race_key, horse_id, n_past,
             avg_pos_1c_last3, avg_pos_4c_last3, avg_gain_last3,
             front_rate_last3, avg_pos_1c_pct_last3, avg_pos_4c_pct_last3)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            features,
        )
        conn.commit()

    conn.close()
    print(f"[INFO] horse_past_passing_features: {len(features)} 行を挿入しました")


if __name__ == "__main__":
    main()
