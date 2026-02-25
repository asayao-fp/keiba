"""
suggest_place_bets.py
=====================
予測確率 JSON とオッズ CSV (または DB) を突合して複勝買い目候補を出力する。

使用例:
  python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv
  python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv --format csv
  python scripts/suggest_place_bets.py --pred-json pred.json --odds-csv data/sample_place_odds.csv --odds-use mid --min-ev 0.05 --stake 500 --max-bets 5
  python scripts/suggest_place_bets.py --pred-json pred.json --db jv_data.db --race-key 202401010102010101
"""

import argparse
import csv
import json
import sqlite3
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="予測確率 JSON とオッズ CSV または DB から複勝買い目候補を出力する"
    )
    parser.add_argument(
        "--pred-json",
        required=True,
        metavar="PATH",
        help="predict_place.py が出力した JSON ファイルパス",
    )
    parser.add_argument(
        "--odds-csv",
        default=None,
        metavar="PATH",
        help="オッズ CSV ファイルパス (horse_no, place_odds_min, place_odds_max 列必須)。省略時は --db/--race-key から取得",
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help="SQLite DB ファイルパス (--odds-csv 省略時に使用)",
    )
    parser.add_argument(
        "--race-key",
        default=None,
        metavar="KEY",
        help="レースキー (--odds-csv 省略時に使用)",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["json", "csv"],
        default="json",
        help="出力フォーマット (デフォルト: json)",
    )
    parser.add_argument(
        "--odds-use",
        choices=["min", "max", "mid"],
        default="min",
        help="使用するオッズ (デフォルト: min)",
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=0.0,
        metavar="FLOAT",
        help="期待値しきい値 (デフォルト: 0.0)",
    )
    parser.add_argument(
        "--stake",
        type=int,
        default=100,
        metavar="INT",
        help="1点あたり賭け金 (デフォルト: 100)",
    )
    parser.add_argument(
        "--max-bets",
        type=int,
        default=3,
        metavar="INT",
        help="最大購入点数 (デフォルト: 3)",
    )
    return parser.parse_args()


def _norm_horse_no(x) -> str:
    """馬番を正規化する。"04" と "4" を同一視するため int 変換後に文字列化する。"""
    try:
        return str(int(str(x)))
    except (ValueError, TypeError):
        return str(x)


def load_pred_json(path: str) -> list[dict]:
    # BOM 付き UTF-8 / UTF-16LE / UTF-16BE に対応するためバイナリで読み込む
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        print(f"[ERROR] 予測 JSON が見つかりません: {path}", file=sys.stderr)
        sys.exit(1)

    if raw.startswith(b"\xff\xfe"):
        encoding = "utf-16-le"
        raw = raw[2:]
    elif raw.startswith(b"\xfe\xff"):
        encoding = "utf-16-be"
        raw = raw[2:]
    elif raw.startswith(b"\xef\xbb\xbf"):
        encoding = "utf-8"
        raw = raw[3:]
    else:
        encoding = "utf-8"

    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError as e:
        print(
            f"[ERROR] 予測 JSON のデコードに失敗しました (encoding={encoding}): {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[ERROR] 予測 JSON の解析に失敗しました: {e}", file=sys.stderr)
        sys.exit(1)
    if not isinstance(data, list):
        print("[ERROR] 予測 JSON はリスト形式である必要があります", file=sys.stderr)
        sys.exit(1)
    return data


def load_odds_csv(path: str) -> dict[str, dict]:
    """horse_no をキーとするオッズ辞書を返す。不正な行はエラー終了。"""
    try:
        f = open(path, encoding="utf-8-sig", newline="")
    except FileNotFoundError:
        print(f"[ERROR] オッズ CSV が見つかりません: {path}", file=sys.stderr)
        sys.exit(1)

    with f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print("[ERROR] オッズ CSV が空またはヘッダがありません", file=sys.stderr)
            sys.exit(1)
        missing_cols = {"horse_no", "place_odds_min", "place_odds_max"} - set(reader.fieldnames)
        if missing_cols:
            print(
                f"[ERROR] オッズ CSV に必須列がありません: {', '.join(sorted(missing_cols))}",
                file=sys.stderr,
            )
            sys.exit(1)

        odds_map: dict[str, dict] = {}
        for lineno, row in enumerate(reader, start=2):
            horse_no = row["horse_no"]
            min_str = row["place_odds_min"]
            max_str = row["place_odds_max"]

            if min_str == "" or min_str is None:
                print(
                    f"[ERROR] オッズ CSV {lineno}行目: place_odds_min が空です (horse_no={horse_no})",
                    file=sys.stderr,
                )
                sys.exit(1)
            if max_str == "" or max_str is None:
                print(
                    f"[ERROR] オッズ CSV {lineno}行目: place_odds_max が空です (horse_no={horse_no})",
                    file=sys.stderr,
                )
                sys.exit(1)

            try:
                odds_min = float(min_str)
                odds_max = float(max_str)
            except ValueError:
                print(
                    f"[ERROR] オッズ CSV {lineno}行目: オッズが数値でありません (horse_no={horse_no})",
                    file=sys.stderr,
                )
                sys.exit(1)

            if odds_min > odds_max:
                print(
                    f"[ERROR] オッズ CSV {lineno}行目: place_odds_min > place_odds_max (horse_no={horse_no}, min={odds_min}, max={odds_max})",
                    file=sys.stderr,
                )
                sys.exit(1)

            odds_map[_norm_horse_no(horse_no)] = {
                "place_odds_min": odds_min,
                "place_odds_max": odds_max,
            }

    return odds_map


def load_odds_db(db_path: str, race_key: str) -> dict[str, dict]:
    """place_odds テーブルから horse_no をキーとするオッズ辞書を返す。"""
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.OperationalError as e:
        print(f"[ERROR] DB 接続に失敗しました: {e}", file=sys.stderr)
        sys.exit(1)

    with conn:
        cursor = conn.execute(
            "SELECT horse_no, place_odds_min, place_odds_max"
            " FROM place_odds WHERE race_key = ?",
            (race_key,),
        )
        odds_map: dict[str, dict] = {}
        for horse_no, odds_min, odds_max in cursor:
            if odds_min is None or odds_max is None:
                continue
            odds_map[_norm_horse_no(horse_no)] = {
                "place_odds_min": odds_min,
                "place_odds_max": odds_max,
            }

    if not odds_map:
        print(
            f"[WARN] DB の place_odds テーブルに race_key={race_key} のデータがありません。",
            file=sys.stderr,
        )

    return odds_map


def compute_bets(
    pred_rows: list[dict],
    odds_map: dict[str, dict],
    odds_use: str,
    min_ev: float,
    stake: int,
    max_bets: int,
) -> list[dict]:
    results = []
    for row in pred_rows:
        horse_no = _norm_horse_no(row.get("horse_no", ""))
        if horse_no not in odds_map:
            print(
                f"[WARN] horse_no={horse_no} はオッズ CSV にありません。スキップします。",
                file=sys.stderr,
            )
            continue

        p_place = float(row["p_place"])
        o = odds_map[horse_no]
        odds_min = o["place_odds_min"]
        odds_max = o["place_odds_max"]
        odds_mid = (odds_min + odds_max) / 2

        if odds_use == "min":
            odds_used = odds_min
        elif odds_use == "max":
            odds_used = odds_max
        else:
            odds_used = odds_mid

        ev_per_1unit = p_place * odds_used - 1
        expected_value_yen = round(ev_per_1unit * stake, 2)

        results.append(
            {
                "horse_no": horse_no,
                "horse_id": row.get("horse_id", ""),
                "p_place": p_place,
                "place_odds_min": odds_min,
                "place_odds_max": odds_max,
                "place_odds_used": round(odds_used, 2),
                "ev_per_1unit": round(ev_per_1unit, 4),
                "stake": stake,
                "expected_value_yen": expected_value_yen,
            }
        )

    # ev_per_1unit 降順
    results.sort(key=lambda r: r["ev_per_1unit"], reverse=True)

    # min_ev フィルタ
    results = [r for r in results if r["ev_per_1unit"] >= min_ev]

    # max_bets で切る
    results = results[:max_bets]

    return results


def output_json(rows: list[dict]) -> None:
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def output_csv(rows: list[dict]) -> None:
    if not rows:
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)


def main():
    args = parse_args()

    pred_rows = load_pred_json(args.pred_json)

    if args.odds_csv is not None:
        odds_map = load_odds_csv(args.odds_csv)
    else:
        if not args.db or not args.race_key:
            print(
                "[ERROR] --odds-csv を省略する場合は --db と --race-key を両方指定してください。",
                file=sys.stderr,
            )
            sys.exit(1)
        odds_map = load_odds_db(args.db, args.race_key)

    bets = compute_bets(
        pred_rows,
        odds_map,
        odds_use=args.odds_use,
        min_ev=args.min_ev,
        stake=args.stake,
        max_bets=args.max_bets,
    )

    if args.fmt == "csv":
        output_csv(bets)
    else:
        output_json(bets)


if __name__ == "__main__":
    main()
