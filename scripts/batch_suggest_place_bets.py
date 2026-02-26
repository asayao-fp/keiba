"""
batch_suggest_place_bets.py
===========================
複数レースの複勝買い目提案を一括生成し、集計 CSV を出力する。

使用例:
  python scripts/batch_suggest_place_bets.py \\
      --race-keys 202401010102010101 202401010102010102 \\
      --db jv_data.db --model models/place_model.cbm --out-dir out/

  python scripts/batch_suggest_place_bets.py \\
      --race-keys-file race_keys.txt \\
      --db jv_data.db --model models/place_model.cbm --out-dir out/ \\
      --summary-csv out/summary.csv

  # 既存の pred_<race_key>.json を再利用して買い目提案のみ実行 (モデル不要)
  python scripts/batch_suggest_place_bets.py \\
      --race-keys 202401010102010101 202401010102010102 \\
      --db jv_data.db --out-dir out/ \\
      --skip-predict --pred-dir out/
"""

import argparse
import csv
import json
import os
import sqlite3
import sys

import pandas as pd
from catboost import CatBoostClassifier

# Ensure sibling scripts are importable when running from any working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from predict_place import (  # noqa: E402
    CATEGORICAL_FEATURES,
    FEATURE_COLS,
    NUMERIC_FEATURES,
    fetch_entries_for_race,
)
from suggest_place_bets import compute_bets, load_odds_db  # noqa: E402


DEFAULT_DB_PATH = "jv_data.db"
DEFAULT_MODEL_PATH = "models/place_model.cbm"

SUMMARY_FIELDS = [
    "race_key",
    "status",
    "n_bets",
    "total_stake",
    "sum_expected_value_yen",
    "avg_p_place",
    "avg_odds_used",
    "max_p_place",
    "max_ev_per_1unit",
    "fallback_used",
    "error",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="複数レースの複勝買い目提案を一括生成して集計CSVを出力する"
    )

    # --- 入力レースキー ---
    keys_group = parser.add_argument_group("入力レースキー (どちらか一方は必須)")
    keys_group.add_argument(
        "--race-keys",
        nargs="+",
        metavar="RACE_KEY",
        help="レースキー（スペース区切りで複数指定）",
    )
    keys_group.add_argument(
        "--race-keys-file",
        metavar="PATH",
        help="レースキーを1行1件で記載したテキストファイル",
    )

    # --- モデル・DB ---
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite DB ファイルパス (デフォルト: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL_PATH,
        metavar="PATH",
        help=f"学習済みモデルパス (デフォルト: {DEFAULT_MODEL_PATH})",
    )

    # --- 買い目設定 (suggest_place_bets.py と同等) ---
    parser.add_argument(
        "--mode",
        choices=["balance"],
        default=None,
        help="運用プリセット: balance=収益性を維持しつつ当たりやすさにも配慮"
        " (rank_by=ev_then_p, min_p_place=0.20, max_odds_used=15 をデフォルト設定。明示指定した引数は優先される)",
    )
    parser.add_argument(
        "--rank-by",
        choices=["p", "ev", "ev_then_p"],
        default=None,
        help="ランキング基準: p=p_place降順, ev=期待値降順, ev_then_p=期待値降順→同率はp_placeでtie-break"
        " (デフォルト: ev、balance モード時は ev_then_p)",
    )
    parser.add_argument(
        "--min-p-place",
        type=float,
        default=None,
        metavar="FLOAT",
        help="複勝圏確率の下限しきい値 (デフォルト: 0.0、balance モード時は 0.20)。これ未満の候補は除外",
    )
    parser.add_argument(
        "--max-odds-used",
        type=float,
        default=None,
        metavar="FLOAT",
        help="使用オッズの上限 (デフォルト: なし、balance モード時は 15)。これを超える候補は除外",
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=0.0,
        metavar="FLOAT",
        help="期待値しきい値 (デフォルト: 0.0)",
    )
    parser.add_argument(
        "--odds-use",
        choices=["min", "max", "mid"],
        default="min",
        help="使用するオッズ (デフォルト: min)",
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

    # --- 出力 ---
    parser.add_argument(
        "--out-dir",
        required=True,
        metavar="PATH",
        help="出力ディレクトリ",
    )
    parser.add_argument(
        "--summary-csv",
        default=None,
        metavar="PATH",
        help="集計CSVパス (デフォルト: <out-dir>/summary.csv)",
    )

    # --- 動作 ---
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="いずれかのレースでエラーが発生した際に即座に終了する (デフォルト: 他レースは続行)",
    )
    parser.add_argument(
        "--skip-predict",
        action="store_true",
        help="予測ステップをスキップし、既存の pred_<race_key>.json を再利用する (モデル不要)",
    )
    parser.add_argument(
        "--pred-dir",
        default=None,
        metavar="PATH",
        help="--skip-predict 時に pred_<race_key>.json を読み込むディレクトリ"
        " (デフォルト: --out-dir と同じ)",
    )

    return parser.parse_args()


def _predict_for_race(
    conn: sqlite3.Connection,
    race_key: str,
    model: CatBoostClassifier,
) -> list[dict]:
    """指定レースの複勝圏確率を推論して馬ごとの結果リストを返す。"""
    entries = fetch_entries_for_race(conn, race_key)
    if not entries:
        raise ValueError(f"出走馬が見つかりません: {race_key}")

    df = pd.DataFrame(entries)
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].fillna("").astype(str)

    proba = model.predict_proba(df[FEATURE_COLS].copy())[:, 1]

    results = [
        {
            "horse_no": entry["horse_no"],
            "horse_id": entry["horse_id"],
            "entry_key": entry["entry_key"],
            "race_key": entry["race_key"],
            "p_place": round(float(proba[i]), 4),
        }
        for i, entry in enumerate(entries)
    ]
    results.sort(key=lambda r: r["p_place"], reverse=True)
    return results


def _summarize_bets(race_key: str, bets: list[dict], *, fallback_used: bool = False) -> dict:
    """bets リストから summary.csv の1行を生成する。"""
    n = len(bets)
    if n == 0:
        return {
            "race_key": race_key,
            "status": "ok",
            "n_bets": 0,
            "total_stake": 0,
            "sum_expected_value_yen": 0.0,
            "avg_p_place": None,
            "avg_odds_used": None,
            "max_p_place": None,
            "max_ev_per_1unit": None,
            "fallback_used": False,
            "error": "",
        }
    return {
        "race_key": race_key,
        "status": "ok",
        "n_bets": n,
        "total_stake": sum(b["stake"] for b in bets),
        "sum_expected_value_yen": round(sum(b["expected_value_yen"] for b in bets), 2),
        "avg_p_place": round(sum(b["p_place"] for b in bets) / n, 4),
        "avg_odds_used": round(sum(b["place_odds_used"] for b in bets) / n, 4),
        "max_p_place": round(max(b["p_place"] for b in bets), 4),
        "max_ev_per_1unit": round(max(b["ev_per_1unit"] for b in bets), 4),
        "fallback_used": fallback_used,
        "error": "",
    }


def main() -> None:
    args = parse_args()

    # --- レースキーを収集 ---
    race_keys: list[str] = list(args.race_keys) if args.race_keys else []
    if args.race_keys_file:
        try:
            with open(args.race_keys_file, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            print(
                f"[ERROR] レースキーファイルが見つかりません: {args.race_keys_file}",
                file=sys.stderr,
            )
            sys.exit(1)

        # Encoding detection: utf-8-sig strips UTF-8 BOM, utf-16 handles UTF-16 LE/BE BOM
        for enc in ("utf-8-sig", "utf-16", "utf-8"):
            try:
                text = raw.decode(enc)
                break
            except (UnicodeDecodeError, ValueError):
                continue
        else:
            print(
                f"[ERROR] レースキーファイルのデコードに失敗しました: {args.race_keys_file}",
                file=sys.stderr,
            )
            sys.exit(1)

        for lineno, line in enumerate(text.splitlines(), start=1):
            key = line.strip().lstrip("\ufeff")
            if not key or key.startswith("#"):
                print(
                    f"[DEBUG] レースキーファイル行 {lineno} をスキップしました: {line!r}",
                    file=sys.stderr,
                )
                continue
            race_keys.append(key)

    if not race_keys:
        print(
            "[ERROR] --race-keys または --race-keys-file を指定してください。",
            file=sys.stderr,
        )
        sys.exit(1)

    # 順序を保ったまま重複除去
    race_keys = list(dict.fromkeys(race_keys))

    # --- 出力ディレクトリ準備 ---
    os.makedirs(args.out_dir, exist_ok=True)
    summary_csv_path = args.summary_csv or os.path.join(args.out_dir, "summary.csv")

    # --skip-predict 時の pred ディレクトリ (デフォルトは out_dir と同じ)
    pred_dir = args.pred_dir or args.out_dir

    # --- --mode balance プリセットを適用 ---
    if args.mode == "balance":
        if args.rank_by is None:
            args.rank_by = "ev_then_p"
        if args.min_p_place is None:
            args.min_p_place = 0.20
        if args.max_odds_used is None:
            args.max_odds_used = 15.0

    # --- 残りのデフォルト値を適用 ---
    if args.rank_by is None:
        args.rank_by = "ev"
    if args.min_p_place is None:
        args.min_p_place = 0.0

    # --- モデルを一度だけ読み込む (--skip-predict 時はスキップ) ---
    model = None
    if not args.skip_predict:
        try:
            model = CatBoostClassifier()
            model.load_model(args.model)
        except Exception as e:
            print(f"[ERROR] モデル読み込み失敗: {e}", file=sys.stderr)
            sys.exit(1)

    # --- DB に一度だけ接続する ---
    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"[ERROR] DB接続失敗: {e}", file=sys.stderr)
        sys.exit(1)

    summary_rows: list[dict] = []
    failed: list[str] = []

    try:
        for race_key in race_keys:
            print(f"[INFO] race_key={race_key}", file=sys.stderr)
            try:
                if args.skip_predict:
                    # 1. 既存の pred JSON を読み込む
                    pred_path = os.path.join(pred_dir, f"pred_{race_key}.json")
                    if not os.path.exists(pred_path):
                        raise FileNotFoundError(
                            f"pred ファイルが見つかりません: {pred_path}"
                        )
                    with open(pred_path, encoding="utf-8") as fh:
                        pred = json.load(fh)
                else:
                    # 1. 予測
                    assert model is not None, "モデルが読み込まれていません"
                    pred = _predict_for_race(conn, race_key, model)
                    pred_path = os.path.join(args.out_dir, f"pred_{race_key}.json")
                    with open(pred_path, "w", encoding="utf-8") as fh:
                        json.dump(pred, fh, ensure_ascii=False, indent=2)

                # 2. オッズを DB から取得
                odds_map = load_odds_db(args.db, race_key)

                # 3. 買い目を計算
                bets = compute_bets(
                    pred,
                    odds_map,
                    odds_use=args.odds_use,
                    min_ev=args.min_ev,
                    stake=args.stake,
                    max_bets=args.max_bets,
                    rank_by=args.rank_by,
                    min_p_place=args.min_p_place,
                    max_odds_used=args.max_odds_used,
                )
                fallback_used = False
                if not bets:
                    # フォールバック: EV 制約を解除し、p_place 最大の1頭を選ぶ
                    bets = compute_bets(
                        pred,
                        odds_map,
                        odds_use=args.odds_use,
                        min_ev=-1e9,
                        stake=args.stake,
                        max_bets=1,
                        rank_by="p",
                        min_p_place=args.min_p_place,
                        max_odds_used=args.max_odds_used,
                    )
                    if bets:
                        fallback_used = True
                        print(
                            f"[INFO] race_key={race_key}: フォールバック適用"
                            f" - horse_no={bets[0]['horse_no']} (p_place={bets[0]['p_place']})",
                            file=sys.stderr,
                        )
                bets_path = os.path.join(args.out_dir, f"bets_{race_key}.json")
                with open(bets_path, "w", encoding="utf-8") as fh:
                    json.dump(bets, fh, ensure_ascii=False, indent=2)

                # 4. 集計行を追加
                summary_rows.append(_summarize_bets(race_key, bets, fallback_used=fallback_used))

            except Exception as e:
                print(
                    f"[ERROR] race_key={race_key} の処理に失敗しました: {e}",
                    file=sys.stderr,
                )
                failed.append(race_key)
                summary_rows.append(
                    {
                        "race_key": race_key,
                        "status": "failed",
                        "n_bets": 0,
                        "total_stake": 0,
                        "sum_expected_value_yen": 0.0,
                        "avg_p_place": None,
                        "avg_odds_used": None,
                        "max_p_place": None,
                        "max_ev_per_1unit": None,
                        "fallback_used": False,
                        "error": str(e),
                    }
                )
                if args.fail_fast:
                    break
    finally:
        conn.close()

    # --- 集計 CSV を出力 ---
    with open(summary_csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"[INFO] 集計CSV: {summary_csv_path}", file=sys.stderr)

    if failed:
        print(
            f"[WARN] 失敗したレース ({len(failed)}/{len(race_keys)}): {', '.join(failed)}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
