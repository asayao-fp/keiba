"""
train_place_model.py
====================
学習データ CSV を読み込んで CatBoostClassifier で is_place モデルを学習する。

使用例:
  python scripts/train_place_model.py --train-csv data/place_train.csv --model-out models/place_model.cbm
"""

import argparse
import os
import sys

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split


DEFAULT_TRAIN_CSV = "data/place_train.csv"
DEFAULT_MODEL_OUT = "models/place_model.cbm"

CATEGORICAL_FEATURES = ["jockey_code", "trainer_code", "course_code", "grade_code", "track_code", "surface"]
NUMERIC_FEATURES = [
    "body_weight",
    "handicap_weight_x10",
    "distance_m",
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
]
FEATURE_COLS = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COL = "is_place"

# 識別用列 (特徴量に含めない)
ID_COLS = ["race_key", "entry_key", "horse_id", "horse_no", "yyyymmdd"]

# 通過順特徴量は NULL を許容するため dropna 対象から除外
PASSING_FEATURE_COLS = [
    "avg_pos_1c_last3",
    "avg_pos_4c_last3",
    "avg_gain_last3",
    "front_rate_last3",
    "avg_pos_1c_pct_last3",
    "avg_pos_4c_pct_last3",
    "n_past",
]
REQUIRED_FEATURE_COLS = [c for c in FEATURE_COLS if c not in PASSING_FEATURE_COLS]


def parse_args():
    parser = argparse.ArgumentParser(
        description="is_place 予測モデルを CatBoost で学習する"
    )
    parser.add_argument(
        "--train-csv",
        default=DEFAULT_TRAIN_CSV,
        metavar="PATH",
        help=f"学習データ CSV パス (デフォルト: {DEFAULT_TRAIN_CSV})",
    )
    parser.add_argument(
        "--model-out",
        default=DEFAULT_MODEL_OUT,
        metavar="PATH",
        help=f"モデル出力パス (デフォルト: {DEFAULT_MODEL_OUT})",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=3,
        metavar="K",
        help="Top-K for precision@k / hit_rate@k (デフォルト: 3)",
    )
    parser.add_argument(
        "--split",
        choices=["chrono", "random"],
        default="chrono",
        help="train/val 分割方法: chrono=時系列順, random=ランダム (デフォルト: chrono)",
    )
    parser.add_argument(
        "--val-ratio",
        type=float,
        default=0.2,
        metavar="RATIO",
        help="val セットの割合 (デフォルト: 0.2)。--val-from 指定時は無視される",
    )
    parser.add_argument(
        "--val-from",
        default=None,
        metavar="YYYYMMDD",
        help="val セット開始日 (例: 20230101)。指定時は --val-ratio を無視して日付でカット",
    )
    return parser.parse_args()


def _get_race_dates(df):
    """race_key ごとの日付マッピングを返す。

    1. ``race_key`` の先頭 8 桁 (YYYYMMDD) を優先。
    2. ``yyyymmdd`` 列を次に試みる。
    3. いずれも失敗したら ``(None, None)`` を返す。

    日付が取得できないレース (全行 NaT) は警告付きで除外する。
    複数行あるレースは最小 (min) の非 null 日付を採用する。

    Returns
    -------
    race_date_map : dict[str, pd.Timestamp] or None
        race_key -> Timestamp マッピング。
    source : str or None
        使用した列名の説明文字列。
    """
    if "race_key" not in df.columns:
        return None, None

    race_keys = df["race_key"].astype(str)

    # Try race_key prefix (YYYYMMDD)
    dates = pd.to_datetime(race_keys.str[:8], format="%Y%m%d", errors="coerce")
    source = "race_key prefix"

    if not dates.notna().any():
        # Fall back to yyyymmdd column
        if "yyyymmdd" not in df.columns:
            return None, None
        dates = pd.to_datetime(df["yyyymmdd"].astype(str), format="%Y%m%d", errors="coerce")
        source = "yyyymmdd"
        if not dates.notna().any():
            return None, None

    # Per-race date: min non-null date
    race_date_series = (
        pd.DataFrame({"race_key": race_keys, "date": dates})
        .groupby("race_key")["date"]
        .min()
    )

    # Drop races with no valid date
    null_races = race_date_series[race_date_series.isna()]
    if not null_races.empty:
        print(f"[WARN] 日付が取得できないレースを除外します: {len(null_races)} 件")
        race_date_series = race_date_series.dropna()

    if race_date_series.empty:
        return None, None

    return race_date_series.to_dict(), source


def _chrono_split_indices(df, race_date_map, val_ratio=0.2, val_from=None):
    """時系列順の train/val インデックスをレース単位で返す。

    レース単位で分割することで train と val に同じ race_key が混在しない
    ことを保証する。

    Parameters
    ----------
    df : pd.DataFrame
    race_date_map : dict[str, pd.Timestamp]
        race_key -> Timestamp マッピング。
    val_ratio : float
        ``val_from`` 未指定時に val セットとする末尾のレース割合。
    val_from : str or None
        ``"YYYYMMDD"`` 形式の文字列。指定時はこの日以降のレースを val とする。

    Returns
    -------
    train_idx, val_idx : pd.Index
    cutoff_date : pd.Timestamp
    train_race_keys : set
    val_race_keys : set
    """
    sorted_races = sorted(race_date_map.items(), key=lambda x: x[1])
    sorted_race_keys = [rk for rk, _ in sorted_races]
    sorted_dates = [d for _, d in sorted_races]

    if val_from is not None:
        cutoff_date = pd.to_datetime(val_from, format="%Y%m%d")
        val_race_keys = {rk for rk, d in sorted_races if d >= cutoff_date}
    else:
        n_races = len(sorted_race_keys)
        cutoff_pos = int(n_races * (1 - val_ratio))
        # cutoff_pos が境界を超えないようにクリップ
        cutoff_pos = max(0, min(cutoff_pos, n_races - 1))
        cutoff_date = sorted_dates[cutoff_pos]
        val_race_keys = set(sorted_race_keys[cutoff_pos:])

    train_race_keys = set(race_date_map.keys()) - val_race_keys

    race_key_series = df["race_key"].astype(str)
    train_idx = df.index[race_key_series.isin(train_race_keys)]
    val_idx = df.index[race_key_series.isin(val_race_keys)]
    return train_idx, val_idx, cutoff_date, train_race_keys, val_race_keys


def compute_topk_metrics(race_keys, y_true, y_proba, k=3):
    """各レースで上位 k 頭を選んだときの精度指標を返す。

    Returns
    -------
    precision_at_1 : float
    precision_at_k : float
    hit_rate_at_k  : float
    n_races        : int
    """
    val_df = pd.DataFrame(
        {
            "race_key": race_keys.values,
            "is_place": y_true.values,
            "p_place": y_proba,
        }
    ).dropna(subset=["race_key", "is_place", "p_place"])

    precision_at_1_list = []
    precision_at_k_list = []
    hit_rate_at_k_list = []

    for _, grp in val_df.groupby("race_key"):
        grp_sorted = grp.sort_values("p_place", ascending=False)

        top1 = grp_sorted.iloc[:1]
        precision_at_1_list.append(float(top1["is_place"].mean()))

        actual_k = min(k, len(grp_sorted))
        topk = grp_sorted.iloc[:actual_k]
        precision_at_k_list.append(float(topk["is_place"].mean()))
        hit_rate_at_k_list.append(int(topk["is_place"].sum() > 0))

    n_races = len(precision_at_1_list)
    if n_races == 0:
        return float("nan"), float("nan"), float("nan"), 0

    precision_at_1 = sum(precision_at_1_list) / n_races
    precision_at_k = sum(precision_at_k_list) / n_races
    hit_rate_at_k = sum(hit_rate_at_k_list) / n_races
    return precision_at_1, precision_at_k, hit_rate_at_k, n_races


def main():
    args = parse_args()

    print(f"[INFO] 学習データ読み込み: {args.train_csv}")
    try:
        df = pd.read_csv(args.train_csv, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] ファイルが見つかりません: {args.train_csv}", file=sys.stderr)
        sys.exit(1)

    # 数値列を変換
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # is_place を整数に変換
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce")

    # 欠損を除外 (通過順特徴量は任意のため除外対象に含めない)
    required = REQUIRED_FEATURE_COLS + [TARGET_COL]
    before = len(df)
    df = df.dropna(subset=required)
    after = len(df)
    if before != after:
        print(f"[INFO] 欠損行を除外: {before - after} 件 (残 {after} 件)")

    if df.empty:
        print("[ERROR] 有効な学習データがありません。", file=sys.stderr)
        sys.exit(1)

    X = df[FEATURE_COLS].copy()
    y = df[TARGET_COL].astype(int)

    # カテゴリ列の欠損を空文字で埋める (CatBoost は空文字を許容する)
    for col in CATEGORICAL_FEATURES:
        X[col] = X[col].fillna("")

    # --- train/val 分割 ---
    split_method = args.split
    val_ratio = args.val_ratio
    val_from = args.val_from

    train_idx = val_idx = None

    if split_method == "chrono":
        race_date_map, date_source = _get_race_dates(df)
        if race_date_map is None:
            print(
                "[WARN] race_key / yyyymmdd 列が見つからないか日付解析に失敗しました。"
                " ランダム分割にフォールバックします。"
            )
            split_method = "random"
        else:
            try:
                train_idx, val_idx, cutoff_date, train_race_keys, val_race_keys = _chrono_split_indices(
                    df, race_date_map, val_ratio=val_ratio, val_from=val_from
                )
                if len(train_idx) == 0 or len(val_idx) == 0:
                    print(
                        "[WARN] 時系列分割の結果 train または val が空になりました。"
                        " ランダム分割にフォールバックします。"
                    )
                    split_method = "random"
                    train_idx = val_idx = None
                else:
                    if val_from:
                        print(f"[INFO] 分割方法: chrono (source={date_source}, val_from={val_from})")
                    else:
                        print(f"[INFO] 分割方法: chrono (source={date_source}, val_ratio={val_ratio})")
                    train_dates = [race_date_map[rk] for rk in train_race_keys]
                    val_dates = [race_date_map[rk] for rk in val_race_keys]
                    if train_dates:
                        print(
                            f"[INFO] train 日付範囲: {min(train_dates).date()} ~ {max(train_dates).date()}"
                            f"  ({len(train_idx)} 件, {len(train_race_keys)} レース)"
                        )
                    else:
                        print(f"[INFO] train 日付範囲: (不明)  ({len(train_idx)} 件, {len(train_race_keys)} レース)")
                    if val_dates:
                        print(
                            f"[INFO] val   日付範囲: {min(val_dates).date()} ~ {max(val_dates).date()}"
                            f"  ({len(val_idx)} 件, {len(val_race_keys)} レース)"
                        )
                    else:
                        print(f"[INFO] val   日付範囲: (不明)  ({len(val_idx)} 件, {len(val_race_keys)} レース)")
            except Exception as exc:
                print(f"[WARN] 時系列分割中にエラーが発生しました ({exc})。ランダム分割にフォールバックします。")
                split_method = "random"
                train_idx = val_idx = None

    if split_method == "random":
        print(f"[INFO] 分割方法: random (val_ratio={val_ratio})")
        try:
            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=val_ratio, random_state=42, stratify=y
            )
        except ValueError:
            # クラス数が少ない場合など stratify に失敗したときは stratify なしで分割
            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=val_ratio, random_state=42
            )
    else:
        X_train = X.loc[train_idx]
        X_val = X.loc[val_idx]
        y_train = y.loc[train_idx]
        y_val = y.loc[val_idx]

    print(f"[INFO] train={len(X_train)}, val={len(X_val)}")

    # race_key は特徴量に含まれないが TopK 指標の計算に使う
    has_race_key = "race_key" in df.columns

    cat_feature_indices = [FEATURE_COLS.index(c) for c in CATEGORICAL_FEATURES]

    model = CatBoostClassifier(
        iterations=500,
        learning_rate=0.05,
        depth=6,
        eval_metric="AUC",
        cat_features=cat_feature_indices,
        verbose=100,
        random_seed=42,
    )

    print(f"[INFO] 学習開始 (train={len(X_train)}, val={len(X_val)})")
    model.fit(X_train, y_train, eval_set=(X_val, y_val), use_best_model=True)

    # 評価
    y_pred_proba = model.predict_proba(X_val)[:, 1]
    y_pred = model.predict(X_val)
    auc = roc_auc_score(y_val, y_pred_proba)
    acc = accuracy_score(y_val, y_pred)
    print(f"[INFO] Val AUC: {auc:.4f}  Accuracy: {acc:.4f}")
    print(f"[INFO] Val entries: {len(X_val)}")

    if not has_race_key:
        print("[WARN] race_key 列が見つからないため TopK 指標をスキップします。")
    else:
        race_keys_val = df.loc[X_val.index, "race_key"]
        k = args.topk
        p1, pk, hr, n_races = compute_topk_metrics(race_keys_val, y_val, y_pred_proba, k=k)
        print(f"[INFO] Val races: {n_races}")
        print(f"[INFO] Precision@1: {p1:.4f}")
        print(f"[INFO] Precision@{k}: {pk:.4f}")
        print(f"[INFO] HitRate@{k}: {hr:.4f}")

    out_dir = os.path.dirname(args.model_out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    model.save_model(args.model_out)
    print(f"[INFO] モデルを保存しました: {args.model_out}")


if __name__ == "__main__":
    main()
