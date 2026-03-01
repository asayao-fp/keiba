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
        help="検証分割方法: chrono=時系列分割 (デフォルト), random=ランダム分割",
    )
    parser.add_argument(
        "--val-ratio",
        type=float,
        default=0.2,
        metavar="RATIO",
        help="検証データの割合 (デフォルト: 0.2)",
    )
    parser.add_argument(
        "--val-from",
        default=None,
        metavar="YYYYMMDD",
        help="時系列分割の検証開始日 (例: 20230101)。指定しない場合は val-ratio で自動決定",
    )
    parser.add_argument(
        "--feature-set",
        choices=["race_day", "all"],
        default="race_day",
        help="特徴量セット: race_day=当日入力可能な特徴量のみ (デフォルト), all=全利用可能特徴量",
    )
    return parser.parse_args()


def resolve_date_series(df: pd.DataFrame) -> pd.Series | None:
    """データフレームから日付 Series (YYYYMMDD 文字列) を取得して返す。

    優先順位:
    1. race_date 列
    2. yyyymmdd 列
    3. race_key 先頭 8 桁
    見つからない場合は None を返す。
    """
    for col in ("race_date", "yyyymmdd"):
        if col in df.columns:
            return df[col].astype(str)
    if "race_key" in df.columns:
        derived = df["race_key"].astype(str).str[:8]
        if derived.str.match(r"^\d{8}$").fillna(False).all():
            return derived
    return None


def chrono_split(
    df: pd.DataFrame,
    X: pd.DataFrame,
    y: pd.Series,
    val_ratio: float = 0.2,
    val_from: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """時系列順で学習/検証データを分割する。

    日付列が取得できない場合はランダム分割にフォールバックする。

    Parameters
    ----------
    df : 元データフレーム (日付列の参照に使用)
    X  : 特徴量
    y  : 目的変数
    val_ratio : 検証データの割合 (val_from 未指定時に使用)
    val_from  : 検証開始日 (YYYYMMDD)。指定時は val_ratio を無視する。
    """
    date_col = resolve_date_series(df)
    if date_col is None:
        print(
            "[WARN] 日付列 (race_date / yyyymmdd / race_key) が見つからないため"
            " ランダム分割にフォールバックします。",
            file=sys.stderr,
        )
        return train_test_split(X, y, test_size=val_ratio, random_state=42, stratify=y)

    if val_from:
        cutoff = val_from
    else:
        sorted_dates = date_col.sort_values()
        n = len(sorted_dates)
        cutoff_idx = int(n * (1 - val_ratio))
        cutoff = sorted_dates.iloc[cutoff_idx]

    train_mask = (date_col < cutoff).values
    val_mask = (date_col >= cutoff).values

    if train_mask.sum() == 0 or val_mask.sum() == 0:
        print(
            f"[WARN] 時系列分割 (cutoff={cutoff}) で一方のデータが空になるため"
            " ランダム分割にフォールバックします。",
            file=sys.stderr,
        )
        return train_test_split(X, y, test_size=val_ratio, random_state=42, stratify=y)

    print(f"[INFO] 時系列分割: cutoff={cutoff}  train={train_mask.sum()}, val={val_mask.sum()}")
    return X[train_mask], X[val_mask], y[train_mask], y[val_mask]


def build_feature_cols(df: pd.DataFrame, feature_set: str) -> tuple[list[str], list[str], list[str]]:
    """特徴量列リストを構築して (feature_cols, numeric_cols, categorical_cols) を返す。

    feature_set:
      race_day - デフォルト。当日入力可能な特徴量 + CSV に odds 列があれば追加。
      all      - race_day に加え、CSV に存在するその他の数値列を追加。
    """
    num_cols = list(NUMERIC_FEATURES)
    cat_cols = list(CATEGORICAL_FEATURES)

    # odds 列が CSV に存在する場合は条件なく追加
    if "odds" in df.columns:
        if "odds" not in num_cols:
            num_cols.append("odds")

    if feature_set == "all":
        # CSV に存在し、かつ ID 列・目的変数でない数値列をすべて追加
        exclude = set(ID_COLS) | {TARGET_COL} | set(num_cols) | set(cat_cols)
        for col in df.columns:
            if col in exclude:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                num_cols.append(col)

    feat_cols = num_cols + cat_cols
    print(f"[INFO] 使用特徴量 ({feature_set}): {feat_cols}")
    return feat_cols, num_cols, cat_cols


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

    # 特徴量列を決定 (odds の有無や --feature-set を考慮)
    feature_cols, numeric_features, categorical_features = build_feature_cols(df, args.feature_set)

    # 数値列を変換
    for col in numeric_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # is_place を整数に変換
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce")

    # 欠損を除外 (通過順特徴量・odds は任意のため除外対象に含めない)
    optional_cols = set(PASSING_FEATURE_COLS) | {"odds"}
    required = [c for c in feature_cols if c not in optional_cols] + [TARGET_COL]
    # 存在する列のみ対象にする
    required = [c for c in required if c in df.columns]
    before = len(df)
    df = df.dropna(subset=required)
    after = len(df)
    if before != after:
        print(f"[INFO] 欠損行を除外: {before - after} 件 (残 {after} 件)")

    if df.empty:
        print("[ERROR] 有効な学習データがありません。", file=sys.stderr)
        sys.exit(1)

    # feature_cols のうち実際に df に存在する列だけを使う
    feature_cols = [c for c in feature_cols if c in df.columns]
    categorical_features = [c for c in categorical_features if c in feature_cols]
    numeric_features = [c for c in numeric_features if c in feature_cols]

    X = df[feature_cols].copy()
    y = df[TARGET_COL].astype(int)

    # カテゴリ列の欠損を空文字で埋める (CatBoost は空文字を許容する)
    for col in categorical_features:
        X[col] = X[col].fillna("")

    # 分割方法に応じて学習/検証データを分割
    if args.split == "chrono":
        X_train, X_val, y_train, y_val = chrono_split(
            df, X, y, val_ratio=args.val_ratio, val_from=args.val_from
        )
    else:
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=args.val_ratio, random_state=42, stratify=y
        )

    # race_key は特徴量に含まれないが TopK 指標の計算に使う
    has_race_key = "race_key" in df.columns

    cat_feature_indices = [feature_cols.index(c) for c in categorical_features]

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
