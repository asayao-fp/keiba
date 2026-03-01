"""
gui/main.py
===========
JV-Link 更新 & 複勝買い目提案 GUI (PySide6)

起動方法:
  # 64-bit venv を有効化してから
  python gui/main.py
"""

import csv
import datetime
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Callable

from PySide6.QtCore import QDate, QProcess, QStringListModel, Qt, QUrl
from PySide6.QtGui import QBrush, QCloseEvent, QColor, QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QCompleter,
    QDateEdit,
    QDialog,
    QFileDialog,
    QFormLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLayout,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

logger = logging.getLogger(__name__)

# リポジトリルート (gui/ の親ディレクトリ)
REPO_ROOT = Path(__file__).resolve().parent.parent

# scripts/ ディレクトリの絶対パス
SCRIPTS_DIR = REPO_ROOT / "scripts"

# 再学習スクリプトの存在チェック
_PLACE_BUILD_SCRIPT = SCRIPTS_DIR / "build_place_training_data.py"
_PLACE_TRAIN_SCRIPT = SCRIPTS_DIR / "train_place_model.py"
_PLACE_RA7_SCRIPT = SCRIPTS_DIR / "build_race_passing_positions_from_ra7.py"
_PLACE_PASSING_FEATURES_SCRIPT = SCRIPTS_DIR / "build_horse_past_passing_features.py"
_PLACE_RETRAIN_AVAILABLE = (
    _PLACE_BUILD_SCRIPT.exists()
    and _PLACE_TRAIN_SCRIPT.exists()
    and _PLACE_RA7_SCRIPT.exists()
    and _PLACE_PASSING_FEATURES_SCRIPT.exists()
)

_WIDE_BUILD_SCRIPT = SCRIPTS_DIR / "build_wide_training_data.py"
_WIDE_TRAIN_SCRIPT = SCRIPTS_DIR / "train_wide_model.py"
_WIDE_RETRAIN_AVAILABLE = _WIDE_BUILD_SCRIPT.exists() and _WIDE_TRAIN_SCRIPT.exists()

_SANRENPUKU_BUILD_SCRIPT = SCRIPTS_DIR / "build_sanrenpuku_training_data.py"
_SANRENPUKU_TRAIN_SCRIPT = SCRIPTS_DIR / "train_sanrenpuku_model.py"
_SANRENPUKU_RETRAIN_AVAILABLE = _SANRENPUKU_BUILD_SCRIPT.exists() and _SANRENPUKU_TRAIN_SCRIPT.exists()

# 複勝パイプライン (一括) スクリプト
_PLACE_PIPELINE_BUILD_SCRIPT = SCRIPTS_DIR / "build_place_training_data.py"
_PLACE_PIPELINE_SPLIT_SCRIPT = SCRIPTS_DIR / "split_labeled_unlabeled_csv.py"
_PLACE_PIPELINE_TRAIN_SCRIPT = SCRIPTS_DIR / "train_place_model_lgbm.py"
_PLACE_PIPELINE_PREDICT_SCRIPT = SCRIPTS_DIR / "predict_place_model_lgbm.py"
_PLACE_PIPELINE_RECOMMEND_SCRIPT = SCRIPTS_DIR / "make_place_recommendations_rich.py"
_PLACE_PIPELINE_AVAILABLE = (
    _PLACE_PIPELINE_BUILD_SCRIPT.exists()
    and _PLACE_PIPELINE_SPLIT_SCRIPT.exists()
    and _PLACE_PIPELINE_TRAIN_SCRIPT.exists()
    and _PLACE_PIPELINE_PREDICT_SCRIPT.exists()
    and _PLACE_PIPELINE_RECOMMEND_SCRIPT.exists()
)

# 複勝 推奨生成 (一括) GUI セクションの表示/非表示フラグ
# False に設定すると、当該セクションは GUI に追加されません。
_ENABLE_PLACE_PIPELINE_GUI = False

# scripts/ を sys.path に追加して fetch_races をインポート
sys.path.insert(0, str(SCRIPTS_DIR))
from list_races import fetch_races  # noqa: E402
try:
    from predict_place import (  # noqa: E402
        CATEGORICAL_FEATURES as _PRED_CAT_FEATS,
        FEATURE_COLS as _PRED_FEAT_COLS,
        NUMERIC_FEATURES as _PRED_NUM_FEATS,
    )
except ImportError:
    _PRED_CAT_FEATS = _PRED_FEAT_COLS = _PRED_NUM_FEATS = None  # type: ignore[assignment]

# 設定ファイルのパス
CONFIG_PATH = REPO_ROOT / ".keiba_gui_config.json"

# スナップショット保存ディレクトリ (ユーザーホーム下)
PRESETS_DIR = Path.home() / ".keiba" / "presets"

# スナップショットファイルのパターン: YYYYMMDD_HHMMSS_<kind>.json
_SNAPSHOT_RE = re.compile(r"^\d{8}_\d{6}_.+\.json$")
# 保持するスナップショットの最大件数
_MAX_SNAPSHOTS = 10

# レース選択テーブルの列ヘッダー
_RACE_TABLE_COLS = ["✓", "レース名", "競馬場", "R", "距離", "馬場", "グレード", "race_key"]

# 予想結果テーブルの列ヘッダー
_SUMMARY_TABLE_COLS = ["レース名", "競馬場", "R", "S", "買い目数", "賭金計", "期待値計", "avg p", "F/B", "race_key"]
_BETS_TABLE_COLS = ["馬番", "賭金", "p_place", "オッズ使用", "期待値(円)", "EV/1unit"]
_PRED_TABLE_COLS = ["順位", "馬番", "馬名", "騎手名", "調教師名", "p_place", "着順", "複勝圏", "TP"]

# 真陽性ハイライト色 (予測上位かつ複勝圏的中)
_TP_HIGHLIGHT_COLOR = QColor("#c8f5c8")
# フォールバック順位値 (rank が未設定の場合に使用)
_RANK_FALLBACK = 9999

# 組み合わせ予測結果テーブルの列ヘッダー
_WIDE_TABLE_COLS = ["順位", "race_key", "馬番A", "馬番B", "p_wide"]
_SANRENPUKU_TABLE_COLS = ["順位", "race_key", "馬番A", "馬番B", "馬番C", "p_sanrenpuku"]
_PLACE_RECO_TABLE_COLS = [
    "race_date", "course_code", "race_no", "distance_m", "surface",
    "grade_code", "race_name_short", "rank_in_race", "horse_no",
    "horse_name", "jockey_name_short", "trainer_name_short", "pred_is_place_proba",
]

# 手動予測セクションの定数
_TRACK_CONDITION_MAP = {"良": "1", "稍重": "2", "重": "3", "不良": "4"}
_MANUAL_ENTRY_COLS = ["馬番", "馬名", "騎手", "調教師", "斤量(kg)", "馬体重(kg)"]
_MANUAL_COL_HORSE_NO = 0
_MANUAL_COL_HORSE = 1
_MANUAL_COL_JOCKEY = 2
_MANUAL_COL_TRAINER = 3
_MANUAL_COL_HANDICAP = 4
_MANUAL_COL_BODY_WEIGHT = 5

# 距離プリセット (m)
_DISTANCE_PRESETS = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2500, 3000, 3200, 3400, 3600]

# 馬場種別オプション
_SURFACE_OPTIONS = ["", "芝", "ダート", "サンド", "障害", "不明"]

# 馬番ドロップダウンの最大値
_HORSE_NO_MAX = 20

# モデルが持つ可能性のある馬場種別特徴量列名
_KNOWN_SURFACE_COL_NAMES = {"surface", "surface_code", "track_surface"}

# JRA 競馬場コード一覧 (表示名, コード)
_COURSE_CODES: list[tuple[str, str]] = [
    ("札幌 (01)", "01"),
    ("函館 (02)", "02"),
    ("福島 (03)", "03"),
    ("新潟 (04)", "04"),
    ("東京 (05)", "05"),
    ("中山 (06)", "06"),
    ("中京 (07)", "07"),
    ("京都 (08)", "08"),
    ("阪神 (09)", "09"),
    ("小倉 (10)", "10"),
]


def _script(name: str) -> str:
    return str(SCRIPTS_DIR / name)


# ── Python32 自動検出 ─────────────────────────────────────────────────────────

_PY_LAUNCHER_VERSIONS = ["-3.11-32", "-3.10-32", "-3.9-32", "-3.8-32"]

_COMMON_PY32_PATHS = [
    r"C:\Python311-32\python.exe",
    r"C:\Python310-32\python.exe",
    r"C:\Python39-32\python.exe",
    r"C:\Python38-32\python.exe",
    r"C:\Python311\python.exe",
    r"C:\Python310\python.exe",
]


def _is_32bit_python(program: str, extra_args: list[str] | None = None) -> bool:
    """program (+ extra_args) で Python を起動し 32-bit かどうかを確認する。"""
    args = (extra_args or []) + ["-c", "import platform; print(platform.architecture()[0])"]
    try:
        result = subprocess.run(
            [program] + args,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0 and "32bit" in result.stdout
    except Exception:
        return False


def detect_python32() -> list[str] | None:
    """32-bit Python の起動コマンドを検出して返す。見つからない場合は None。

    戻り値は QProcess.start() の [program, *arguments] 形式のリスト。
    """
    # a) py ランチャー
    for ver in _PY_LAUNCHER_VERSIONS:
        if _is_32bit_python("py", [ver]):
            return ["py", ver]

    # b) 共通インストールパス
    local_app = os.environ.get("LOCALAPPDATA", "")
    extra_paths = []
    if local_app:
        for sub in ["Python311-32", "Python310-32", "Python39-32", "Python38-32"]:
            extra_paths.append(os.path.join(local_app, "Programs", "Python", sub, "python.exe"))

    for path in _COMMON_PY32_PATHS + extra_paths:
        if os.path.isfile(path) and _is_32bit_python(path):
            return [path]

    return None


def py32_to_display(cmd: list[str]) -> str:
    """[program, *args] → 表示用文字列 (スペース区切り)"""
    return " ".join(cmd)


def display_to_py32(text: str) -> list[str]:
    """表示用文字列 → [program, *args]"""
    return text.strip().split() if text.strip() else []


# ── 設定の読み書き ────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_config(data: dict) -> None:
    try:
        CONFIG_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# ── 折り畳みコンテナウィジェット ─────────────────────────────────────────────

class CollapsibleBox(QWidget):
    """クリックで展開/折り畳みができるセクションウィジェット。"""

    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._collapsed = False

        self._toggle_btn = QPushButton()
        self._toggle_btn.setCheckable(False)
        self._toggle_btn.setStyleSheet(
            "QPushButton { text-align: left; border: none;"
            " background: palette(button); padding: 4px 6px; font-weight: bold; }"
            "QPushButton:hover { background: palette(midlight); }"
        )
        self._toggle_btn.clicked.connect(self.toggle)

        self._content = QWidget()

        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._toggle_btn)
        layout.addWidget(self._content)

        self._title = title
        self._update_label()

    def _update_label(self) -> None:
        arrow = "▶" if self._collapsed else "▼"
        self._toggle_btn.setText(f"{arrow}  {self._title}")

    def setContentLayout(self, content_layout: QLayout) -> None:
        """コンテンツ領域にレイアウトをセットする。"""
        self._content.setLayout(content_layout)

    def toggle(self) -> None:
        self.setCollapsed(not self._collapsed)

    def isCollapsed(self) -> bool:
        return self._collapsed

    def setCollapsed(self, collapsed: bool) -> None:
        if self._collapsed == collapsed:
            return
        self._collapsed = collapsed
        self._content.setVisible(not self._collapsed)
        self._update_label()



# ── タイプアヘッド補完ウィジェット ────────────────────────────────────────────

class _MasterLineEdit(QLineEdit):
    """QLineEdit with type-ahead QCompleter for master data (horse / jockey / trainer).

    items: list of (display_text, code_or_id) pairs.
    As the user types, a popup shows matching entries (case-insensitive substring match).
    selected_code() returns the backing code/ID for the current text, or the raw text
    when no match is found (free-text fallback).
    """

    def __init__(
        self,
        items: list[tuple[str, str]],
        placeholder: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._value_map: dict[str, str] = {}
        display_names: list[str] = []
        for display, code in items:
            display_names.append(display)
            if display not in self._value_map:
                self._value_map[display] = code

        string_model = QStringListModel(display_names, self)
        completer = QCompleter(string_model, self)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        completer.setFilterMode(Qt.MatchFlag.MatchContains)
        completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.setCompleter(completer)

        if placeholder:
            self.setPlaceholderText(placeholder)

    def selected_code(self) -> str:
        """Return the backing code/ID for the current display text, or the raw text."""
        text = self.text().strip()
        return self._value_map.get(text, text)


# ── メインウィンドウ ──────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Keiba Pipeline GUI")
        self.resize(740, 720)

        # 実行中プロセス管理
        self._processes: list[QProcess] = []
        self._cancelled = False

        # 手動予測セクション用マスタデータキャッシュ
        self._manual_horse_data: list[tuple[str, str]] = []
        self._manual_jockey_data: list[tuple[str, str]] = []
        self._manual_trainer_data: list[tuple[str, str]] = []
        self._manual_horses_available = False
        self._manual_jockeys_available = False
        self._manual_trainers_available = False

        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        # ── 設定フォーム ──────────────────────────────
        self._settings_box = CollapsibleBox("設定")
        form = QFormLayout()

        self.db_edit = QLineEdit()
        self.db_edit.setPlaceholderText("jv_data.db")
        form.addRow("DB パス:", self._with_browse(self.db_edit, file=True))

        # カレンダーポップアップ付き日付選択
        self.date_edit = QDateEdit()
        self.date_edit.setDisplayFormat("yyyyMMdd")
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDate(QDate.currentDate())
        form.addRow("取込開始日:", self.date_edit)

        self.outdir_edit = QLineEdit()
        self.outdir_edit.setPlaceholderText("出力ディレクトリを選択")
        form.addRow("出力ディレクトリ:", self._with_browse(self.outdir_edit, file=False))

        self.model_edit = QLineEdit()
        self.model_edit.setPlaceholderText("models/place_model.cbm")
        form.addRow("モデルパス:", self._with_browse(self.model_edit, file=True))

        self.py32_edit = QLineEdit()
        self.py32_edit.setPlaceholderText("32-bit python.exe (自動検出 or 選択)")
        self.detect_btn = QPushButton("検出")
        self.detect_btn.setFixedWidth(52)
        self.detect_btn.clicked.connect(self._on_detect_python32)
        form.addRow("32-bit Python:", self._with_browse_extra(
            self.py32_edit, file=True, extra_btn=self.detect_btn
        ))

        self.racekeys_edit = QLineEdit()
        self.racekeys_edit.setPlaceholderText(
            "レースキー (スペース区切り) — テーブル未選択時の手動入力"
        )
        form.addRow("レースキー (手動):", self.racekeys_edit)

        load_snapshot_btn = QPushButton("履歴から読み込む")
        load_snapshot_btn.clicked.connect(self._on_load_snapshot)
        form.addRow("スナップショット:", load_snapshot_btn)

        self._settings_box.setContentLayout(form)
        root_layout.addWidget(self._settings_box)

        # ── レース選択 ──────────────────────────────────
        self._races_box = CollapsibleBox("レース選択")
        races_layout = QVBoxLayout()

        # 1行目: place_odds チェックボックス + 重賞読み込みボタン
        races_ctrl = QHBoxLayout()
        self.place_odds_chk = QCheckBox("place_odds のみ")
        self.place_odds_chk.setChecked(True)
        races_ctrl.addWidget(self.place_odds_chk)
        races_ctrl.addStretch()
        self.load_races_btn = QPushButton("重賞レースを読み込む")
        self.load_races_btn.clicked.connect(self._on_load_graded_races)
        races_ctrl.addWidget(self.load_races_btn)
        races_layout.addLayout(races_ctrl)

        # 2行目: キーワード検索 + 週末メインレース + 検索ボタン
        search_ctrl = QHBoxLayout()
        search_ctrl.addWidget(QLabel("キーワード:"))
        self.keyword_edit = QLineEdit()
        self.keyword_edit.setPlaceholderText("レース名で検索 (部分一致)")
        self.keyword_edit.returnPressed.connect(self._on_search_races)
        search_ctrl.addWidget(self.keyword_edit)
        self.weekend_chk = QCheckBox("週末メイン (R≥10)")
        search_ctrl.addWidget(self.weekend_chk)
        self.search_races_btn = QPushButton("検索")
        self.search_races_btn.clicked.connect(self._on_search_races)
        search_ctrl.addWidget(self.search_races_btn)
        races_layout.addLayout(search_ctrl)

        # レーステーブル
        self.races_table = QTableWidget(0, len(_RACE_TABLE_COLS))
        self.races_table.setHorizontalHeaderLabels(_RACE_TABLE_COLS)
        self.races_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Stretch
        )
        self.races_table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.races_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.races_table.verticalHeader().setVisible(False)
        self.races_table.setMinimumHeight(120)
        races_layout.addWidget(self.races_table)

        # 下部: 選択したレースをキーに設定するボタン
        use_sel_btn = QPushButton("選択したレースをキーに設定")
        use_sel_btn.clicked.connect(self._on_use_selected_races)
        races_layout.addWidget(use_sel_btn, alignment=Qt.AlignmentFlag.AlignRight)

        self._races_box.setContentLayout(races_layout)
        root_layout.addWidget(self._races_box)

        # ── ボタン ────────────────────────────────────
        btn_layout = QHBoxLayout()

        self.update_btn = QPushButton("Update (RACE)")
        self.update_btn.setMinimumHeight(36)
        self.update_btn.clicked.connect(self._on_update)
        btn_layout.addWidget(self.update_btn)

        self.suggest_btn = QPushButton("Suggest")
        self.suggest_btn.setMinimumHeight(36)
        self.suggest_btn.clicked.connect(self._on_suggest)
        btn_layout.addWidget(self.suggest_btn)

        self.update_suggest_btn = QPushButton("Update + Suggest")
        self.update_suggest_btn.setMinimumHeight(36)
        self.update_suggest_btn.clicked.connect(self._on_update_suggest)
        btn_layout.addWidget(self.update_suggest_btn)

        self.cancel_btn = QPushButton("キャンセル")
        self.cancel_btn.setMinimumHeight(36)
        self.cancel_btn.setEnabled(False)
        self.cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(self.cancel_btn)

        root_layout.addLayout(btn_layout)

        # ── 当日入力（手動予測）────────────────────────────────
        self._manual_box = CollapsibleBox("当日入力（手動予測）")
        manual_layout = QVBoxLayout()

        # レース条件入力フォーム
        manual_race_form = QFormLayout()

        self.manual_course_combo = QComboBox()
        self.manual_course_combo.setEditable(True)
        for label, code in _COURSE_CODES:
            self.manual_course_combo.addItem(label, code)
        self.manual_course_combo.lineEdit().setPlaceholderText("例: 05 (東京), 06 (中山)")
        manual_race_form.addRow("競馬場コード *:", self.manual_course_combo)

        self.manual_distance_spin = QSpinBox()
        self.manual_distance_spin.setRange(0, 9999)
        self.manual_distance_spin.setValue(0)
        self.manual_distance_spin.setSuffix(" m")
        self.manual_distance_preset_combo = QComboBox()
        self.manual_distance_preset_combo.addItem("選択")
        for _d in _DISTANCE_PRESETS:
            self.manual_distance_preset_combo.addItem(str(_d))
        self.manual_distance_preset_combo.currentIndexChanged.connect(
            self._on_distance_preset_changed
        )
        _dist_row = QHBoxLayout()
        _dist_row.setContentsMargins(0, 0, 0, 0)
        _dist_row.addWidget(self.manual_distance_spin)
        _dist_row.addWidget(self.manual_distance_preset_combo)
        manual_race_form.addRow("距離 * (m):", _dist_row)

        self.manual_track_combo = QComboBox()
        self.manual_track_combo.addItems(list(_TRACK_CONDITION_MAP.keys()))
        manual_race_form.addRow("馬場状態 *:", self.manual_track_combo)

        self.manual_surface_combo = QComboBox()
        self.manual_surface_combo.addItems(_SURFACE_OPTIONS)
        manual_race_form.addRow("馬場種別 (芝/ダート):", self.manual_surface_combo)

        self.manual_grade_edit = QLineEdit()
        self.manual_grade_edit.setPlaceholderText("任意 (例: A, B, C, 15)")
        manual_race_form.addRow("グレードコード:", self.manual_grade_edit)

        manual_layout.addLayout(manual_race_form)

        # 出走馬テーブルのコントロール行
        manual_entries_ctrl = QHBoxLayout()
        manual_entries_ctrl.addWidget(QLabel("出走馬 (* = 必須):"))
        manual_entries_ctrl.addStretch()
        manual_load_masters_btn = QPushButton("マスタ読み込み")
        manual_load_masters_btn.setToolTip("DB から馬・騎手・調教師マスタを読み込んでドロップダウンを更新します")
        manual_load_masters_btn.clicked.connect(self._on_manual_load_masters)
        manual_entries_ctrl.addWidget(manual_load_masters_btn)
        manual_entries_ctrl.addWidget(QLabel("出走頭数:"))
        self.manual_nhorses_spin = QSpinBox()
        self.manual_nhorses_spin.setRange(1, 18)
        self.manual_nhorses_spin.setValue(18)
        self.manual_nhorses_spin.setFixedWidth(52)
        manual_entries_ctrl.addWidget(self.manual_nhorses_spin)
        manual_gen_rows_btn = QPushButton("行生成")
        manual_gen_rows_btn.setToolTip("指定した頭数で行をリセットし、馬番を 1..N で自動入力します")
        manual_gen_rows_btn.clicked.connect(self._on_manual_gen_rows)
        manual_entries_ctrl.addWidget(manual_gen_rows_btn)
        manual_add_row_btn = QPushButton("行追加")
        manual_add_row_btn.clicked.connect(self._on_manual_add_row)
        manual_entries_ctrl.addWidget(manual_add_row_btn)
        manual_remove_row_btn = QPushButton("行削除")
        manual_remove_row_btn.clicked.connect(self._on_manual_remove_row)
        manual_entries_ctrl.addWidget(manual_remove_row_btn)
        manual_layout.addLayout(manual_entries_ctrl)

        # 出走馬テーブル
        self.manual_table = QTableWidget(0, len(_MANUAL_ENTRY_COLS))
        self.manual_table.setHorizontalHeaderLabels(_MANUAL_ENTRY_COLS)
        self.manual_table.horizontalHeader().setSectionResizeMode(
            _MANUAL_COL_HORSE, QHeaderView.ResizeMode.Stretch
        )
        self.manual_table.verticalHeader().setVisible(False)
        self.manual_table.setMinimumHeight(120)
        manual_layout.addWidget(self.manual_table)

        # 予測ボタン
        manual_predict_btn = QPushButton("予測")
        manual_predict_btn.setMinimumHeight(36)
        manual_predict_btn.clicked.connect(self._on_manual_predict)
        manual_layout.addWidget(manual_predict_btn, alignment=Qt.AlignmentFlag.AlignRight)

        self._manual_box.setContentLayout(manual_layout)
        root_layout.addWidget(self._manual_box)

        # ── 再学習 ────────────────────────────────────
        self._retrain_box = CollapsibleBox("再学習")
        retrain_layout = QVBoxLayout()

        retrain_form = QFormLayout()

        self.place_train_csv_edit = QLineEdit()
        self.place_train_csv_edit.setPlaceholderText("data/place_train.csv")
        retrain_form.addRow("複勝学習CSV出力:", self._with_browse(self.place_train_csv_edit, file=True))

        self.place_retrain_model_edit = QLineEdit()
        self.place_retrain_model_edit.setPlaceholderText("models/place_model.cbm")
        retrain_form.addRow("複勝モデル出力:", self._with_browse(self.place_retrain_model_edit, file=True))

        self.wide_train_csv_edit = QLineEdit()
        self.wide_train_csv_edit.setPlaceholderText("data/wide_train.csv")
        retrain_form.addRow("ワイド学習CSV出力:", self._with_browse(self.wide_train_csv_edit, file=True))

        self.wide_retrain_model_edit = QLineEdit()
        self.wide_retrain_model_edit.setPlaceholderText("models/wide_model.cbm")
        retrain_form.addRow("ワイドモデル出力:", self._with_browse(self.wide_retrain_model_edit, file=True))

        self.sanrenpuku_train_csv_edit = QLineEdit()
        self.sanrenpuku_train_csv_edit.setPlaceholderText("data/sanrenpuku_train.csv")
        retrain_form.addRow("3連複学習CSV出力:", self._with_browse(self.sanrenpuku_train_csv_edit, file=True))

        self.sanrenpuku_retrain_model_edit = QLineEdit()
        self.sanrenpuku_retrain_model_edit.setPlaceholderText("models/sanrenpuku_model.cbm")
        retrain_form.addRow("3連複モデル出力:", self._with_browse(self.sanrenpuku_retrain_model_edit, file=True))

        retrain_layout.addLayout(retrain_form)

        retrain_btn_layout = QHBoxLayout()

        self.retrain_place_btn = QPushButton("複勝モデル再学習")
        self.retrain_place_btn.setMinimumHeight(36)
        self.retrain_place_btn.clicked.connect(self._on_retrain_place)
        if not _PLACE_RETRAIN_AVAILABLE:
            self.retrain_place_btn.setEnabled(False)
            self.retrain_place_btn.setToolTip("未対応 (スクリプトが見つかりません)")
        retrain_btn_layout.addWidget(self.retrain_place_btn)

        self.retrain_wide_btn = QPushButton("ワイドモデル再学習")
        self.retrain_wide_btn.setMinimumHeight(36)
        if _WIDE_RETRAIN_AVAILABLE:
            self.retrain_wide_btn.clicked.connect(self._on_retrain_wide)
        else:
            self.retrain_wide_btn.setEnabled(False)
            self.retrain_wide_btn.setToolTip("未対応 (スクリプトが見つかりません)")
        retrain_btn_layout.addWidget(self.retrain_wide_btn)

        self.retrain_sanrenpuku_btn = QPushButton("3連複モデル再学習")
        self.retrain_sanrenpuku_btn.setMinimumHeight(36)
        if _SANRENPUKU_RETRAIN_AVAILABLE:
            self.retrain_sanrenpuku_btn.clicked.connect(self._on_retrain_sanrenpuku)
        else:
            self.retrain_sanrenpuku_btn.setEnabled(False)
            self.retrain_sanrenpuku_btn.setToolTip("未対応 (スクリプトが見つかりません)")
        retrain_btn_layout.addWidget(self.retrain_sanrenpuku_btn)

        self.retrain_all_btn = QPushButton("全部再学習")
        self.retrain_all_btn.setMinimumHeight(36)
        self.retrain_all_btn.clicked.connect(self._on_retrain_all)
        if not _PLACE_RETRAIN_AVAILABLE:
            self.retrain_all_btn.setEnabled(False)
            self.retrain_all_btn.setToolTip("未対応 (スクリプトが見つかりません)")
        retrain_btn_layout.addWidget(self.retrain_all_btn)

        retrain_layout.addLayout(retrain_btn_layout)
        self._retrain_box.setContentLayout(retrain_layout)
        root_layout.addWidget(self._retrain_box)

        # ── 組み合わせ予測 ────────────────────────────
        self._combo_box = CollapsibleBox("組み合わせ予測")
        combo_layout = QVBoxLayout()

        combo_form = QFormLayout()

        self.wide_model_edit = QLineEdit()
        self.wide_model_edit.setPlaceholderText("models/wide_model.cbm")
        combo_form.addRow("ワイドモデルパス:", self._with_browse(self.wide_model_edit, file=True))

        self.sanrenpuku_model_edit = QLineEdit()
        self.sanrenpuku_model_edit.setPlaceholderText("models/sanrenpuku_model.cbm")
        combo_form.addRow("3連複モデルパス:", self._with_browse(self.sanrenpuku_model_edit, file=True))

        self.combo_topn_spin = QSpinBox()
        self.combo_topn_spin.setRange(1, 200)
        self.combo_topn_spin.setValue(10)
        combo_form.addRow("上位 N 件:", self.combo_topn_spin)

        combo_layout.addLayout(combo_form)

        combo_btn_layout = QHBoxLayout()

        self.predict_wide_btn = QPushButton("ワイド予測")
        self.predict_wide_btn.setMinimumHeight(36)
        self.predict_wide_btn.clicked.connect(self._on_predict_wide)
        combo_btn_layout.addWidget(self.predict_wide_btn)

        self.predict_sanrenpuku_btn = QPushButton("3連複予測")
        self.predict_sanrenpuku_btn.setMinimumHeight(36)
        self.predict_sanrenpuku_btn.clicked.connect(self._on_predict_sanrenpuku)
        combo_btn_layout.addWidget(self.predict_sanrenpuku_btn)

        combo_layout.addLayout(combo_btn_layout)

        combo_layout.addWidget(QLabel("ワイド予測結果:"))
        self.wide_table = QTableWidget(0, len(_WIDE_TABLE_COLS))
        self.wide_table.setHorizontalHeaderLabels(_WIDE_TABLE_COLS)
        self.wide_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.wide_table.verticalHeader().setVisible(False)
        self.wide_table.setMinimumHeight(80)
        combo_layout.addWidget(self.wide_table)

        combo_layout.addWidget(QLabel("3連複予測結果:"))
        self.sanrenpuku_table = QTableWidget(0, len(_SANRENPUKU_TABLE_COLS))
        self.sanrenpuku_table.setHorizontalHeaderLabels(_SANRENPUKU_TABLE_COLS)
        self.sanrenpuku_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.sanrenpuku_table.verticalHeader().setVisible(False)
        self.sanrenpuku_table.setMinimumHeight(80)
        combo_layout.addWidget(self.sanrenpuku_table)

        self._combo_box.setContentLayout(combo_layout)
        root_layout.addWidget(self._combo_box)

        # ── 複勝 推奨生成 (一括) ───────────────────────
        if _ENABLE_PLACE_PIPELINE_GUI:
            self._place_pipeline_box = CollapsibleBox("複勝 推奨生成 (一括)")
            place_pipeline_layout = QVBoxLayout()

            place_pipeline_form = QFormLayout()

            # 日付範囲
            self.place_pipeline_from_edit = QLineEdit()
            self.place_pipeline_from_edit.setPlaceholderText("20200101 (空白 = 制限なし)")
            place_pipeline_form.addRow("取得開始日 (From):", self.place_pipeline_from_edit)

            self.place_pipeline_to_edit = QLineEdit()
            self.place_pipeline_to_edit.setPlaceholderText("20231231 (空白 = 制限なし)")
            place_pipeline_form.addRow("取得終了日 (To):", self.place_pipeline_to_edit)

            # TopN
            self.place_pipeline_topn_spin = QSpinBox()
            self.place_pipeline_topn_spin.setRange(1, 200)
            self.place_pipeline_topn_spin.setValue(3)
            place_pipeline_form.addRow("上位 N 件 (TopN):", self.place_pipeline_topn_spin)

            # モデルパス
            self.place_pipeline_model_edit = QLineEdit()
            self.place_pipeline_model_edit.setPlaceholderText("models/place_lgbm.pkl")
            place_pipeline_form.addRow("モデルパス:", self._with_browse(self.place_pipeline_model_edit, file=True))

            # データ出力ディレクトリ
            self.place_pipeline_datadir_edit = QLineEdit()
            self.place_pipeline_datadir_edit.setPlaceholderText("data/")
            place_pipeline_form.addRow("データ出力ディレクトリ:", self._with_browse(self.place_pipeline_datadir_edit, file=False))

            place_pipeline_layout.addLayout(place_pipeline_form)

            # 実行ボタン
            self.place_pipeline_btn = QPushButton("推奨生成 (一括実行)")
            self.place_pipeline_btn.setMinimumHeight(36)
            if _PLACE_PIPELINE_AVAILABLE:
                self.place_pipeline_btn.clicked.connect(self._on_place_pipeline)
            else:
                self.place_pipeline_btn.setEnabled(False)
                missing = [
                    s.name for s in [
                        _PLACE_PIPELINE_BUILD_SCRIPT, _PLACE_PIPELINE_SPLIT_SCRIPT,
                        _PLACE_PIPELINE_TRAIN_SCRIPT, _PLACE_PIPELINE_PREDICT_SCRIPT,
                        _PLACE_PIPELINE_RECOMMEND_SCRIPT,
                    ] if not s.exists()
                ]
                self.place_pipeline_btn.setToolTip(
                    "未対応 (スクリプトが見つかりません: " + ", ".join(missing) + ")"
                )
            place_pipeline_layout.addWidget(self.place_pipeline_btn, alignment=Qt.AlignmentFlag.AlignRight)

            # 推奨結果テーブル
            place_pipeline_layout.addWidget(QLabel("複勝推奨結果:"))
            self.place_reco_table = QTableWidget(0, len(_PLACE_RECO_TABLE_COLS))
            self.place_reco_table.setHorizontalHeaderLabels(_PLACE_RECO_TABLE_COLS)
            self.place_reco_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
            self.place_reco_table.verticalHeader().setVisible(False)
            self.place_reco_table.horizontalHeader().setSectionResizeMode(
                _PLACE_RECO_TABLE_COLS.index("race_name_short"), QHeaderView.ResizeMode.Stretch
            )
            self.place_reco_table.horizontalHeader().setSectionResizeMode(
                _PLACE_RECO_TABLE_COLS.index("horse_name"), QHeaderView.ResizeMode.Stretch
            )
            self.place_reco_table.setMinimumHeight(120)
            place_pipeline_layout.addWidget(self.place_reco_table)

            self._place_pipeline_box.setContentLayout(place_pipeline_layout)
            root_layout.addWidget(self._place_pipeline_box)

        # ── ログ出力 ──────────────────────────────────
        self._log_box = CollapsibleBox("ログ")
        log_layout = QVBoxLayout()
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumBlockCount(5000)
        log_layout.addWidget(self.log_view)

        clear_btn = QPushButton("ログをクリア")
        clear_btn.clicked.connect(self.log_view.clear)
        log_layout.addWidget(clear_btn, alignment=Qt.AlignmentFlag.AlignRight)

        self._log_box.setContentLayout(log_layout)
        root_layout.addWidget(self._log_box)

        # ── 予想結果 ──────────────────────────────────
        self._results_box = CollapsibleBox("予想結果")
        results_layout = QVBoxLayout()

        # コントロール行
        results_ctrl = QHBoxLayout()
        self.refresh_results_btn = QPushButton("結果を更新")
        self.refresh_results_btn.clicked.connect(self._on_refresh_results)
        results_ctrl.addWidget(self.refresh_results_btn)
        open_folder_btn = QPushButton("出力フォルダを開く")
        open_folder_btn.clicked.connect(self._on_open_outdir)
        results_ctrl.addWidget(open_folder_btn)
        results_ctrl.addStretch()
        results_layout.addLayout(results_ctrl)

        # サマリーテーブル
        results_layout.addWidget(QLabel("サマリー (行をクリックすると買い目・予測を表示):"))
        self.summary_table = QTableWidget(0, len(_SUMMARY_TABLE_COLS))
        self.summary_table.setHorizontalHeaderLabels(_SUMMARY_TABLE_COLS)
        self.summary_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch
        )
        self.summary_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.summary_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.summary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.summary_table.verticalHeader().setVisible(False)
        self.summary_table.setMinimumHeight(80)
        self.summary_table.itemSelectionChanged.connect(self._on_summary_row_selected)
        results_layout.addWidget(self.summary_table)

        # レースヘッダーラベル
        self.race_header_label = QLabel("")
        self.race_header_label.setStyleSheet(
            "QLabel { font-weight: bold; padding: 4px 6px;"
            " background: palette(midlight); border-radius: 3px; }"
        )
        self.race_header_label.setVisible(False)
        results_layout.addWidget(self.race_header_label)

        # 買い目コントロール行
        bets_header_row = QHBoxLayout()
        self._bets_label = QLabel("買い目:")
        bets_header_row.addWidget(self._bets_label)
        bets_header_row.addStretch()
        self.toggle_bets_btn = QPushButton("買い目を隠す")
        self.toggle_bets_btn.setCheckable(True)
        self.toggle_bets_btn.setChecked(False)
        self.toggle_bets_btn.clicked.connect(self._on_toggle_bets)
        bets_header_row.addWidget(self.toggle_bets_btn)
        results_layout.addLayout(bets_header_row)

        # 買い目テーブル
        self.bets_table = QTableWidget(0, len(_BETS_TABLE_COLS))
        self.bets_table.setHorizontalHeaderLabels(_BETS_TABLE_COLS)
        self.bets_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.bets_table.verticalHeader().setVisible(False)
        self.bets_table.setMinimumHeight(60)
        results_layout.addWidget(self.bets_table)

        # 予測フィルタ行
        pred_filter_row = QHBoxLayout()
        pred_filter_row.addWidget(QLabel("予測:"))
        pred_filter_row.addStretch()
        pred_filter_row.addWidget(QLabel("上位表示:"))
        self.topn_spin = QSpinBox()
        self.topn_spin.setMinimum(0)
        self.topn_spin.setMaximum(99)
        self.topn_spin.setValue(8)
        self.topn_spin.setSpecialValueText("全て")
        self.topn_spin.setToolTip("0 = 全て表示 / N = 上位N頭のみ表示")
        self.topn_spin.valueChanged.connect(self._on_pred_filter_changed)
        pred_filter_row.addWidget(self.topn_spin)
        self.placed_only_chk = QCheckBox("複勝圏のみ")
        self.placed_only_chk.setToolTip("実際に複勝圏に入った馬のみ表示")
        self.placed_only_chk.stateChanged.connect(self._on_pred_filter_changed)
        pred_filter_row.addWidget(self.placed_only_chk)
        self.has_odds_chk = QCheckBox("オッズあり馬のみ")
        self.has_odds_chk.setToolTip("place_odds テーブルにオッズが存在する馬のみ表示")
        self.has_odds_chk.stateChanged.connect(self._on_pred_filter_changed)
        pred_filter_row.addWidget(self.has_odds_chk)
        results_layout.addLayout(pred_filter_row)

        # 予測テーブル
        self.pred_table = QTableWidget(0, len(_PRED_TABLE_COLS))
        self.pred_table.setHorizontalHeaderLabels(_PRED_TABLE_COLS)
        self.pred_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.pred_table.verticalHeader().setVisible(False)
        self.pred_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.Stretch
        )
        self.pred_table.horizontalHeader().setSectionResizeMode(
            4, QHeaderView.ResizeMode.Stretch
        )
        self.pred_table.setMinimumHeight(80)
        results_layout.addWidget(self.pred_table)

        # 内部キャッシュ: フィルタ再適用用
        self._pred_rows_cache: list[dict] = []

        self._results_box.setContentLayout(results_layout)
        root_layout.addWidget(self._results_box)

        # ── 設定の読込・デフォルト検出 ────────────────
        self._load_settings()

    # ── 設定の保存・読込 ──────────────────────────────

    def _load_settings(self) -> None:
        cfg = load_config()

        # DB パス
        db = cfg.get("db_path") or str(REPO_ROOT / "jv_data.db")
        self.db_edit.setText(db)

        # モデルパス
        model = cfg.get("model_path") or str(REPO_ROOT / "models" / "place_model.cbm")
        self.model_edit.setText(model)

        # 出力ディレクトリ
        out = cfg.get("out_dir") or str(REPO_ROOT / "out")
        out_path = Path(out)
        if not out_path.exists():
            try:
                out_path.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
        self.outdir_edit.setText(out)

        # 32-bit Python
        py32 = cfg.get("python32_cmd", "")
        if not py32:
            detected = detect_python32()
            if detected:
                py32 = py32_to_display(detected)
                self._log(f"[自動検出] 32-bit Python: {py32}")
            else:
                self._log("[警告] 32-bit Python が見つかりませんでした。手動で選択してください。")
        self.py32_edit.setText(py32)

        # 取込開始日
        last_date = cfg.get("last_from_date", "")
        if last_date:
            d = QDate.fromString(last_date, "yyyyMMdd")
            if d.isValid():
                self.date_edit.setDate(d)
            else:
                self._log(f"[警告] 保存された日付 '{last_date}' が無効です。今日の日付を使用します。")

        # レースキー
        self.racekeys_edit.setText(cfg.get("race_keys", ""))

        # 検索キーワード・週末フィルタ
        self.keyword_edit.setText(cfg.get("search_keyword", ""))
        self.weekend_chk.setChecked(cfg.get("weekend_filter", False))

        # 複勝再学習パス
        place_train_csv = cfg.get("place_train_csv") or str(REPO_ROOT / "data" / "place_train.csv")
        self.place_train_csv_edit.setText(place_train_csv)
        place_retrain_model = cfg.get("place_retrain_model") or str(REPO_ROOT / "models" / "place_model.cbm")
        self.place_retrain_model_edit.setText(place_retrain_model)

        # ワイド再学習パス
        wide_train_csv = cfg.get("wide_train_csv") or str(REPO_ROOT / "data" / "wide_train.csv")
        self.wide_train_csv_edit.setText(wide_train_csv)
        wide_retrain_model = cfg.get("wide_retrain_model") or str(REPO_ROOT / "models" / "wide_model.cbm")
        self.wide_retrain_model_edit.setText(wide_retrain_model)

        # 3連複再学習パス
        sanrenpuku_train_csv = cfg.get("sanrenpuku_train_csv") or str(REPO_ROOT / "data" / "sanrenpuku_train.csv")
        self.sanrenpuku_train_csv_edit.setText(sanrenpuku_train_csv)
        sanrenpuku_retrain_model = cfg.get("sanrenpuku_retrain_model") or str(REPO_ROOT / "models" / "sanrenpuku_model.cbm")
        self.sanrenpuku_retrain_model_edit.setText(sanrenpuku_retrain_model)

        # 組み合わせ予測モデルパス
        wide_predict_model = cfg.get("wide_predict_model") or str(REPO_ROOT / "models" / "wide_model.cbm")
        self.wide_model_edit.setText(wide_predict_model)
        sanrenpuku_predict_model = cfg.get("sanrenpuku_predict_model") or str(REPO_ROOT / "models" / "sanrenpuku_model.cbm")
        self.sanrenpuku_model_edit.setText(sanrenpuku_predict_model)
        self.combo_topn_spin.setValue(int(cfg.get("combo_topn", 10)))

        # 複勝パイプライン設定
        if _ENABLE_PLACE_PIPELINE_GUI:
            self.place_pipeline_from_edit.setText(cfg.get("place_pipeline_from", ""))
            self.place_pipeline_to_edit.setText(cfg.get("place_pipeline_to", ""))
            self.place_pipeline_topn_spin.setValue(int(cfg.get("place_pipeline_topn", 3)))
            place_pipeline_model = cfg.get("place_pipeline_model") or str(REPO_ROOT / "models" / "place_lgbm.pkl")
            self.place_pipeline_model_edit.setText(place_pipeline_model)
            place_pipeline_datadir = cfg.get("place_pipeline_datadir") or str(REPO_ROOT / "data")
            self.place_pipeline_datadir_edit.setText(place_pipeline_datadir)

        # ウィンドウサイズ
        geom = cfg.get("window_geometry")
        if geom:
            try:
                self.resize(geom["width"], geom["height"])
                if "x" in geom and "y" in geom:
                    self.move(geom["x"], geom["y"])
            except Exception:
                pass

        # 折り畳み状態の復元
        collapsed = cfg.get("ui_collapsed", {})
        self._settings_box.setCollapsed(collapsed.get("settings", False))
        self._races_box.setCollapsed(collapsed.get("races", False))
        self._retrain_box.setCollapsed(collapsed.get("retrain", True))
        self._combo_box.setCollapsed(collapsed.get("combo", True))
        if _ENABLE_PLACE_PIPELINE_GUI:
            self._place_pipeline_box.setCollapsed(collapsed.get("place_pipeline", True))
        self._log_box.setCollapsed(collapsed.get("log", True))
        self._results_box.setCollapsed(collapsed.get("results", True))
        self._manual_box.setCollapsed(collapsed.get("manual", True))

    def _save_settings(self) -> None:
        geom = self.geometry()
        py32_cmd = display_to_py32(self.py32_edit.text())
        cfg = {
            "db_path": self.db_edit.text().strip(),
            "model_path": self.model_edit.text().strip(),
            "out_dir": self.outdir_edit.text().strip(),
            "python32_cmd": py32_to_display(py32_cmd),
            "last_from_date": self.date_edit.date().toString("yyyyMMdd"),
            "race_keys": self.racekeys_edit.text().strip(),
            "search_keyword": self.keyword_edit.text().strip(),
            "weekend_filter": self.weekend_chk.isChecked(),
            "place_train_csv": self.place_train_csv_edit.text().strip(),
            "place_retrain_model": self.place_retrain_model_edit.text().strip(),
            "wide_train_csv": self.wide_train_csv_edit.text().strip(),
            "wide_retrain_model": self.wide_retrain_model_edit.text().strip(),
            "sanrenpuku_train_csv": self.sanrenpuku_train_csv_edit.text().strip(),
            "sanrenpuku_retrain_model": self.sanrenpuku_retrain_model_edit.text().strip(),
            "wide_predict_model": self.wide_model_edit.text().strip(),
            "sanrenpuku_predict_model": self.sanrenpuku_model_edit.text().strip(),
            "combo_topn": self.combo_topn_spin.value(),
            "window_geometry": {
                "x": geom.x(),
                "y": geom.y(),
                "width": geom.width(),
                "height": geom.height(),
            },
            "ui_collapsed": {
                "settings": self._settings_box.isCollapsed(),
                "races": self._races_box.isCollapsed(),
                "retrain": self._retrain_box.isCollapsed(),
                "combo": self._combo_box.isCollapsed(),
                "log": self._log_box.isCollapsed(),
                "results": self._results_box.isCollapsed(),
                "manual": self._manual_box.isCollapsed(),
            },
        }
        if _ENABLE_PLACE_PIPELINE_GUI:
            cfg["place_pipeline_from"] = self.place_pipeline_from_edit.text().strip()
            cfg["place_pipeline_to"] = self.place_pipeline_to_edit.text().strip()
            cfg["place_pipeline_topn"] = self.place_pipeline_topn_spin.value()
            cfg["place_pipeline_model"] = self.place_pipeline_model_edit.text().strip()
            cfg["place_pipeline_datadir"] = self.place_pipeline_datadir_edit.text().strip()
            cfg["ui_collapsed"]["place_pipeline"] = self._place_pipeline_box.isCollapsed()
        save_config(cfg)

    def closeEvent(self, event: QCloseEvent) -> None:
        self._save_settings()
        super().closeEvent(event)

    # ── 32-bit Python 自動検出ボタン ─────────────────

    def _on_detect_python32(self) -> None:
        detected = detect_python32()
        if detected:
            text = py32_to_display(detected)
            self.py32_edit.setText(text)
            self._log(f"[検出] 32-bit Python: {text}")
        else:
            QMessageBox.warning(
                self,
                "検出失敗",
                "32-bit Python が自動検出できませんでした。\n手動でパスを入力または選択してください。",
            )

    # ── レース選択 (重賞) ──────────────────────────────

    def _on_load_graded_races(self) -> None:
        """選択日の重賞レース (grade_code A/B/C) をDBから読み込んでテーブルに表示する。"""
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return
        date_str = self.date_edit.date().toString("yyyyMMdd")
        require_place_odds = self.place_odds_chk.isChecked()

        try:
            conn = sqlite3.connect(db)
        except sqlite3.Error as e:
            self._log(f"[重賞読み込み] DB接続失敗: {e}")
            return

        try:
            rows = fetch_races(
                conn,
                from_date=date_str,
                to_date=date_str,
                grade_codes=["A", "B", "C"],
                name_contains=None,
                course_codes=None,
                require_place_odds=require_place_odds,
            )
        except sqlite3.OperationalError as e:
            self._log(f"[重賞読み込み] クエリ失敗: {e}")
            return
        finally:
            conn.close()

        rows.sort(key=lambda r: (r.get("course_code", ""), int(r.get("race_no") or 0) if str(r.get("race_no") or "").isdigit() else 0))
        self._populate_races_table(rows)
        self._log(f"[重賞読み込み] {len(rows)} 件取得 ({date_str}, place_odds={require_place_odds})")
        if not rows:
            self._log("[重賞読み込み] 該当レースなし。日付・DB・place_odds フィルタを確認してください。")

    def _on_search_races(self) -> None:
        """キーワード/週末メインフィルタでレースを検索してテーブルに表示する。"""
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return
        date_str = self.date_edit.date().toString("yyyyMMdd")
        require_place_odds = self.place_odds_chk.isChecked()
        keyword = self.keyword_edit.text().strip() or None
        min_race_no = 10 if self.weekend_chk.isChecked() else None

        try:
            conn = sqlite3.connect(db)
        except sqlite3.Error as e:
            self._log(f"[検索] DB接続失敗: {e}")
            return

        try:
            rows = fetch_races(
                conn,
                from_date=date_str,
                to_date=date_str,
                grade_codes=None,
                name_contains=keyword,
                course_codes=None,
                require_place_odds=require_place_odds,
                min_race_no=min_race_no,
            )
        except sqlite3.OperationalError as e:
            self._log(f"[検索] クエリ失敗: {e}")
            return
        finally:
            conn.close()

        rows.sort(key=lambda r: (r.get("course_code", ""), int(r.get("race_no") or 0) if str(r.get("race_no") or "").isdigit() else 0))
        self._populate_races_table(rows)
        self._log(f"[検索] {len(rows)} 件取得 (date={date_str}, keyword={keyword!r}, weekend={self.weekend_chk.isChecked()}, place_odds={require_place_odds})")
        if not rows:
            self._log("[検索] 該当レースなし。日付・キーワード・フィルタを確認してください。")

    def _populate_races_table(self, rows: list[dict]) -> None:
        """rows をレーステーブルに表示する (既存データをクリアする)。"""
        self.races_table.setRowCount(0)

        def _ro_item(val: object) -> QTableWidgetItem:
            it = QTableWidgetItem(str(val) if val is not None else "")
            it.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            return it

        for row_data in rows:
            row_idx = self.races_table.rowCount()
            self.races_table.insertRow(row_idx)

            chk = QTableWidgetItem()
            chk.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
            chk.setCheckState(Qt.CheckState.Unchecked)
            self.races_table.setItem(row_idx, 0, chk)

            self.races_table.setItem(row_idx, 1, _ro_item(row_data.get("race_name_short", "")))
            self.races_table.setItem(row_idx, 2, _ro_item(row_data.get("course_code", "")))
            self.races_table.setItem(row_idx, 3, _ro_item(row_data.get("race_no", "")))
            self.races_table.setItem(row_idx, 4, _ro_item(row_data.get("distance_m", "")))
            self.races_table.setItem(row_idx, 5, _ro_item(row_data.get("track_code", "")))
            self.races_table.setItem(row_idx, 6, _ro_item(row_data.get("grade_code", "")))
            self.races_table.setItem(row_idx, 7, _ro_item(row_data.get("race_key", "")))

    def _get_selected_race_keys(self) -> list[str]:
        """テーブルでチェックされた行の race_key を返す。"""
        keys = []
        for row in range(self.races_table.rowCount()):
            chk_item = self.races_table.item(row, 0)
            if chk_item and chk_item.checkState() == Qt.CheckState.Checked:
                key_item = self.races_table.item(row, 7)
                if key_item:
                    keys.append(key_item.text())
        return keys

    def _on_use_selected_races(self) -> None:
        """テーブルで選択されたレースの race_key をレースキー欄に設定する。"""
        keys = self._get_selected_race_keys()
        if keys:
            self.racekeys_edit.setText(" ".join(keys))
            self._log(f"[レース選択] {len(keys)} 件をレースキーに設定: {' '.join(keys)}")
        else:
            QMessageBox.information(self, "レース選択", "テーブルでレースにチェックを入れてください。")

    # ── 予想結果 ──────────────────────────────────────

    def _on_open_outdir(self) -> None:
        """出力フォルダをファイルマネージャーで開く。"""
        out_dir = self.outdir_edit.text().strip()
        if not out_dir:
            QMessageBox.warning(self, "エラー", "出力ディレクトリが未設定です。")
            return
        p = Path(out_dir)
        if not p.exists():
            QMessageBox.warning(self, "エラー", f"出力ディレクトリが見つかりません:\n{out_dir}")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(p)))

    def _on_refresh_results(self) -> None:
        """summary.csv を読み込んでサマリーテーブルを更新する。"""
        out_dir = self.outdir_edit.text().strip()
        if not out_dir:
            self._log("[結果更新] 出力ディレクトリが未設定です")
            return

        summary_path = Path(out_dir) / "summary.csv"
        if not summary_path.exists():
            self._log(f"[結果更新] summary.csv が見つかりません: {summary_path}")
            self.summary_table.setRowCount(0)
            return

        try:
            with open(summary_path, encoding="utf-8", newline="") as fh:
                summary_rows = list(csv.DictReader(fh))
        except Exception as e:
            self._log(f"[結果更新] summary.csv 読み込み失敗: {e}")
            return

        # DB からレース情報を取得して表示名を補完する
        race_info: dict[str, dict] = {}
        db = self.db_edit.text().strip()
        if db and Path(db).exists():
            try:
                conn = sqlite3.connect(db)
                try:
                    for row in summary_rows:
                        rk = row.get("race_key", "")
                        if rk:
                            cur = conn.execute(
                                "SELECT race_name_short, course_code, race_no"
                                " FROM races WHERE race_key = ?",
                                (rk,),
                            )
                            r = cur.fetchone()
                            if r:
                                race_info[rk] = {
                                    "race_name_short": r[0],
                                    "course_code": r[1],
                                    "race_no": r[2],
                                }
                finally:
                    conn.close()
            except Exception as e:
                self._log(f"[結果更新] DB からレース情報取得失敗: {e}")

        def _ro(val: object) -> QTableWidgetItem:
            it = QTableWidgetItem(str(val) if val is not None else "")
            it.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            return it

        self.summary_table.setRowCount(0)
        for row_data in summary_rows:
            rk = row_data.get("race_key", "")
            info = race_info.get(rk, {})
            row_idx = self.summary_table.rowCount()
            self.summary_table.insertRow(row_idx)
            self.summary_table.setItem(row_idx, 0, _ro(info.get("race_name_short", rk)))
            self.summary_table.setItem(row_idx, 1, _ro(info.get("course_code", "")))
            self.summary_table.setItem(row_idx, 2, _ro(info.get("race_no", "")))
            self.summary_table.setItem(row_idx, 3, _ro(row_data.get("status", "")))
            self.summary_table.setItem(row_idx, 4, _ro(row_data.get("n_bets", "")))
            self.summary_table.setItem(row_idx, 5, _ro(row_data.get("total_stake", "")))
            self.summary_table.setItem(row_idx, 6, _ro(row_data.get("sum_expected_value_yen", "")))
            self.summary_table.setItem(row_idx, 7, _ro(row_data.get("avg_p_place", "")))
            self.summary_table.setItem(row_idx, 8, _ro(row_data.get("fallback_used", "")))
            self.summary_table.setItem(row_idx, 9, _ro(rk))

        self.bets_table.setRowCount(0)
        self.pred_table.setRowCount(0)
        self._pred_rows_cache = []
        self.race_header_label.setVisible(False)
        self._log(f"[結果更新] {len(summary_rows)} 件読み込みました: {summary_path}")

    def _on_summary_row_selected(self) -> None:
        """サマリーテーブルの選択行の買い目・予測を詳細テーブルに表示する。"""
        selected = self.summary_table.selectedItems()
        if not selected:
            return

        row = selected[0].row()
        race_key_item = self.summary_table.item(row, 9)
        if not race_key_item:
            return
        race_key = race_key_item.text()
        out_dir = self.outdir_edit.text().strip()
        if not out_dir:
            return

        db = self.db_edit.text().strip()

        # レースヘッダーラベルを更新
        self._update_race_header(race_key, db)

        # 買い目テーブルを更新
        self.bets_table.setRowCount(0)
        bets_path = Path(out_dir) / f"bets_{race_key}.json"
        if bets_path.exists():
            try:
                bets: list[dict] = json.loads(bets_path.read_text(encoding="utf-8"))
                for bet in bets:
                    r = self.bets_table.rowCount()
                    self.bets_table.insertRow(r)
                    self.bets_table.setItem(r, 0, QTableWidgetItem(str(bet.get("horse_no", ""))))
                    self.bets_table.setItem(r, 1, QTableWidgetItem(str(bet.get("stake", ""))))
                    self.bets_table.setItem(r, 2, QTableWidgetItem(str(bet.get("p_place", ""))))
                    self.bets_table.setItem(r, 3, QTableWidgetItem(str(bet.get("place_odds_used", ""))))
                    self.bets_table.setItem(r, 4, QTableWidgetItem(str(bet.get("expected_value_yen", ""))))
                    self.bets_table.setItem(r, 5, QTableWidgetItem(str(bet.get("ev_per_1unit", ""))))
            except Exception as e:
                self._log(f"[結果] 買い目ファイル読み込み失敗: {e}")
        else:
            self._log(f"[結果] 買い目ファイルが見つかりません: {bets_path}")

        # 予測データをロードしてDBで拡充しキャッシュ
        self._pred_rows_cache = []
        pred_path = Path(out_dir) / f"pred_{race_key}.json"
        if pred_path.exists():
            try:
                preds: list[dict] = json.loads(pred_path.read_text(encoding="utf-8"))
            except Exception as e:
                self._log(f"[結果] 予測ファイル読み込み失敗: {e}")
                preds = []
        else:
            self._log(f"[結果] 予測ファイルが見つかりません: {pred_path}")
            preds = []

        if preds:
            self._pred_rows_cache = self._enrich_preds(preds, race_key, db)

        self._apply_pred_filters()

    def _update_race_header(self, race_key: str, db: str) -> None:
        """race_key に対応するレースヘッダーラベルを更新する。"""
        if not db or not Path(db).exists():
            self.race_header_label.setText(race_key)
            self.race_header_label.setVisible(True)
            return
        try:
            conn = sqlite3.connect(db)
            try:
                cur = conn.execute(
                    "SELECT yyyymmdd, course_code, race_no, race_name_short,"
                    " grade_code, distance_m, track_code"
                    " FROM races WHERE race_key = ?",
                    (race_key,),
                )
                r = cur.fetchone()
                if r:
                    yyyymmdd, course_code, race_no, race_name_short, grade_code, distance_m, track_code = r
                    # 出走頭数を取得
                    cnt_cur = conn.execute(
                        "SELECT COUNT(*) FROM entries WHERE race_key = ?", (race_key,)
                    )
                    n_entries = cnt_cur.fetchone()[0]
                    parts = [
                        str(yyyymmdd or ""),
                        str(course_code or ""),
                        f"R{race_no}" if race_no else "",
                        str(race_name_short or ""),
                        str(grade_code or ""),
                        f"{distance_m}m" if distance_m else "",
                        str(track_code or ""),
                        f"出走{n_entries}頭",
                    ]
                    label = "  |  ".join(p for p in parts if p)
                    self.race_header_label.setText(label)
                else:
                    self.race_header_label.setText(race_key)
            finally:
                conn.close()
        except Exception as e:
            logger.warning("[レースヘッダー] DB取得失敗: %s", e)
            self.race_header_label.setText(race_key)
        self.race_header_label.setVisible(True)

    def _enrich_preds(self, preds: list[dict], race_key: str, db: str) -> list[dict]:
        """予測リストを DB の entries/jockeys/trainers/place_odds で拡充して返す。"""
        # p_place 降順でソート → rank 付け
        sorted_preds = sorted(preds, key=lambda x: float(x.get("p_place", 0) or 0), reverse=True)
        for i, pred in enumerate(sorted_preds, start=1):
            pred["rank"] = i

        if not db or not Path(db).exists():
            return sorted_preds

        try:
            conn = sqlite3.connect(db)
        except Exception as e:
            logger.warning("[予測拡充] DB接続失敗: %s", e)
            return sorted_preds

        try:
            # entries テーブルが存在するか確認
            tbl_check = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='entries'"
            ).fetchone()
            if not tbl_check:
                logger.warning("[予測拡充] entries テーブルが存在しません")
                return sorted_preds

            # entries を horse_no キーでフェッチ
            entry_rows = conn.execute(
                "SELECT horse_no, finish_pos, is_place, jockey_code, trainer_code, horse_id"
                " FROM entries WHERE race_key = ?",
                (race_key,),
            ).fetchall()
            entry_map: dict[str, dict] = {
                r[0]: {
                    "finish_pos": r[1], "is_place": r[2],
                    "jockey_code": r[3], "trainer_code": r[4],
                    "horse_id": r[5],
                }
                for r in entry_rows
            }

            # jockeys テーブルが存在すれば騎手名を取得
            jockey_map: dict[str, str] = {}
            try:
                jk_rows = conn.execute(
                    "SELECT jockey_code, jockey_name FROM jockeys"
                ).fetchall()
                jockey_map = {r[0]: r[1] for r in jk_rows if r[0]}
            except Exception:
                logger.warning("[予測拡充] jockeys テーブルが利用できません")

            # trainers テーブルが存在すれば調教師名を取得
            trainer_map: dict[str, str] = {}
            try:
                tr_rows = conn.execute(
                    "SELECT trainer_code, trainer_name FROM trainers"
                ).fetchall()
                trainer_map = {r[0]: r[1] for r in tr_rows if r[0]}
            except Exception:
                logger.warning("[予測拡充] trainers テーブルが利用できません")

            # horses テーブルが存在すれば馬名を取得
            horse_map: dict[str, str] = {}
            try:
                hr_rows = conn.execute(
                    "SELECT horse_id, horse_name FROM horses"
                ).fetchall()
                horse_map = {r[0]: r[1] for r in hr_rows if r[0]}
            except Exception:
                logger.warning("[予測拡充] horses テーブルが利用できません")

            # place_odds テーブルが存在すれば has_odds セットを取得
            has_odds_set: set[str] = set()
            try:
                od_rows = conn.execute(
                    "SELECT horse_no FROM place_odds WHERE race_key = ?",
                    (race_key,),
                ).fetchall()
                has_odds_set = {r[0] for r in od_rows}
            except Exception:
                logger.warning("[予測拡充] place_odds テーブルが利用できません")

            # 拡充
            for pred in sorted_preds:
                hno = str(pred.get("horse_no", ""))
                entry = entry_map.get(hno, {})
                jockey_code = entry.get("jockey_code") or ""
                trainer_code = entry.get("trainer_code") or ""
                horse_id = entry.get("horse_id") or str(pred.get("horse_id", ""))
                pred["jockey_name"] = jockey_map.get(jockey_code, jockey_code)
                pred["trainer_name"] = trainer_map.get(trainer_code, trainer_code)
                pred["horse_name"] = horse_map.get(horse_id) or horse_id
                pred["finish_pos"] = entry.get("finish_pos")
                pred["is_place"] = entry.get("is_place")
                pred["has_odds"] = hno in has_odds_set

        except Exception as e:
            logger.warning("[予測拡充] 拡充処理失敗: %s", e)
        finally:
            conn.close()

        return sorted_preds

    def _apply_pred_filters(self) -> None:
        """キャッシュされた予測行にフィルタを適用して pred_table を再描画する。"""
        rows = self._pred_rows_cache
        topn = self.topn_spin.value()
        placed_only = self.placed_only_chk.isChecked()
        has_odds_only = self.has_odds_chk.isChecked()

        # "Show top N" フィルタ (rank 順 = p_place 降順ですでにソート済み)
        if topn > 0:
            rows = [r for r in rows if r.get("rank", _RANK_FALLBACK) <= topn]

        if placed_only:
            rows = [r for r in rows if r.get("is_place")]

        if has_odds_only:
            rows = [r for r in rows if r.get("has_odds")]

        self.pred_table.setRowCount(0)
        highlight = QBrush(_TP_HIGHLIGHT_COLOR)

        for pred in rows:
            r = self.pred_table.rowCount()
            self.pred_table.insertRow(r)

            is_tp = bool(pred.get("is_place")) and pred.get("rank", _RANK_FALLBACK) <= (topn if topn > 0 else _RANK_FALLBACK)

            def _item(val: object) -> QTableWidgetItem:
                it = QTableWidgetItem(str(val) if val is not None else "")
                if is_tp:
                    it.setBackground(highlight)
                return it

            finish_pos = pred.get("finish_pos")
            is_place = pred.get("is_place")

            self.pred_table.setItem(r, 0, _item(pred.get("rank", "")))
            self.pred_table.setItem(r, 1, _item(pred.get("horse_no", "")))
            self.pred_table.setItem(r, 2, _item(pred.get("horse_name") or pred.get("horse_id", "")))
            self.pred_table.setItem(r, 3, _item(pred.get("jockey_name", "")))
            self.pred_table.setItem(r, 4, _item(pred.get("trainer_name", "")))
            self.pred_table.setItem(r, 5, _item(f"{float(pred.get('p_place', 0)):.4f}" if pred.get("p_place") is not None else ""))
            self.pred_table.setItem(r, 6, _item(finish_pos if finish_pos is not None else ""))
            self.pred_table.setItem(r, 7, _item("✓" if is_place else ("" if is_place is None else "✗")))
            self.pred_table.setItem(r, 8, _item("✓" if is_tp else ""))

    def _on_pred_filter_changed(self) -> None:
        """フィルタ変更時に予測テーブルを再描画する。"""
        self._apply_pred_filters()

    def _on_toggle_bets(self) -> None:
        """買い目テーブルの表示/非表示を切り替える。"""
        hidden = self.toggle_bets_btn.isChecked()
        self.bets_table.setVisible(not hidden)
        self.toggle_bets_btn.setText("買い目を表示" if hidden else "買い目を隠す")

    # ── ウィジェットヘルパー ──────────────────────────

    def _with_browse(self, edit: QLineEdit, file: bool) -> QWidget:
        """LineEdit + Browse ボタンを横並びにしたウィジェットを返す。"""
        return self._with_browse_extra(edit, file=file, extra_btn=None)

    def _with_browse_extra(
        self, edit: QLineEdit, file: bool, extra_btn: QPushButton | None
    ) -> QWidget:
        """LineEdit + オプション追加ボタン + Browse ボタンを横並びにしたウィジェットを返す。"""
        container = QWidget()
        h = QHBoxLayout(container)
        h.setContentsMargins(0, 0, 0, 0)
        h.addWidget(edit)
        if extra_btn is not None:
            h.addWidget(extra_btn)
        btn = QPushButton("…")
        btn.setFixedWidth(32)
        if file:
            btn.clicked.connect(lambda: self._browse_file(edit))
        else:
            btn.clicked.connect(lambda: self._browse_dir(edit))
        h.addWidget(btn)
        return container

    def _browse_file(self, edit: QLineEdit) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "ファイルを選択", edit.text())
        if path:
            edit.setText(path)

    def _browse_dir(self, edit: QLineEdit) -> None:
        path = QFileDialog.getExistingDirectory(self, "ディレクトリを選択", edit.text())
        if path:
            edit.setText(path)

    # ── ログ ──────────────────────────────────────────

    def _log(self, text: str) -> None:
        self.log_view.appendPlainText(text)

    # ── バリデーション ────────────────────────────────

    def _require(self, value: str, label: str) -> bool:
        if not value.strip():
            QMessageBox.warning(self, "入力エラー", f"{label} を入力してください。")
            return False
        return True

    # ── ボタン有効/無効管理 ───────────────────────────

    def _set_running(self, running: bool) -> None:
        self.update_btn.setEnabled(not running)
        self.suggest_btn.setEnabled(not running)
        self.update_suggest_btn.setEnabled(not running)
        self.load_races_btn.setEnabled(not running)
        self.search_races_btn.setEnabled(not running)
        self.refresh_results_btn.setEnabled(not running)
        self.retrain_place_btn.setEnabled(not running and _PLACE_RETRAIN_AVAILABLE)
        self.retrain_wide_btn.setEnabled(not running and _WIDE_RETRAIN_AVAILABLE)
        self.retrain_sanrenpuku_btn.setEnabled(not running and _SANRENPUKU_RETRAIN_AVAILABLE)
        self.retrain_all_btn.setEnabled(not running and _PLACE_RETRAIN_AVAILABLE)
        self.predict_wide_btn.setEnabled(not running)
        self.predict_sanrenpuku_btn.setEnabled(not running)
        if _ENABLE_PLACE_PIPELINE_GUI:
            self.place_pipeline_btn.setEnabled(not running and _PLACE_PIPELINE_AVAILABLE)
        self.cancel_btn.setEnabled(running)

    # ── Update (RACE) ─────────────────────────────────

    def _build_update_commands(self) -> list[list[str]] | None:
        db = self.db_edit.text().strip()
        from_date = self.date_edit.date().toString("yyyyMMdd")
        py32_cmd = display_to_py32(self.py32_edit.text())

        if not self._require(db, "DB パス"):
            return None
        if not py32_cmd:
            QMessageBox.warning(self, "入力エラー", "32-bit Python 実行ファイル を入力してください。")
            return None

        program = py32_cmd[0]
        extra_args = py32_cmd[1:]

        ingest_cmd = (
            [program] + extra_args
            + [
                _script("jv_ingest_raw.py"),
                "--from-date", from_date,
                "--dataspec", "RACE",
                "--db", db,
            ]
        )

        update_cmd = [
            sys.executable,
            _script("update_db_from_raw.py"),
            "--db", db,
            "--skip-masters",
        ]

        return [ingest_cmd, update_cmd]

    def _build_suggest_commands(self) -> list[list[str]] | None:
        db = self.db_edit.text().strip()
        out_dir = self.outdir_edit.text().strip()
        model = self.model_edit.text().strip()

        if not self._require(db, "DB パス"):
            return None
        if not self._require(out_dir, "出力ディレクトリ"):
            return None
        if not self._require(model, "モデルパス"):
            return None

        # テーブルで選択されたレースキーを優先し、なければ手動入力を使用
        race_keys = self._get_selected_race_keys()
        if not race_keys:
            race_keys_raw = self.racekeys_edit.text().strip()
            if not self._require(race_keys_raw, "レースキー (テーブルで選択するか手動入力)"):
                return None
            race_keys = race_keys_raw.split()

        cmd = [
            sys.executable,
            _script("batch_suggest_place_bets.py"),
            "--db", db,
            "--model", model,
            "--out-dir", out_dir,
            "--race-keys", *race_keys,
        ]
        return [cmd]

    def _on_update(self) -> None:
        cmds = self._build_update_commands()
        if cmds is None:
            return

        self._log("=" * 60)
        self._log("[Update] Step 1/2: jv_ingest_raw.py (32-bit Python)")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_update_done)

    def _on_update_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[Update] 完了")
        else:
            self._log("[Update] キャンセルされました" if self._cancelled else "[Update] エラーで終了しました")

    # ── Suggest ──────────────────────────────────────

    def _on_suggest(self) -> None:
        cmds = self._build_suggest_commands()
        if cmds is None:
            return

        self._save_snapshot("suggest")
        self._log("=" * 60)
        self._log("[Suggest] batch_suggest_place_bets.py を実行します")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_suggest_done)

    def _on_suggest_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[Suggest] 完了")
            self._on_refresh_results()
        else:
            self._log("[Suggest] キャンセルされました" if self._cancelled else "[Suggest] エラーで終了しました")

    # ── Update + Suggest ─────────────────────────────

    def _on_update_suggest(self) -> None:
        update_cmds = self._build_update_commands()
        if update_cmds is None:
            return
        suggest_cmds = self._build_suggest_commands()
        if suggest_cmds is None:
            return

        self._log("=" * 60)
        self._log("[Update+Suggest] Update → Suggest の順に実行します")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(update_cmds + suggest_cmds, on_finish=self._on_update_suggest_done)

    def _on_update_suggest_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[Update+Suggest] 完了")
            self._on_refresh_results()
        else:
            self._log("[Update+Suggest] キャンセルされました" if self._cancelled else "[Update+Suggest] エラーで終了しました")

    # ── 再学習 ────────────────────────────────────────

    def _build_retrain_place_commands(self) -> list[list[str]] | None:
        """複勝モデル再学習用の 3 ステップコマンドリストを構築して返す。

        ステップ:
          1. build_tables_from_raw.py  -- 正規化テーブルを最新化
          2. build_place_training_data.py -- 学習データ CSV を生成
          3. train_place_model.py      -- CatBoost で複勝モデルを学習

        DB パスが未入力の場合はダイアログを表示して None を返す。
        """
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return None

        train_csv = self.place_train_csv_edit.text().strip()
        if not train_csv:
            train_csv = str(REPO_ROOT / "data" / "place_train.csv")

        model_out = self.place_retrain_model_edit.text().strip()
        if not model_out:
            model_out = str(REPO_ROOT / "models" / "place_model.cbm")

        build_tables_cmd = [
            sys.executable,
            _script("build_tables_from_raw.py"),
            "--db", db,
        ]

        build_passing_positions_cmd = [
            sys.executable,
            _script("build_race_passing_positions_from_ra7.py"),
            "--db", db,
        ]

        build_passing_features_cmd = [
            sys.executable,
            _script("build_horse_past_passing_features.py"),
            "--db", db,
            "--n-last", "3",
        ]

        build_data_cmd = [
            sys.executable,
            _script("build_place_training_data.py"),
            "--db", db,
            "--out", train_csv,
        ]

        train_cmd = [
            sys.executable,
            _script("train_place_model.py"),
            "--train-csv", train_csv,
            "--model-out", model_out,
        ]

        return [build_tables_cmd, build_passing_positions_cmd, build_passing_features_cmd, build_data_cmd, train_cmd]

    def _on_retrain_place(self) -> None:
        reply = QMessageBox.question(
            self,
            "確認",
            "複勝モデルを再学習しますか？\n\n"
            "以下のスクリプトを順番に実行します:\n"
            "1. build_tables_from_raw.py\n"
            "2. build_race_passing_positions_from_ra7.py\n"
            "3. build_horse_past_passing_features.py\n"
            "4. build_place_training_data.py\n"
            "5. train_place_model.py",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        cmds = self._build_retrain_place_commands()
        if cmds is None:
            return

        self._log("=" * 60)
        self._log("[再学習] 複勝モデル再学習を開始します")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_retrain_place_done)

    def _on_retrain_place_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[再学習] 複勝モデル再学習 完了")
        else:
            self._log("[再学習] キャンセルされました" if self._cancelled else "[再学習] エラーで終了しました")

    def _build_retrain_wide_commands(self) -> list[list[str]] | None:
        """ワイドモデル再学習用の 2 ステップコマンドリストを構築して返す。

        ステップ:
          1. build_wide_training_data.py -- 学習データ CSV を生成
          2. train_wide_model.py         -- CatBoost でワイドモデルを学習

        DB パスが未入力の場合はダイアログを表示して None を返す。
        """
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return None

        train_csv = self.wide_train_csv_edit.text().strip()
        if not train_csv:
            train_csv = str(REPO_ROOT / "data" / "wide_train.csv")

        model_out = self.wide_retrain_model_edit.text().strip()
        if not model_out:
            model_out = str(REPO_ROOT / "models" / "wide_model.cbm")

        build_data_cmd = [
            sys.executable,
            _script("build_wide_training_data.py"),
            "--db", db,
            "--out", train_csv,
        ]

        train_cmd = [
            sys.executable,
            _script("train_wide_model.py"),
            "--train-csv", train_csv,
            "--model-out", model_out,
        ]

        return [build_data_cmd, train_cmd]

    def _on_retrain_wide(self) -> None:
        reply = QMessageBox.question(
            self,
            "確認",
            "ワイドモデルを再学習しますか？\n\n"
            "以下のスクリプトを順番に実行します:\n"
            "1. build_wide_training_data.py\n"
            "2. train_wide_model.py",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        cmds = self._build_retrain_wide_commands()
        if cmds is None:
            return

        self._log("=" * 60)
        self._log("[再学習] ワイドモデル再学習を開始します")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_retrain_wide_done)

    def _on_retrain_wide_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[再学習] ワイドモデル再学習 完了")
        else:
            self._log("[再学習] キャンセルされました" if self._cancelled else "[再学習] エラーで終了しました")

    def _build_retrain_sanrenpuku_commands(self) -> list[list[str]] | None:
        """3連複モデル再学習用の 2 ステップコマンドリストを構築して返す。

        ステップ:
          1. build_sanrenpuku_training_data.py -- 学習データ CSV を生成
          2. train_sanrenpuku_model.py         -- CatBoost で3連複モデルを学習

        DB パスが未入力の場合はダイアログを表示して None を返す。
        """
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return None

        train_csv = self.sanrenpuku_train_csv_edit.text().strip()
        if not train_csv:
            train_csv = str(REPO_ROOT / "data" / "sanrenpuku_train.csv")

        model_out = self.sanrenpuku_retrain_model_edit.text().strip()
        if not model_out:
            model_out = str(REPO_ROOT / "models" / "sanrenpuku_model.cbm")

        build_data_cmd = [
            sys.executable,
            _script("build_sanrenpuku_training_data.py"),
            "--db", db,
            "--out", train_csv,
        ]

        train_cmd = [
            sys.executable,
            _script("train_sanrenpuku_model.py"),
            "--train-csv", train_csv,
            "--model-out", model_out,
        ]

        return [build_data_cmd, train_cmd]

    def _on_retrain_sanrenpuku(self) -> None:
        reply = QMessageBox.question(
            self,
            "確認",
            "3連複モデルを再学習しますか？\n\n"
            "以下のスクリプトを順番に実行します:\n"
            "1. build_sanrenpuku_training_data.py\n"
            "2. train_sanrenpuku_model.py",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        cmds = self._build_retrain_sanrenpuku_commands()
        if cmds is None:
            return

        self._log("=" * 60)
        self._log("[再学習] 3連複モデル再学習を開始します")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_retrain_sanrenpuku_done)

    def _on_retrain_sanrenpuku_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[再学習] 3連複モデル再学習 完了")
        else:
            self._log("[再学習] キャンセルされました" if self._cancelled else "[再学習] エラーで終了しました")

    def _on_retrain_all(self) -> None:
        reply = QMessageBox.question(
            self,
            "確認",
            "全モデルを再学習しますか？\n（複勝 → ワイド → 3連複の順に実行）\n\n"
            "以下のスクリプトを順番に実行します:\n"
            "1. build_tables_from_raw.py\n"
            "2. build_place_training_data.py\n"
            "3. train_place_model.py\n"
            "4. build_wide_training_data.py\n"
            "5. train_wide_model.py\n"
            "6. build_sanrenpuku_training_data.py\n"
            "7. train_sanrenpuku_model.py",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        cmds = self._build_retrain_place_commands()
        if cmds is None:
            return

        if _WIDE_RETRAIN_AVAILABLE:
            wide_cmds = self._build_retrain_wide_commands()
            if wide_cmds is None:
                return
            cmds = cmds + wide_cmds

        if _SANRENPUKU_RETRAIN_AVAILABLE:
            sanrenpuku_cmds = self._build_retrain_sanrenpuku_commands()
            if sanrenpuku_cmds is None:
                return
            cmds = cmds + sanrenpuku_cmds

        self._log("=" * 60)
        self._log("[再学習] 全モデル再学習を開始します（複勝 → ワイド → 3連複）")
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_retrain_all_done)

    def _on_retrain_all_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[再学習] 全モデル再学習 完了")
        else:
            self._log("[再学習] キャンセルされました" if self._cancelled else "[再学習] エラーで終了しました")

    # ── 組み合わせ予測 ─────────────────────────────────

    def _get_combo_race_keys(self) -> list[str] | None:
        """テーブルで選択されたか手動入力されたレースキーを返す。なければ警告を出して None を返す。"""
        race_keys = self._get_selected_race_keys()
        if not race_keys:
            race_keys_raw = self.racekeys_edit.text().strip()
            if not race_keys_raw:
                QMessageBox.warning(
                    self, "入力エラー", "レースキーをテーブルで選択するか手動入力してください。"
                )
                return None
            race_keys = race_keys_raw.split()
        return race_keys

    def _build_wide_predict_commands(self) -> list[list[str]] | None:
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return None
        model = self.wide_model_edit.text().strip() or str(REPO_ROOT / "models" / "wide_model.cbm")
        topn = self.combo_topn_spin.value()
        race_keys = self._get_combo_race_keys()
        if race_keys is None:
            return None
        return [
            [
                sys.executable,
                _script("predict_wide.py"),
                "--db", db,
                "--race-key", rk,
                "--model", model,
                "--topn", str(topn),
                "--format", "jsonl",
            ]
            for rk in race_keys
        ]

    def _build_sanrenpuku_predict_commands(self) -> list[list[str]] | None:
        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return None
        model = self.sanrenpuku_model_edit.text().strip() or str(REPO_ROOT / "models" / "sanrenpuku_model.cbm")
        topn = self.combo_topn_spin.value()
        race_keys = self._get_combo_race_keys()
        if race_keys is None:
            return None
        return [
            [
                sys.executable,
                _script("predict_sanrenpuku.py"),
                "--db", db,
                "--race-key", rk,
                "--model", model,
                "--topn", str(topn),
                "--format", "jsonl",
            ]
            for rk in race_keys
        ]

    def _on_predict_wide(self) -> None:
        cmds = self._build_wide_predict_commands()
        if cmds is None:
            return
        self._save_snapshot("wide")
        self._log("=" * 60)
        self._log("[ワイド予測] predict_wide.py を実行します")
        self.wide_table.setRowCount(0)
        self._set_running(True)
        self._cancelled = False
        self._run_combo_sequential(cmds, on_finish=self._on_wide_predict_done)

    def _on_wide_predict_done(self, success: bool, results: list[dict]) -> None:
        self._set_running(False)
        if success:
            self._log(f"[ワイド予測] 完了: {len(results)} 件")
            self._display_wide_results(results)
            self._combo_box.setCollapsed(False)
        else:
            self._log(
                "[ワイド予測] キャンセルされました"
                if self._cancelled
                else "[ワイド予測] エラーで終了しました"
            )

    def _display_wide_results(self, results: list[dict]) -> None:
        self.wide_table.setRowCount(0)
        for rank, row in enumerate(results, start=1):
            r = self.wide_table.rowCount()
            self.wide_table.insertRow(r)
            self.wide_table.setItem(r, 0, QTableWidgetItem(str(rank)))
            self.wide_table.setItem(r, 1, QTableWidgetItem(str(row.get("race_key", ""))))
            self.wide_table.setItem(r, 2, QTableWidgetItem(str(row.get("horse_no_a", ""))))
            self.wide_table.setItem(r, 3, QTableWidgetItem(str(row.get("horse_no_b", ""))))
            self.wide_table.setItem(r, 4, QTableWidgetItem(str(row.get("p_wide", ""))))

    def _on_predict_sanrenpuku(self) -> None:
        cmds = self._build_sanrenpuku_predict_commands()
        if cmds is None:
            return
        self._save_snapshot("sanrenpuku")
        self._log("=" * 60)
        self._log("[3連複予測] predict_sanrenpuku.py を実行します")
        self.sanrenpuku_table.setRowCount(0)
        self._set_running(True)
        self._cancelled = False
        self._run_combo_sequential(cmds, on_finish=self._on_sanrenpuku_predict_done)

    def _on_sanrenpuku_predict_done(self, success: bool, results: list[dict]) -> None:
        self._set_running(False)
        if success:
            self._log(f"[3連複予測] 完了: {len(results)} 件")
            self._display_sanrenpuku_results(results)
            self._combo_box.setCollapsed(False)
        else:
            self._log(
                "[3連複予測] キャンセルされました"
                if self._cancelled
                else "[3連複予測] エラーで終了しました"
            )

    def _display_sanrenpuku_results(self, results: list[dict]) -> None:
        self.sanrenpuku_table.setRowCount(0)
        for rank, row in enumerate(results, start=1):
            r = self.sanrenpuku_table.rowCount()
            self.sanrenpuku_table.insertRow(r)
            self.sanrenpuku_table.setItem(r, 0, QTableWidgetItem(str(rank)))
            self.sanrenpuku_table.setItem(r, 1, QTableWidgetItem(str(row.get("race_key", ""))))
            self.sanrenpuku_table.setItem(r, 2, QTableWidgetItem(str(row.get("horse_no_a", ""))))
            self.sanrenpuku_table.setItem(r, 3, QTableWidgetItem(str(row.get("horse_no_b", ""))))
            self.sanrenpuku_table.setItem(r, 4, QTableWidgetItem(str(row.get("horse_no_c", ""))))
            self.sanrenpuku_table.setItem(r, 5, QTableWidgetItem(str(row.get("p_sanrenpuku", ""))))

    # ── 複勝 推奨生成 (一括) ────────────────────────────

    def _build_place_pipeline_commands(self) -> list[list[str]] | None:
        """5段階の複勝パイプラインコマンドリストを構築する。"""
        # 必須スクリプトの存在確認
        missing = [
            s.name for s in [
                _PLACE_PIPELINE_BUILD_SCRIPT, _PLACE_PIPELINE_SPLIT_SCRIPT,
                _PLACE_PIPELINE_TRAIN_SCRIPT, _PLACE_PIPELINE_PREDICT_SCRIPT,
                _PLACE_PIPELINE_RECOMMEND_SCRIPT,
            ] if not s.exists()
        ]
        if missing:
            QMessageBox.critical(
                self,
                "スクリプト不足",
                "以下のスクリプトが見つかりません:\n" + "\n".join(missing),
            )
            return None

        db = self.db_edit.text().strip()
        if not self._require(db, "DB パス"):
            return None

        data_dir = self.place_pipeline_datadir_edit.text().strip() or str(REPO_ROOT / "data")
        model_out = self.place_pipeline_model_edit.text().strip() or str(REPO_ROOT / "models" / "place_lgbm.pkl")
        topn = self.place_pipeline_topn_spin.value()
        date_from = self.place_pipeline_from_edit.text().strip()
        date_to = self.place_pipeline_to_edit.text().strip()

        data_dir_path = Path(data_dir)
        combined_csv = str(data_dir_path / "place_train_with_unlabeled.csv")
        train_csv = str(data_dir_path / "place_train_labeled.csv")
        pred_csv = str(data_dir_path / "place_pred_unlabeled.csv")
        scored_csv = str(data_dir_path / "place_pred_scored.csv")
        recommendations_csv = str(data_dir_path / "place_recommendations_rich.csv")

        # Step 1: build_place_training_data.py --include-unlabeled
        build_cmd = [
            sys.executable,
            _script("build_place_training_data.py"),
            "--db", db,
            "--out", combined_csv,
            "--include-unlabeled",
        ]
        if date_from:
            build_cmd += ["--from", date_from]
        if date_to:
            build_cmd += ["--to", date_to]

        # Step 2: split_labeled_unlabeled_csv.py
        split_cmd = [
            sys.executable,
            _script("split_labeled_unlabeled_csv.py"),
            "--in", combined_csv,
            "--labeled", train_csv,
            "--unlabeled", pred_csv,
        ]

        # Step 3: train_place_model_lgbm.py
        train_cmd = [
            sys.executable,
            _script("train_place_model_lgbm.py"),
            "--train-csv", train_csv,
            "--model-out", model_out,
        ]

        # Step 4: predict_place_model_lgbm.py
        predict_cmd = [
            sys.executable,
            _script("predict_place_model_lgbm.py"),
            "--in", pred_csv,
            "--model", model_out,
            "--out", scored_csv,
        ]

        # Step 5: make_place_recommendations_rich.py
        recommend_cmd = [
            sys.executable,
            _script("make_place_recommendations_rich.py"),
            "--scored-csv", scored_csv,
            "--db", db,
            "--out", recommendations_csv,
            "--topn", str(topn),
        ]

        return [build_cmd, split_cmd, train_cmd, predict_cmd, recommend_cmd]

    def _on_place_pipeline(self) -> None:
        cmds = self._build_place_pipeline_commands()
        if cmds is None:
            return
        self._log("=" * 60)
        self._log("[複勝パイプライン] 推奨生成パイプラインを開始します")
        self._log("[複勝パイプライン] Step 1: データ生成 (build_place_training_data.py)")
        self._log("[複勝パイプライン] Step 2: ラベル分割 (split_labeled_unlabeled_csv.py)")
        self._log("[複勝パイプライン] Step 3: モデル学習 (train_place_model_lgbm.py)")
        self._log("[複勝パイプライン] Step 4: スコアリング (predict_place_model_lgbm.py)")
        self._log("[複勝パイプライン] Step 5: 推奨生成 (make_place_recommendations_rich.py)")
        self.place_reco_table.setRowCount(0)
        self._set_running(True)
        self._cancelled = False
        self._run_sequential(cmds, on_finish=self._on_place_pipeline_done)

    def _on_place_pipeline_done(self, success: bool) -> None:
        self._set_running(False)
        if success:
            self._log("[複勝パイプライン] 完了")
            data_dir = self.place_pipeline_datadir_edit.text().strip() or str(REPO_ROOT / "data")
            recommendations_csv = str(Path(data_dir) / "place_recommendations_rich.csv")
            self._log(f"[複勝パイプライン] 推奨ファイル: {recommendations_csv}")
            self._display_place_recommendations(recommendations_csv)
            if _ENABLE_PLACE_PIPELINE_GUI:
                self._place_pipeline_box.setCollapsed(False)
        else:
            self._log(
                "[複勝パイプライン] キャンセルされました"
                if self._cancelled
                else "[複勝パイプライン] エラーで終了しました"
            )

    def _display_place_recommendations(self, csv_path: str) -> None:
        """place_recommendations_rich.csv を読み込んでテーブルに表示する。"""
        self.place_reco_table.setRowCount(0)
        try:
            import csv as _csv
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = _csv.DictReader(f)
                rows = list(reader)
        except FileNotFoundError:
            self._log(f"[複勝パイプライン] 推奨ファイルが見つかりません: {csv_path}")
            return
        except Exception as exc:
            self._log(f"[複勝パイプライン] 推奨ファイル読み込み失敗: {exc}")
            return

        # race_date / course_code / race_no / rank_in_race でソート
        def _sort_key(row: dict) -> tuple[str, str, str, str]:
            return (
                row.get("race_date", ""),
                row.get("course_code", ""),
                row.get("race_no", ""),
                row.get("rank_in_race", ""),
            )

        rows.sort(key=_sort_key)

        for row in rows:
            r = self.place_reco_table.rowCount()
            self.place_reco_table.insertRow(r)
            for c, col in enumerate(_PLACE_RECO_TABLE_COLS):
                self.place_reco_table.setItem(r, c, QTableWidgetItem(str(row.get(col, ""))))

        self._log(f"[複勝パイプライン] {len(rows)} 件の推奨を表示しました")

    def _run_combo_sequential(
        self,
        commands: list[list[str]],
        on_finish: Callable[[bool, list[dict]], None],
        _index: int = 0,
        _accumulated: list[dict] | None = None,
    ) -> None:
        """commands をインデックス順に逐次実行し、各コマンドの JSONL 出力を収集して
        全完了後に on_finish(success, rows) を呼ぶ。"""
        if _accumulated is None:
            _accumulated = []

        if self._cancelled:
            return

        if _index >= len(commands):
            on_finish(True, _accumulated)
            return

        cmd = commands[_index]
        step_label = f"Step {_index + 1}/{len(commands)}"
        self._log(f"[{step_label}] $ {' '.join(cmd)}")

        proc = QProcess(self)
        self._processes.append(proc)
        proc.setProcessChannelMode(QProcess.ProcessChannelMode.SeparateChannels)

        _stdout_chunks: list[bytes] = []

        proc.readyReadStandardOutput.connect(
            lambda: _stdout_chunks.append(proc.readAllStandardOutput().data())
        )

        def _read_stderr() -> None:
            data = proc.readAllStandardError().data()
            try:
                text = data.decode("cp932")
            except UnicodeDecodeError:
                text = data.decode("utf-8", errors="replace")
            for line in text.splitlines():
                self._log(line)

        proc.readyReadStandardError.connect(_read_stderr)

        def _finished(
            exit_code: int,
            exit_status: QProcess.ExitStatus,
            p: QProcess = proc,
        ) -> None:
            if p in self._processes:
                self._processes.remove(p)
            if self._cancelled:
                return

            raw = b"".join(_stdout_chunks)
            try:
                text = raw.decode("utf-8", errors="replace")
            except UnicodeDecodeError:
                text = ""

            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    _accumulated.append(json.loads(line))
                except json.JSONDecodeError:
                    self._log(f"  [JSON解析失敗] {line}")

            if (
                exit_status == QProcess.ExitStatus.NormalExit
                and exit_code == 0
            ):
                self._log(f"[{step_label}] 終了 (exit code 0)")
                self._run_combo_sequential(commands, on_finish, _index + 1, _accumulated)
            else:
                self._log(
                    f"[{step_label}] 失敗 (exit code {exit_code}, status {exit_status})"
                )
                on_finish(False, _accumulated)

        proc.finished.connect(_finished)
        proc.start(cmd[0], cmd[1:])

    # ── キャンセル ────────────────────────────────────

    def _on_cancel(self) -> None:
        self._cancelled = True
        for proc in list(self._processes):
            proc.kill()
        self._processes.clear()
        self._set_running(False)
        self._log("[キャンセル] 実行中のプロセスを停止しました")

    # ── プロセス実行 ──────────────────────────────────

    def _run_sequential(
        self,
        commands: list[list[str]],
        on_finish: Callable[[bool], None],
        _index: int = 0,
    ) -> None:
        """commands をインデックス順に逐次実行し、全完了後に on_finish(success) を呼ぶ。"""
        if self._cancelled:
            return

        if _index >= len(commands):
            on_finish(True)
            return

        cmd = commands[_index]
        step_label = f"Step {_index + 1}/{len(commands)}"
        self._log(f"[{step_label}] $ {' '.join(cmd)}")

        proc = QProcess(self)
        self._processes.append(proc)
        proc.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)

        proc.readyReadStandardOutput.connect(
            lambda: self._on_stdout(proc)
        )

        def _finished(exit_code: int, exit_status: QProcess.ExitStatus, p=proc) -> None:
            if p in self._processes:
                self._processes.remove(p)
            if self._cancelled:
                return
            if (
                exit_status == QProcess.ExitStatus.NormalExit
                and exit_code == 0
            ):
                self._log(f"[{step_label}] 終了 (exit code 0)")
                self._run_sequential(commands, on_finish, _index + 1)
            else:
                self._log(
                    f"[{step_label}] 失敗 (exit code {exit_code}, status {exit_status})"
                )
                on_finish(False)

        proc.finished.connect(_finished)
        proc.start(cmd[0], cmd[1:])

    def _on_stdout(self, proc: QProcess) -> None:
        data = proc.readAllStandardOutput().data()

        # Windowsの多くのCLI出力はcp932(=Shift-JIS拡張)なのでまずcp932で試す
        try:
            text = data.decode("cp932")
        except Exception:
            # フォールバック
            text = data.decode("utf-8", errors="replace")

        for line in text.splitlines():
            self._log(line)

    # ── 当日入力（手動予測）────────────────────────────

    def _get_manual_cell(self, row: int, col: int) -> tuple[str, str]:
        """セルの (表示テキスト, 値) を返す。コンボは (表示名, データ)、テキストは (text, text)。"""
        widget = self.manual_table.cellWidget(row, col)
        if widget is None:
            item = self.manual_table.item(row, col)
            text = item.text().strip() if item else ""
            return text, text
        if isinstance(widget, _MasterLineEdit):
            display = widget.text().strip()
            value = widget.selected_code()
            return display, value
        if isinstance(widget, QComboBox):
            display = widget.currentText().strip()
            data = widget.currentData()
            value = str(data) if (data is not None and data != "") else display
            return display, value
        if isinstance(widget, QLineEdit):
            text = widget.text().strip()
            return text, text
        return "", ""

    def _lookup_latest_metrics(self, horse_id: str) -> dict | None:
        """horse_id の最新斤量・馬体重を DB から取得する。

        優先順位:
        1. horse_latest_metrics テーブル (高速)
        2. entries テーブルの最新 race_key (フォールバック)

        戻り値: {"handicap_weight_x10": int | None, "body_weight": int | None, "source": str}
        見つからない場合は None を返す。
        """
        db = self.db_edit.text().strip()
        if not db or not Path(db).exists():
            return None
        try:
            with sqlite3.connect(db) as conn:
                # 1. horse_latest_metrics テーブルを試みる
                try:
                    row = conn.execute(
                        "SELECT handicap_weight_x10, body_weight FROM horse_latest_metrics"
                        " WHERE horse_id = ?",
                        (horse_id,),
                    ).fetchone()
                    if row and (row[0] is not None or row[1] is not None):
                        return {
                            "handicap_weight_x10": row[0],
                            "body_weight": row[1],
                            "source": "horse_latest_metrics",
                        }
                except Exception:
                    pass

                # 2. entries テーブルからフォールバック
                try:
                    row = conn.execute(
                        "SELECT handicap_weight_x10, body_weight FROM entries"
                        " WHERE horse_id = ?"
                        "   AND (handicap_weight_x10 IS NOT NULL OR body_weight IS NOT NULL)"
                        " ORDER BY race_key DESC LIMIT 1",
                        (horse_id,),
                    ).fetchone()
                    if row and (row[0] is not None or row[1] is not None):
                        return {
                            "handicap_weight_x10": row[0],
                            "body_weight": row[1],
                            "source": "entries",
                        }
                except Exception:
                    pass
        except Exception:
            pass
        return None

    def _on_manual_horse_changed(self, row: int) -> None:
        """馬コンボの選択が変わったときに斤量・馬体重を自動入力する (空欄の場合のみ)。"""
        _display, horse_id = self._get_manual_cell(row, _MANUAL_COL_HORSE)
        if not horse_id:
            return

        metrics = self._lookup_latest_metrics(horse_id)
        if metrics is None:
            return

        filled_any = False

        # 斤量: 空のセルのみ更新
        handicap_item = self.manual_table.item(row, _MANUAL_COL_HANDICAP)
        handicap_text = handicap_item.text().strip() if handicap_item else ""
        if not handicap_text and metrics.get("handicap_weight_x10") is not None:
            hw = metrics["handicap_weight_x10"]
            self.manual_table.setItem(
                row, _MANUAL_COL_HANDICAP, QTableWidgetItem(f"{hw / 10:.1f}")
            )
            filled_any = True

        # 馬体重: 空のセルのみ更新
        bw_item = self.manual_table.item(row, _MANUAL_COL_BODY_WEIGHT)
        bw_text = bw_item.text().strip() if bw_item else ""
        if not bw_text and metrics.get("body_weight") is not None:
            bw = metrics["body_weight"]
            self.manual_table.setItem(
                row, _MANUAL_COL_BODY_WEIGHT, QTableWidgetItem(str(bw))
            )
            filled_any = True

        if filled_any:
            self._log(
                f"[手動予測] 行{row + 1} 馬 {horse_id}: 斤量/馬体重を自動入力"
                f" (source: {metrics['source']})"
            )

    def _on_manual_load_masters(self) -> None:
        """DB からマスタデータを読み込んでテーブルのドロップダウンを更新する。"""
        db = self.db_edit.text().strip()
        if not db:
            self._log("[手動予測] DB パスが未設定です。フリーテキスト入力になります。")
            return
        if not Path(db).exists():
            self._log(f"[手動予測] DB ファイルが見つかりません: {db}")
            return
        try:
            conn = sqlite3.connect(db)
        except Exception as exc:
            self._log(f"[手動予測] DB 接続失敗: {exc}")
            return

        try:
            try:
                rows = conn.execute(
                    "SELECT horse_id, horse_name FROM horses ORDER BY horse_name"
                ).fetchall()
                name_counts: dict[str, int] = {}
                for r in rows:
                    name_counts[r[1]] = name_counts.get(r[1], 0) + 1
                self._manual_horse_data = [
                    (f"{r[1]} ({r[0]})" if name_counts[r[1]] > 1 else r[1], r[0])
                    for r in rows
                ]
                self._manual_horses_available = True
                self._log(f"[手動予測] 馬マスタ: {len(rows)} 件読み込み")
            except Exception:
                self._manual_horse_data = []
                self._manual_horses_available = False
                self._log("[手動予測] horses テーブルが利用できません。horse_id を直接入力してください。")

            # 騎手: jockeys (full name) → jockey_aliases (short name) → free text
            jockey_loaded = False
            try:
                rows = conn.execute(
                    "SELECT jockey_code, jockey_name FROM jockeys ORDER BY jockey_name"
                ).fetchall()
                if rows:
                    self._manual_jockey_data = [(f"{r[1]} ({r[0]})", r[0]) for r in rows]
                    self._manual_jockeys_available = True
                    self._log(f"[手動予測] 騎手マスタ (jockeys): {len(rows)} 件読み込み")
                    jockey_loaded = True
                else:
                    self._log("[手動予測] jockeys テーブルは 0 件です。jockey_aliases にフォールバックします。")
            except Exception:
                pass
            if not jockey_loaded:
                try:
                    rows = conn.execute(
                        "SELECT jockey_code, jockey_name_short FROM jockey_aliases ORDER BY jockey_name_short"
                    ).fetchall()
                    if rows:
                        self._manual_jockey_data = [(f"{r[1]} ({r[0]})", r[0]) for r in rows]
                        self._manual_jockeys_available = True
                        self._log(f"[手動予測] 騎手マスタ (jockey_aliases): {len(rows)} 件読み込み")
                        jockey_loaded = True
                except Exception:
                    pass
            if not jockey_loaded:
                self._manual_jockey_data = []
                self._manual_jockeys_available = False
                self._log("[手動予測] jockeys / jockey_aliases テーブルが利用できません。コードを直接入力してください。")

            # 調教師: trainers (full name) → trainer_aliases (short name) → free text
            trainer_loaded = False
            try:
                rows = conn.execute(
                    "SELECT trainer_code, trainer_name FROM trainers ORDER BY trainer_name"
                ).fetchall()
                if rows:
                    self._manual_trainer_data = [(f"{r[1]} ({r[0]})", r[0]) for r in rows]
                    self._manual_trainers_available = True
                    self._log(f"[手動予測] 調教師マスタ (trainers): {len(rows)} 件読み込み")
                    trainer_loaded = True
                else:
                    self._log("[手動予測] trainers テーブルは 0 件です。trainer_aliases にフォールバックします。")
            except Exception:
                pass
            if not trainer_loaded:
                try:
                    rows = conn.execute(
                        "SELECT trainer_code, trainer_name_short FROM trainer_aliases ORDER BY trainer_name_short"
                    ).fetchall()
                    if rows:
                        self._manual_trainer_data = [(f"{r[1]} ({r[0]})", r[0]) for r in rows]
                        self._manual_trainers_available = True
                        self._log(f"[手動予測] 調教師マスタ (trainer_aliases): {len(rows)} 件読み込み")
                        trainer_loaded = True
                except Exception:
                    pass
            if not trainer_loaded:
                self._manual_trainer_data = []
                self._manual_trainers_available = False
                self._log("[手動予測] trainers / trainer_aliases テーブルが利用できません。コードを直接入力してください。")
        finally:
            conn.close()

        for r in range(self.manual_table.rowCount()):
            self._update_manual_row_widgets(r)

    def _update_manual_row_widgets(self, row: int) -> None:
        """指定行のウィジェットを現在のマスタデータに合わせて更新する。"""
        # 馬番: ドロップダウン (空 + 1..20)
        existing_widget = self.manual_table.cellWidget(row, _MANUAL_COL_HORSE_NO)
        if not isinstance(existing_widget, QComboBox):
            horse_no_combo = QComboBox()
            horse_no_combo.addItem("")
            horse_no_combo.addItems([str(n) for n in range(1, _HORSE_NO_MAX + 1)])
            self.manual_table.setCellWidget(row, _MANUAL_COL_HORSE_NO, horse_no_combo)

        if self._manual_horses_available and self._manual_horse_data:
            edit = _MasterLineEdit(self._manual_horse_data, "馬名 / horse_id")
            edit.editingFinished.connect(lambda r=row: self._on_manual_horse_changed(r))
            self.manual_table.setCellWidget(row, _MANUAL_COL_HORSE, edit)
        else:
            edit = QLineEdit()
            edit.setPlaceholderText("horse_id")
            edit.editingFinished.connect(lambda r=row: self._on_manual_horse_changed(r))
            self.manual_table.setCellWidget(row, _MANUAL_COL_HORSE, edit)

        if self._manual_jockeys_available and self._manual_jockey_data:
            self.manual_table.setCellWidget(
                row, _MANUAL_COL_JOCKEY, _MasterLineEdit(self._manual_jockey_data, "騎手コード")
            )
        else:
            edit = QLineEdit()
            edit.setPlaceholderText("騎手コード")
            self.manual_table.setCellWidget(row, _MANUAL_COL_JOCKEY, edit)

        if self._manual_trainers_available and self._manual_trainer_data:
            self.manual_table.setCellWidget(
                row, _MANUAL_COL_TRAINER, _MasterLineEdit(self._manual_trainer_data, "調教師コード")
            )
        else:
            edit = QLineEdit()
            edit.setPlaceholderText("調教師コード")
            self.manual_table.setCellWidget(row, _MANUAL_COL_TRAINER, edit)

    def _on_manual_add_row(self) -> None:
        """出走馬テーブルに 1 行追加する。"""
        r = self.manual_table.rowCount()
        self.manual_table.insertRow(r)
        self._update_manual_row_widgets(r)

    def _on_manual_remove_row(self) -> None:
        """出走馬テーブルの最終行を削除する。"""
        r = self.manual_table.rowCount()
        if r > 0:
            self.manual_table.removeRow(r - 1)

    def _on_manual_gen_rows(self) -> None:
        """出走頭数 N でテーブルをリセットし、馬番を 1..N で自動入力する。"""
        n = self.manual_nhorses_spin.value()
        # 既存行にデータがある場合は確認ダイアログを表示
        if self.manual_table.rowCount() > 0:
            has_data = False
            for row in range(self.manual_table.rowCount()):
                for col in range(self.manual_table.columnCount()):
                    widget = self.manual_table.cellWidget(row, col)
                    if widget is not None:
                        if isinstance(widget, QComboBox) and widget.currentText().strip():
                            has_data = True
                            break
                        if isinstance(widget, QLineEdit) and widget.text().strip():
                            has_data = True
                            break
                    item = self.manual_table.item(row, col)
                    if item and item.text().strip():
                        has_data = True
                        break
                if has_data:
                    break
            if has_data:
                reply = QMessageBox.question(
                    self,
                    "確認",
                    f"現在の入力内容をクリアして {n} 頭分の行を生成しますか？",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                )
                if reply != QMessageBox.StandardButton.Yes:
                    return
        self.manual_table.setRowCount(0)
        for i in range(1, n + 1):
            r = self.manual_table.rowCount()
            self.manual_table.insertRow(r)
            self._update_manual_row_widgets(r)
            horse_no_combo = self.manual_table.cellWidget(r, _MANUAL_COL_HORSE_NO)
            if isinstance(horse_no_combo, QComboBox):
                idx = horse_no_combo.findText(str(i))
                if idx >= 0:
                    horse_no_combo.setCurrentIndex(idx)

    def _on_distance_preset_changed(self, index: int) -> None:
        """距離プリセットが選択されたときスピンボックスの値を更新する。"""
        if index <= 0:
            return
        text = self.manual_distance_preset_combo.itemText(index)
        try:
            self.manual_distance_spin.setValue(int(text))
        except ValueError:
            # _DISTANCE_PRESETS は整数リストなので通常ここには到達しない
            pass

    def _on_manual_predict(self) -> None:
        """入力値を検証して推論を実行し、結果を予測テーブルに表示する。"""
        errors: list[str] = []

        course_code = self.manual_course_combo.currentData()
        if not course_code:
            # fall back to the edited text if no item data (e.g. user typed manually)
            course_code = self.manual_course_combo.currentText().strip()
        if not course_code:
            errors.append("競馬場コードを入力してください。")

        distance_m = self.manual_distance_spin.value()
        if distance_m <= 0:
            errors.append("距離 (m) を 1 以上で入力してください。")

        track_condition_text = self.manual_track_combo.currentText()
        track_code = _TRACK_CONDITION_MAP.get(track_condition_text, "")
        grade_code = self.manual_grade_edit.text().strip()

        # 馬場種別: モジュールレベルの特徴量列でサポートを確認
        # _PRED_FEAT_COLS はモジュール起動時に確定し以降変化しないため、
        # 予測ボタン押下ごとの参照で問題ない
        _surface_col_name: str | None = None
        if _PRED_FEAT_COLS:
            _surface_col_name = next(
                (c for c in _KNOWN_SURFACE_COL_NAMES if c in _PRED_FEAT_COLS), None
            )
        surface_text = self.manual_surface_combo.currentText()
        if _surface_col_name and not surface_text:
            errors.append("馬場種別 (芝/ダート) を選択してください。")

        if self.manual_table.rowCount() == 0:
            errors.append("出走馬を少なくとも 1 頭入力してください (「行追加」ボタン)。")

        entries: list[dict] = []
        for row in range(self.manual_table.rowCount()):
            row_label = f"行 {row + 1}"

            horse_no_widget = self.manual_table.cellWidget(row, _MANUAL_COL_HORSE_NO)
            if isinstance(horse_no_widget, QComboBox):
                horse_no_text = horse_no_widget.currentText().strip()
            else:
                horse_no_item = self.manual_table.item(row, _MANUAL_COL_HORSE_NO)
                horse_no_text = horse_no_item.text().strip() if horse_no_item else ""
            if not horse_no_text or not horse_no_text.isdigit():
                errors.append(f"{row_label}: 馬番をドロップダウンから選択してください。")
                continue

            horse_display, horse_id = self._get_manual_cell(row, _MANUAL_COL_HORSE)
            if not horse_id:
                errors.append(f"{row_label}: 馬名 / horse_id を入力してください。")
                continue

            jockey_display, jockey_code = self._get_manual_cell(row, _MANUAL_COL_JOCKEY)
            if not jockey_code:
                errors.append(f"{row_label}: 騎手を入力してください。")
                continue

            trainer_display, trainer_code = self._get_manual_cell(row, _MANUAL_COL_TRAINER)
            if not trainer_code:
                errors.append(f"{row_label}: 調教師を入力してください。")
                continue

            handicap_item = self.manual_table.item(row, _MANUAL_COL_HANDICAP)
            handicap_text = handicap_item.text().strip() if handicap_item else ""
            if not handicap_text:
                errors.append(f"{row_label}: 斤量を入力してください。")
                continue
            try:
                handicap_weight_x10 = round(float(handicap_text) * 10)
            except ValueError:
                errors.append(f"{row_label}: 斤量は数値で入力してください。")
                continue

            body_weight_item = self.manual_table.item(row, _MANUAL_COL_BODY_WEIGHT)
            body_weight_text = body_weight_item.text().strip() if body_weight_item else ""
            if not body_weight_text:
                errors.append(f"{row_label}: 馬体重 (必須) を入力してください。")
                continue
            try:
                body_weight = int(body_weight_text)
            except ValueError:
                errors.append(f"{row_label}: 馬体重は整数で入力してください。")
                continue

            entry: dict = {
                "horse_no": horse_no_text,
                "horse_id": horse_id,
                "horse_name": horse_display,
                "jockey_code": jockey_code,
                "jockey_name": jockey_display,
                "trainer_code": trainer_code,
                "trainer_name": trainer_display,
                "handicap_weight_x10": handicap_weight_x10,
                "body_weight": body_weight,
                "course_code": course_code,
                "distance_m": distance_m,
                "track_code": track_code,
                "grade_code": grade_code,
            }
            if _surface_col_name:
                entry[_surface_col_name] = surface_text
            entries.append(entry)

        if errors:
            QMessageBox.warning(
                self,
                "入力エラー",
                "以下の入力を確認してください:\n\n" + "\n".join(f"・{e}" for e in errors),
            )
            return

        self._save_snapshot("manual")

        model_path = self.model_edit.text().strip()
        if not model_path or not Path(model_path).exists():
            QMessageBox.warning(
                self, "エラー", f"モデルファイルが見つかりません:\n{model_path or '(未設定)'}"
            )
            return

        try:
            import pandas as _pd
            from catboost import CatBoostClassifier as _CBC
        except ImportError as exc:
            QMessageBox.critical(
                self, "インポートエラー", f"必要なライブラリが読み込めません:\n{exc}"
            )
            return

        # predict_place の特徴量定義 (モジュール起動時のインポートが失敗した場合は再試行)
        feat_cols = _PRED_FEAT_COLS
        num_feats = _PRED_NUM_FEATS
        cat_feats = _PRED_CAT_FEATS
        if feat_cols is None:
            try:
                from predict_place import (  # noqa: E402
                    CATEGORICAL_FEATURES as feat_cat,
                    FEATURE_COLS as feat_all,
                    NUMERIC_FEATURES as feat_num,
                )
                feat_cols, num_feats, cat_feats = feat_all, feat_num, feat_cat
            except ImportError as exc:
                QMessageBox.critical(
                    self, "インポートエラー", f"predict_place モジュールが読み込めません:\n{exc}"
                )
                return

        try:
            model = _CBC()
            model.load_model(model_path)
        except Exception as exc:
            QMessageBox.critical(
                self, "モデル読み込みエラー", f"モデルの読み込みに失敗しました:\n{exc}"
            )
            return

        df = _pd.DataFrame(entries)
        for col in num_feats:
            df[col] = _pd.to_numeric(df[col], errors="coerce")
        for col in cat_feats:
            df[col] = df[col].fillna("").astype(str)

        try:
            proba = model.predict_proba(df[feat_cols].copy())[:, 1]
        except Exception as exc:
            QMessageBox.critical(
                self, "推論エラー", f"予測の実行に失敗しました:\n{exc}"
            )
            return

        results: list[dict] = []
        for i, entry in enumerate(entries):
            results.append({
                "horse_no": entry["horse_no"],
                "horse_id": entry["horse_id"],
                "horse_name": entry["horse_name"],
                "jockey_name": entry["jockey_name"],
                "trainer_name": entry["trainer_name"],
                "p_place": round(float(proba[i]), 4),
            })

        results.sort(key=lambda r: r["p_place"], reverse=True)
        for i, r in enumerate(results, start=1):
            r["rank"] = i

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = self.outdir_edit.text().strip()
        if out_dir:
            pred_path = Path(out_dir) / f"pred_manual_{timestamp}.json"
            try:
                pred_path.write_text(
                    json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                self._log(f"[手動予測] 予測結果を保存: {pred_path}")
            except Exception as exc:
                self._log(f"[手動予測] ファイル保存失敗: {exc}")

        self._pred_rows_cache = results
        self._apply_pred_filters()
        self._results_box.setCollapsed(False)

        parts = [f"競馬場: {course_code}", f"{distance_m}m", track_condition_text]
        if grade_code:
            parts.append(f"Grade: {grade_code}")
        parts.append(f"出走{len(entries)}頭")
        self.race_header_label.setText("手動入力  |  " + "  |  ".join(parts))
        self.race_header_label.setVisible(True)
        self.bets_table.setRowCount(0)

        self._log(f"[手動予測] 推論完了: {len(entries)} 頭")
        for r in results[:3]:
            self._log(
                f"  馬番{r['horse_no']} {r.get('horse_name') or r.get('horse_id', '')} "
                f"p_place={r['p_place']:.4f}"
            )

    # ── スナップショット履歴 ───────────────────────────

    def _collect_gui_snapshot(self, kind: str) -> dict:
        """現在の GUI 入力状態をスナップショット dict として収集して返す。"""
        entries: list[dict] = []
        for row in range(self.manual_table.rowCount()):
            horse_no_widget = self.manual_table.cellWidget(row, _MANUAL_COL_HORSE_NO)
            if isinstance(horse_no_widget, QComboBox):
                horse_no = horse_no_widget.currentText().strip()
            else:
                item = self.manual_table.item(row, _MANUAL_COL_HORSE_NO)
                horse_no = item.text().strip() if item else ""

            horse_display, horse_id = self._get_manual_cell(row, _MANUAL_COL_HORSE)
            jockey_display, jockey_code = self._get_manual_cell(row, _MANUAL_COL_JOCKEY)
            trainer_display, trainer_code = self._get_manual_cell(row, _MANUAL_COL_TRAINER)

            handicap_item = self.manual_table.item(row, _MANUAL_COL_HANDICAP)
            handicap = handicap_item.text().strip() if handicap_item else ""

            bw_item = self.manual_table.item(row, _MANUAL_COL_BODY_WEIGHT)
            body_weight = bw_item.text().strip() if bw_item else ""

            entries.append({
                "horse_no": horse_no,
                "horse_display": horse_display,
                "horse_id": horse_id,
                "jockey_display": jockey_display,
                "jockey_code": jockey_code,
                "trainer_display": trainer_display,
                "trainer_code": trainer_code,
                "handicap": handicap,
                "body_weight": body_weight,
            })

        course_code = self.manual_course_combo.currentData() or self.manual_course_combo.currentText().strip()

        return {
            "snapshot_type": kind,
            "timestamp": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "db_path": self.db_edit.text().strip(),
            "model_path": self.model_edit.text().strip(),
            "out_dir": self.outdir_edit.text().strip(),
            "race_keys": self.racekeys_edit.text().strip(),
            "date": self.date_edit.date().toString("yyyyMMdd"),
            "course_code": course_code,
            "distance_m": self.manual_distance_spin.value(),
            "track_condition": self.manual_track_combo.currentText(),
            "grade_code": self.manual_grade_edit.text().strip(),
            "surface": self.manual_surface_combo.currentText(),
            "entries": entries,
            "wide_model": self.wide_model_edit.text().strip(),
            "sanrenpuku_model": self.sanrenpuku_model_edit.text().strip(),
            "topn": self.combo_topn_spin.value(),
        }

    def _save_snapshot(self, kind: str) -> None:
        """現在の GUI 状態を ~/.keiba/presets/ にタイムスタンプ付きで保存する。"""
        try:
            PRESETS_DIR.mkdir(parents=True, exist_ok=True)
            data = self._collect_gui_snapshot(kind)
            ts = data["timestamp"]
            path = PRESETS_DIR / f"{ts}_{kind}.json"
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            self._log(f"[履歴] スナップショットを保存: {path.name}")
        except Exception as exc:
            self._log(f"[履歴] スナップショット保存失敗: {exc}")
            return
        self._prune_snapshots()

    def _prune_snapshots(self) -> None:
        """PRESETS_DIR 内のスナップショットを最新 _MAX_SNAPSHOTS 件だけ保持する。"""
        try:
            files = sorted(
                [f for f in PRESETS_DIR.glob("*.json") if _SNAPSHOT_RE.match(f.name)],
                key=lambda f: f.name,
                reverse=True,
            )
        except Exception as exc:
            self._log(f"[履歴] スナップショット一覧取得失敗: {exc}")
            return
        for old in files[_MAX_SNAPSHOTS:]:
            try:
                old.unlink()
            except Exception as exc:
                self._log(f"[履歴] 古いスナップショット削除失敗: {old.name}: {exc}")

    def _apply_snapshot(self, data: dict) -> None:
        """スナップショット dict の内容を GUI に反映する。"""
        if "db_path" in data:
            self.db_edit.setText(data["db_path"])
        if "model_path" in data:
            self.model_edit.setText(data["model_path"])
        if "out_dir" in data:
            self.outdir_edit.setText(data["out_dir"])
        if "race_keys" in data:
            self.racekeys_edit.setText(data["race_keys"])
        if "date" in data:
            d = QDate.fromString(data["date"], "yyyyMMdd")
            if d.isValid():
                self.date_edit.setDate(d)

        if "course_code" in data:
            code = data["course_code"]
            idx = self.manual_course_combo.findData(code)
            if idx >= 0:
                self.manual_course_combo.setCurrentIndex(idx)
            else:
                self.manual_course_combo.setCurrentText(code)
        if "distance_m" in data:
            self.manual_distance_spin.setValue(data["distance_m"])
        if "track_condition" in data:
            idx = self.manual_track_combo.findText(data["track_condition"])
            if idx >= 0:
                self.manual_track_combo.setCurrentIndex(idx)
        if "grade_code" in data:
            self.manual_grade_edit.setText(data["grade_code"])
        if "surface" in data:
            idx = self.manual_surface_combo.findText(data["surface"])
            if idx >= 0:
                self.manual_surface_combo.setCurrentIndex(idx)

        if "entries" in data:
            self.manual_table.setRowCount(0)
            for entry in data["entries"]:
                r = self.manual_table.rowCount()
                self.manual_table.insertRow(r)
                self._update_manual_row_widgets(r)

                horse_no_widget = self.manual_table.cellWidget(r, _MANUAL_COL_HORSE_NO)
                if isinstance(horse_no_widget, QComboBox):
                    idx = horse_no_widget.findText(entry.get("horse_no", ""))
                    if idx >= 0:
                        horse_no_widget.setCurrentIndex(idx)

                horse_widget = self.manual_table.cellWidget(r, _MANUAL_COL_HORSE)
                if isinstance(horse_widget, QLineEdit):
                    horse_widget.setText(entry.get("horse_display") or entry.get("horse_id", ""))

                jockey_widget = self.manual_table.cellWidget(r, _MANUAL_COL_JOCKEY)
                if isinstance(jockey_widget, QLineEdit):
                    jockey_widget.setText(entry.get("jockey_display") or entry.get("jockey_code", ""))

                trainer_widget = self.manual_table.cellWidget(r, _MANUAL_COL_TRAINER)
                if isinstance(trainer_widget, QLineEdit):
                    trainer_widget.setText(entry.get("trainer_display") or entry.get("trainer_code", ""))

                self.manual_table.setItem(r, _MANUAL_COL_HANDICAP, QTableWidgetItem(entry.get("handicap", "")))
                self.manual_table.setItem(r, _MANUAL_COL_BODY_WEIGHT, QTableWidgetItem(entry.get("body_weight", "")))

        if "wide_model" in data:
            self.wide_model_edit.setText(data["wide_model"])
        if "sanrenpuku_model" in data:
            self.sanrenpuku_model_edit.setText(data["sanrenpuku_model"])
        if "topn" in data:
            self.combo_topn_spin.setValue(data["topn"])

        kind = data.get("snapshot_type", "")
        ts = data.get("timestamp", "")
        self._log(f"[履歴] スナップショットを読み込みました: {kind}  {ts}")

    def _on_load_snapshot(self) -> None:
        """スナップショット履歴ダイアログを表示し、選択されたスナップショットを復元する。"""
        if not PRESETS_DIR.exists():
            QMessageBox.information(self, "履歴", "保存されたスナップショットがありません。")
            return

        files = sorted(PRESETS_DIR.glob("*.json"), reverse=True)
        if not files:
            QMessageBox.information(self, "履歴", "保存されたスナップショットがありません。")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("スナップショット履歴")
        dlg.resize(500, 380)
        layout = QVBoxLayout(dlg)

        layout.addWidget(QLabel("読み込むスナップショットを選択してください:"))

        list_widget = QListWidget()
        for f in files:
            try:
                d = json.loads(f.read_text(encoding="utf-8"))
                kind = d.get("snapshot_type", "")
                ts = d.get("timestamp", f.stem)
                label = f"{ts}  [{kind}]"
            except Exception:
                label = f.stem
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, str(f))
            list_widget.addItem(item)
        layout.addWidget(list_widget)

        btn_row = QHBoxLayout()
        load_btn = QPushButton("読み込む")
        load_btn.setEnabled(False)
        delete_btn = QPushButton("削除")
        delete_btn.setEnabled(False)
        cancel_btn = QPushButton("キャンセル")
        btn_row.addWidget(load_btn)
        btn_row.addWidget(delete_btn)
        btn_row.addStretch()
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)

        def _on_selection_changed() -> None:
            has = bool(list_widget.selectedItems())
            load_btn.setEnabled(has)
            delete_btn.setEnabled(has)

        list_widget.itemSelectionChanged.connect(_on_selection_changed)
        list_widget.itemDoubleClicked.connect(lambda _item: _on_load())
        cancel_btn.clicked.connect(dlg.reject)

        def _on_load() -> None:
            selected = list_widget.selectedItems()
            if not selected:
                return
            path = Path(selected[0].data(Qt.ItemDataRole.UserRole))
            try:
                snap = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                QMessageBox.critical(dlg, "エラー", f"ファイル読み込み失敗:\n{exc}")
                return
            dlg.accept()
            self._apply_snapshot(snap)

        def _on_delete() -> None:
            selected = list_widget.selectedItems()
            if not selected:
                return
            path = Path(selected[0].data(Qt.ItemDataRole.UserRole))
            reply = QMessageBox.question(
                dlg,
                "確認",
                f"削除しますか？\n{path.name}",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                return
            try:
                path.unlink()
                row = list_widget.row(selected[0])
                list_widget.takeItem(row)
            except Exception as exc:
                QMessageBox.critical(dlg, "エラー", f"削除失敗:\n{exc}")

        load_btn.clicked.connect(_on_load)
        delete_btn.clicked.connect(_on_delete)

        dlg.exec()


def main() -> None:
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
