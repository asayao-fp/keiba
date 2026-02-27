"""
gui/main.py
===========
JV-Link 更新 & 複勝買い目提案 GUI (PySide6)

起動方法:
  # 64-bit venv を有効化してから
  python gui/main.py
"""

import csv
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Callable

from PySide6.QtCore import QDate, QProcess, Qt, QUrl
from PySide6.QtGui import QCloseEvent, QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QDateEdit,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

# リポジトリルート (gui/ の親ディレクトリ)
REPO_ROOT = Path(__file__).resolve().parent.parent

# scripts/ ディレクトリの絶対パス
SCRIPTS_DIR = REPO_ROOT / "scripts"

# scripts/ を sys.path に追加して fetch_races をインポート
sys.path.insert(0, str(SCRIPTS_DIR))
from list_races import fetch_races  # noqa: E402

# 設定ファイルのパス
CONFIG_PATH = REPO_ROOT / ".keiba_gui_config.json"

# レース選択テーブルの列ヘッダー
_RACE_TABLE_COLS = ["✓", "レース名", "競馬場", "R", "距離", "馬場", "グレード", "race_key"]

# 予想結果テーブルの列ヘッダー
_SUMMARY_TABLE_COLS = ["レース名", "競馬場", "R", "S", "買い目数", "賭金計", "期待値計", "avg p", "F/B", "race_key"]
_BETS_TABLE_COLS = ["馬番", "賭金", "p_place", "オッズ使用", "期待値(円)", "EV/1unit"]
_PRED_TABLE_COLS = ["馬番", "馬ID", "p_place"]


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


# ── メインウィンドウ ──────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Keiba Pipeline GUI")
        self.resize(740, 1000)

        # 実行中プロセス管理
        self._processes: list[QProcess] = []
        self._cancelled = False

        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        # ── 設定フォーム ──────────────────────────────
        form_group = QGroupBox("設定")
        form = QFormLayout(form_group)

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

        root_layout.addWidget(form_group)

        # ── レース選択 ──────────────────────────────────
        races_group = QGroupBox("レース選択")
        races_layout = QVBoxLayout(races_group)

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

        root_layout.addWidget(races_group)

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

        # ── ログ出力 ──────────────────────────────────
        log_group = QGroupBox("ログ")
        log_layout = QVBoxLayout(log_group)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumBlockCount(5000)
        log_layout.addWidget(self.log_view)

        clear_btn = QPushButton("ログをクリア")
        clear_btn.clicked.connect(self.log_view.clear)
        log_layout.addWidget(clear_btn, alignment=Qt.AlignmentFlag.AlignRight)

        root_layout.addWidget(log_group)

        # ── 予想結果 ──────────────────────────────────
        results_group = QGroupBox("予想結果")
        results_layout = QVBoxLayout(results_group)

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

        # 買い目テーブル
        results_layout.addWidget(QLabel("買い目:"))
        self.bets_table = QTableWidget(0, len(_BETS_TABLE_COLS))
        self.bets_table.setHorizontalHeaderLabels(_BETS_TABLE_COLS)
        self.bets_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.bets_table.verticalHeader().setVisible(False)
        self.bets_table.setMinimumHeight(60)
        results_layout.addWidget(self.bets_table)

        # 予測テーブル
        results_layout.addWidget(QLabel("予測:"))
        self.pred_table = QTableWidget(0, len(_PRED_TABLE_COLS))
        self.pred_table.setHorizontalHeaderLabels(_PRED_TABLE_COLS)
        self.pred_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.pred_table.verticalHeader().setVisible(False)
        self.pred_table.setMinimumHeight(80)
        results_layout.addWidget(self.pred_table)

        root_layout.addWidget(results_group)

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

        # ウィンドウサイズ
        geom = cfg.get("window_geometry")
        if geom:
            try:
                self.resize(geom["width"], geom["height"])
                if "x" in geom and "y" in geom:
                    self.move(geom["x"], geom["y"])
            except Exception:
                pass

    def _save_settings(self) -> None:
        geom = self.geometry()
        py32_cmd = display_to_py32(self.py32_edit.text())
        save_config({
            "db_path": self.db_edit.text().strip(),
            "model_path": self.model_edit.text().strip(),
            "out_dir": self.outdir_edit.text().strip(),
            "python32_cmd": py32_to_display(py32_cmd),
            "last_from_date": self.date_edit.date().toString("yyyyMMdd"),
            "race_keys": self.racekeys_edit.text().strip(),
            "search_keyword": self.keyword_edit.text().strip(),
            "weekend_filter": self.weekend_chk.isChecked(),
            "window_geometry": {
                "x": geom.x(),
                "y": geom.y(),
                "width": geom.width(),
                "height": geom.height(),
            },
        })

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

        # 予測テーブルを更新
        self.pred_table.setRowCount(0)
        pred_path = Path(out_dir) / f"pred_{race_key}.json"
        if pred_path.exists():
            try:
                preds: list[dict] = json.loads(pred_path.read_text(encoding="utf-8"))
                for pred in preds:
                    r = self.pred_table.rowCount()
                    self.pred_table.insertRow(r)
                    self.pred_table.setItem(r, 0, QTableWidgetItem(str(pred.get("horse_no", ""))))
                    self.pred_table.setItem(r, 1, QTableWidgetItem(str(pred.get("horse_id", ""))))
                    self.pred_table.setItem(r, 2, QTableWidgetItem(str(pred.get("p_place", ""))))
            except Exception as e:
                self._log(f"[結果] 予測ファイル読み込み失敗: {e}")
        else:
            self._log(f"[結果] 予測ファイルが見つかりません: {pred_path}")

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


def main() -> None:
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
