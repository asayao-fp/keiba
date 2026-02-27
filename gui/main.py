"""
gui/main.py
===========
JV-Link 更新 & 複勝買い目提案 GUI (PySide6)

起動方法:
  # 64-bit venv を有効化してから
  python gui/main.py
"""

import os
import sys
from typing import Callable

from PySide6.QtCore import QProcess, Qt
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

# scripts/ ディレクトリの絶対パス (このファイルの親ディレクトリの ../scripts)
SCRIPTS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")
)


def _script(name: str) -> str:
    return os.path.join(SCRIPTS_DIR, name)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Keiba Pipeline GUI")
        self.resize(700, 620)

        # 実行中プロセス管理
        self._processes: list[QProcess] = []

        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        # ── 設定フォーム ──────────────────────────────
        form_group = QGroupBox("設定")
        form = QFormLayout(form_group)

        self.db_edit = QLineEdit("jv_data.db")
        form.addRow("DB パス:", self._with_browse(self.db_edit, file=True))

        self.date_edit = QLineEdit()
        self.date_edit.setPlaceholderText("例: 20240101")
        form.addRow("取込開始日 (YYYYMMDD):", self.date_edit)

        self.outdir_edit = QLineEdit()
        self.outdir_edit.setPlaceholderText("必須 — 出力ディレクトリを選択")
        form.addRow("出力ディレクトリ:", self._with_browse(self.outdir_edit, file=False))

        self.model_edit = QLineEdit("models/place_model.cbm")
        form.addRow("モデルパス:", self._with_browse(self.model_edit, file=True))

        self.py32_edit = QLineEdit()
        self.py32_edit.setPlaceholderText("必須 — 32-bit python.exe を選択")
        form.addRow("32-bit Python 実行ファイル:", self._with_browse(self.py32_edit, file=True))

        self.racekeys_edit = QLineEdit()
        self.racekeys_edit.setPlaceholderText(
            "レースキー (スペース区切り) — Suggest ボタン用"
        )
        form.addRow("レースキー:", self.racekeys_edit)

        root_layout.addWidget(form_group)

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

    # ── ウィジェットヘルパー ──────────────────────────

    def _with_browse(self, edit: QLineEdit, file: bool) -> QWidget:
        """LineEdit + Browse ボタンを横並びにしたウィジェットを返す。"""
        container = QWidget()
        h = QHBoxLayout(container)
        h.setContentsMargins(0, 0, 0, 0)
        h.addWidget(edit)
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

    # ── Update (RACE) ─────────────────────────────────

    def _on_update(self) -> None:
        db = self.db_edit.text().strip()
        from_date = self.date_edit.text().strip()
        py32 = self.py32_edit.text().strip()

        if not self._require(db, "DB パス"):
            return
        if not self._require(from_date, "取込開始日"):
            return
        if not self._require(py32, "32-bit Python 実行ファイル"):
            return

        self._log("=" * 60)
        self._log("[Update] Step 1/2: jv_ingest_raw.py (32-bit Python)")

        ingest_cmd = [
            py32,
            _script("jv_ingest_raw.py"),
            "--from-date", from_date,
            "--dataspec", "RACE",
            "--db", db,
        ]

        update_cmd = [
            sys.executable,
            _script("update_db_from_raw.py"),
            "--db", db,
            "--skip-masters",
        ]

        self.update_btn.setEnabled(False)
        self._run_sequential([ingest_cmd, update_cmd], on_finish=self._on_update_done)

    def _on_update_done(self, success: bool) -> None:
        self.update_btn.setEnabled(True)
        if success:
            self._log("[Update] 完了")
        else:
            self._log("[Update] エラーで終了しました")

    # ── Suggest ──────────────────────────────────────

    def _on_suggest(self) -> None:
        db = self.db_edit.text().strip()
        out_dir = self.outdir_edit.text().strip()
        model = self.model_edit.text().strip()
        race_keys_raw = self.racekeys_edit.text().strip()

        if not self._require(db, "DB パス"):
            return
        if not self._require(out_dir, "出力ディレクトリ"):
            return
        if not self._require(model, "モデルパス"):
            return
        if not self._require(race_keys_raw, "レースキー"):
            return

        race_keys = race_keys_raw.split()

        self._log("=" * 60)
        self._log("[Suggest] batch_suggest_place_bets.py を実行します")

        cmd = [
            sys.executable,
            _script("batch_suggest_place_bets.py"),
            "--db", db,
            "--model", model,
            "--out-dir", out_dir,
            "--race-keys", *race_keys,
        ]

        self.suggest_btn.setEnabled(False)
        self._run_sequential([cmd], on_finish=self._on_suggest_done)

    def _on_suggest_done(self, success: bool) -> None:
        self.suggest_btn.setEnabled(True)
        if success:
            self._log("[Suggest] 完了")
        else:
            self._log("[Suggest] エラーで終了しました")

    # ── プロセス実行 ──────────────────────────────────

    def _run_sequential(
        self,
        commands: list[list[str]],
        on_finish: Callable[[bool], None],
        _index: int = 0,
    ) -> None:
        """commands をインデックス順に逐次実行し、全完了後に on_finish(success) を呼ぶ。"""
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
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            text = str(data)
        for line in text.splitlines():
            self._log(line)


def main() -> None:
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
