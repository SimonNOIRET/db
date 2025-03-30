import sys
import os
import subprocess
import threading
import webbrowser
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QFrame, QSpacerItem, QSizePolicy, QLabel
)
from PyQt6.QtGui import QIcon, QFont, QTextCursor
from PyQt6.QtCore import Qt, QTimer

MARKET_DATA_FOLDER = "C:/Users/Simon/Documents/ArkeaAM/VSCode/lexifi_mkt_data"
SCRIPTS = {
    "📈 Import Market Data": "lexifi_mkt_data_update.py",
    "📤 Upload Spot Data": "postgreSQL_mkt_data_asset_spot_update.py",
    "📤 Upload Forward Data": "postgreSQL_mkt_data_asset_fwd_update.py",
}
STREAMLIT_APP = "lexifi_mkt_data_asset_viz.py"
APP_ICON_PATH = "C:/Users/Simon/Documents/ArkeaAM/VSCode/icons/AAM_1.png"

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("MARKET DATA UPDATER")
        self.setGeometry(100, 50, 1300, 900)
        self.setMinimumSize(1000, 800)

        if os.path.exists(APP_ICON_PATH):
            self.setWindowIcon(QIcon(APP_ICON_PATH))

        self.process = None
        self.running = False
        self.log_buffer = []

        self.flush_timer = QTimer()
        self.flush_timer.timeout.connect(self.flush_console)
        self.flush_timer.start(100)

        main_layout = QHBoxLayout()
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        menu = QFrame()
        menu.setFixedWidth(270)
        menu.setStyleSheet("background-color: #1e1e1e; border-radius: 10px;")
        menu_layout = QVBoxLayout(menu)
        menu_layout.setContentsMargins(15, 15, 15, 15)
        menu_layout.setSpacing(15)

        title1 = QLabel("🗄️  Database Updater")
        title1.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        title1.setStyleSheet("color: #bbbbbb;")
        menu_layout.addWidget(title1)

        for label, script in SCRIPTS.items():
            self.add_script_button(menu_layout, label, script)

        self.add_open_folder_button(menu_layout)

        title2 = QLabel("📊  Data Visualization")
        title2.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        title2.setStyleSheet("color: #bbbbbb; margin-top: 10px;")
        menu_layout.addSpacing(10)
        menu_layout.addWidget(title2)

        self.add_streamlit_button(menu_layout, "🔍 Data Monitor", STREAMLIT_APP)

        menu_layout.addItem(QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding))

        self.add_stop_button(menu_layout)

        console_area = QVBoxLayout()

        self.console = QTextEdit()
        self.console.setAcceptRichText(True)
        self.console.setReadOnly(True)
        self.console.setFont(QFont("Segoe UI", 9))
        self.console.setStyleSheet("""
            background-color: #111111;
            color: #e0e0e0;
            padding: 10px;
            border-radius: 8px;
        """)

        clear_btn = QPushButton("🧹 Clear Console")
        self.style_button(clear_btn)
        clear_btn.clicked.connect(lambda: self.console.clear())

        console_area.addWidget(self.console)
        console_area.addWidget(clear_btn)

        main_layout.addWidget(menu)
        main_layout.addLayout(console_area, stretch=1)

    def add_script_button(self, layout, text, script):
        btn = QPushButton(text)
        self.style_button(btn)
        btn.clicked.connect(lambda: self.run_script(script))
        layout.addWidget(btn)

    def add_streamlit_button(self, layout, text, script_name):
        btn = QPushButton(text)
        self.style_button(btn)
        btn.clicked.connect(lambda: self.run_streamlit_app(script_name))
        layout.addWidget(btn)

    def run_streamlit_app(self, script_name):
        try:
            subprocess.Popen(
                ["streamlit", "run", script_name],
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            webbrowser.open("http://localhost:8501")
            self.log_buffer.append(self.format_console_line(f"🌐 Streamlit app lancée : {script_name}"))
            self.log_buffer.append(self.format_console_line("Navigateur ouvert sur http://localhost:8501"))

        except Exception as e:
            self.log_buffer.append(self.format_console_line(f"❌ Erreur lancement Streamlit : {e}"))

    def add_open_folder_button(self, layout):
        btn = QPushButton("📂 Open Market Data Folder")
        self.style_button(btn)
        btn.clicked.connect(lambda: webbrowser.open(MARKET_DATA_FOLDER))
        layout.addWidget(btn)

    def add_stop_button(self, layout):
        btn = QPushButton("🛑 Stop Script")
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn.setFixedHeight(40)
        btn.setFont(QFont("Segoe UI", 9))
        btn.setStyleSheet("""
            QPushButton {
                text-align: left;
                padding-left: 12px;
                background-color: #770000;
                color: white;
                border: none;
                border-radius: 6px;
                transition: background-color 0.3s ease;
            }
            QPushButton:hover {
                background-color: #aa0000;
            }
        """)
        btn.clicked.connect(self.stop_script)
        layout.addWidget(btn)

    def style_button(self, btn):
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn.setFixedHeight(40)
        btn.setFont(QFont("Segoe UI", 9))
        btn.setStyleSheet("""
            QPushButton {
                text-align: left;
                padding-left: 12px;
                background-color: #2c2c2c;
                color: white;
                border: none;
                border-radius: 6px;
                transition: background-color 0.3s ease;
            }
            QPushButton:hover {
                background-color: #444444;
            }
        """)

    def run_script(self, script_name):
        if self.running:
            self.log_buffer.append(self.format_console_line("⚠️ Un script est déjà en cours. Stoppez-le avant de relancer."))
            return

        self.log_buffer.append(self.format_console_line(f"\n▶️ Lancement de `{script_name}`...\n"))
        self.running = True
        threading.Thread(target=self.execute_script, args=(script_name,), daemon=True).start()

    def stop_script(self):
        if self.process and self.running:
            self.process.terminate()
            self.running = False
            self.log_buffer.append(self.format_console_line("🛑 Script interrompu par l'utilisateur."))

    def flush_console(self):
        if self.log_buffer:
            self.console.append('\n'.join(self.log_buffer))
            self.console.moveCursor(QTextCursor.MoveOperation.End)
            self.log_buffer.clear()

    def format_console_line(self, line: str) -> str:
        if "❌" in line or "Erreur" in line:
            return f'<span style="color: #ff5555;">{line}</span>'
        elif "✅" in line or "terminé" in line:
            return f'<span style="color: #88ff88;">{line}</span>'
        elif "⚠️" in line or "attention" in line.lower():
            return f'<span style="color: #ffaa00;">{line}</span>'
        elif "▶️" in line or "Running" in line:
            return f'<span style="color: #5dcaff;">{line}</span>'
        else:
            return f'<span style="color: #e0e0e0;">{line}</span>'

    def execute_script(self, script_name):
        python_exec = sys.executable
        try:
            self.process = subprocess.Popen(
                [python_exec, script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env={**os.environ, "PYTHONIOENCODING": "utf-8"}
            )

            for line in self.process.stdout:
                self.log_buffer.append(self.format_console_line(line.rstrip()))

        except Exception as e:
            self.log_buffer.append(self.format_console_line(f"❌ Erreur : {e}"))
        finally:
            self.running = False
            self.process = None
            self.log_buffer.append(self.format_console_line("\n✅ Script terminé.\n"))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet("QMainWindow { background-color: #101010; }")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())



