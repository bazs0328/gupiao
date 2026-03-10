from __future__ import annotations

import ctypes
import logging
import os
from pathlib import Path
import socket
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request


APP_NAME = "Gupiao Lab"
APP_SLUG = "gupiao-desktop-mvp"


def _bundle_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


def _user_root() -> Path:
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / APP_SLUG
    return Path.home() / f".{APP_SLUG}"


def _insert_sys_path(path: Path) -> None:
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _bootstrap_paths() -> None:
    bundle_root = _bundle_root()
    _insert_sys_path(bundle_root)
    _insert_sys_path(bundle_root / ".pydeps")
    _insert_sys_path(bundle_root / ".pydeps-build")


def _setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "desktop.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8")],
        force=True,
    )
    return log_path


def _show_error(message: str) -> None:
    if sys.platform == "win32":
        ctypes.windll.user32.MessageBoxW(None, message, APP_NAME, 0x10)
        return
    print(message, file=sys.stderr)


def _find_available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class DesktopRuntime:
    def __init__(self) -> None:
        self.bundle_root = _bundle_root()
        self.user_root = _user_root()
        self.data_dir = self.user_root / "data"
        self.log_dir = self.user_root / "logs"
        if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
            self.frontend_dir = self.bundle_root / "dist" / "frontend"
        else:
            self.frontend_dir = self.bundle_root / ".build" / "frontend"
        self.host = "127.0.0.1"
        self.port = _find_available_port()
        self.base_url = f"http://{self.host}:{self.port}"
        self.server = None
        self.server_thread: threading.Thread | None = None
        self.server_error: BaseException | None = None

    def prepare_environment(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        os.environ["GUPIAO_DATA_DIR"] = str(self.data_dir)
        os.environ["GUPIAO_HOST"] = self.host
        os.environ["GUPIAO_PORT"] = str(self.port)
        os.environ["GUPIAO_FRONTEND_DIR"] = str(self.frontend_dir)

    def start(self) -> None:
        if not self.frontend_dir.exists():
            raise FileNotFoundError(f"Missing frontend bundle: {self.frontend_dir}")

        self.prepare_environment()
        from uvicorn import Config, Server

        from backend.app.config import AppSettings
        from backend.app.main import create_app

        settings = AppSettings.from_env()
        app = create_app(settings=settings, frontend_dir=self.frontend_dir)
        # PyInstaller windowed apps do not expose a real stdout/stderr stream.
        # Disable uvicorn's default stream logging config and reuse the file logger.
        config = Config(app=app, host=self.host, port=self.port, log_level="info", access_log=False, log_config=None)
        self.server = Server(config)
        self.server.install_signal_handlers = lambda: None

        def run_server() -> None:
            try:
                assert self.server is not None
                self.server.run()
            except BaseException as exc:  # pragma: no cover - launcher integration only
                self.server_error = exc
                logging.exception("Desktop backend server crashed.")

        self.server_thread = threading.Thread(target=run_server, name="gupiao-backend", daemon=True)
        self.server_thread.start()
        self.wait_until_ready()

    def wait_until_ready(self, timeout_seconds: float = 25.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        health_url = f"{self.base_url}/health"
        while time.monotonic() < deadline:
            if self.server_error:
                raise RuntimeError("Embedded backend failed to start.") from self.server_error
            try:
                with urllib.request.urlopen(health_url, timeout=1.0) as response:
                    if response.status == 200:
                        logging.info("Desktop backend is ready at %s", self.base_url)
                        return
            except (urllib.error.URLError, TimeoutError):
                time.sleep(0.2)
        raise TimeoutError(f"Timed out waiting for backend readiness at {health_url}")

    def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5.0)


def main() -> None:
    _bootstrap_paths()
    runtime = DesktopRuntime()
    log_path = _setup_logging(runtime.log_dir)
    logging.info("Launching desktop runtime from %s", runtime.bundle_root)

    try:
        runtime.start()
        import webview

        window = webview.create_window(
            APP_NAME,
            runtime.base_url,
            width=1540,
            height=980,
            min_size=(1280, 840),
            background_color="#071216",
        )
        window.events.closed += lambda: runtime.stop()
        webview.start(debug=not getattr(sys, "frozen", False))
    except BaseException:  # pragma: no cover - launcher integration only
        runtime.stop()
        detail = traceback.format_exc()
        logging.exception("Desktop launcher failed.")
        _show_error(f"桌面应用启动失败。\n\n日志: {log_path}\n\n{detail[-1600:]}")
        raise


if __name__ == "__main__":
    main()
