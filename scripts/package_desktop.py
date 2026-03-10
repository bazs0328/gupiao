from __future__ import annotations

import hashlib
import importlib
from pathlib import Path
import shutil
import subprocess
import sys


ROOT_DIR = Path(__file__).resolve().parents[1]
BUILD_DEPS_DIR = ROOT_DIR / ".pydeps-build"
BUILD_STATE_FILE = BUILD_DEPS_DIR / ".requirements.sha256"
DESKTOP_REQUIREMENTS = ROOT_DIR / "backend" / "requirements-desktop.txt"
FRONTEND_DIR = ROOT_DIR / ".build" / "frontend"
OUTPUT_DIR = ROOT_DIR / "dist"
BUILD_DIR = ROOT_DIR / ".pyinstaller" / "build"
SPEC_DIR = ROOT_DIR / ".pyinstaller"
APP_NAME = "Gupiao Lab"
LEGACY_FRONTEND_DIR = OUTPUT_DIR / "frontend"


def _read_requirements_hash() -> str:
    payload = DESKTOP_REQUIREMENTS.read_bytes()
    return hashlib.sha256(payload).hexdigest()


def _ensure_build_deps() -> None:
    BUILD_DEPS_DIR.mkdir(parents=True, exist_ok=True)
    expected_hash = _read_requirements_hash()
    installed_hash = BUILD_STATE_FILE.read_text(encoding="utf-8").strip() if BUILD_STATE_FILE.exists() else None
    if installed_hash == expected_hash:
        try:
            if str(BUILD_DEPS_DIR) not in sys.path:
                sys.path.insert(0, str(BUILD_DEPS_DIR))
            import PyInstaller  # noqa: F401
            return
        except ModuleNotFoundError:
            pass

    if BUILD_DEPS_DIR.exists():
        shutil.rmtree(BUILD_DEPS_DIR)
    BUILD_DEPS_DIR.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--target",
            str(BUILD_DEPS_DIR),
            "-r",
            str(DESKTOP_REQUIREMENTS),
        ],
        check=True,
    )
    BUILD_STATE_FILE.write_text(expected_hash, encoding="utf-8")


def _bootstrap_imports() -> None:
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))
    if str(BUILD_DEPS_DIR) not in sys.path:
        sys.path.insert(0, str(BUILD_DEPS_DIR))
    importlib.invalidate_caches()


def _format_add_data(source: Path, destination: str) -> str:
    separator = ";" if sys.platform == "win32" else ":"
    return f"{source}{separator}{destination}"


def main() -> None:
    if not FRONTEND_DIR.joinpath("index.html").exists():
        raise FileNotFoundError("Frontend bundle is missing. Run `npm run build:web` first.")

    if LEGACY_FRONTEND_DIR.exists():
        shutil.rmtree(LEGACY_FRONTEND_DIR)

    _ensure_build_deps()
    _bootstrap_imports()

    from PyInstaller.__main__ import run as pyinstaller_run

    target_dir = OUTPUT_DIR / APP_NAME
    if target_dir.exists():
        shutil.rmtree(target_dir)

    pyinstaller_run(
        [
            "--noconfirm",
            "--clean",
            "--windowed",
            "--onedir",
            "--name",
            APP_NAME,
            "--distpath",
            str(OUTPUT_DIR),
            "--workpath",
            str(BUILD_DIR),
            "--specpath",
            str(SPEC_DIR),
            "--paths",
            str(ROOT_DIR),
            "--paths",
            str(BUILD_DEPS_DIR),
            "--add-data",
            _format_add_data(FRONTEND_DIR, "dist/frontend"),
            "--collect-all",
            "webview",
            "--collect-all",
            "akshare",
            "--collect-all",
            "py_mini_racer",
            "--collect-submodules",
            "uvicorn",
            str(ROOT_DIR / "desktop_launcher.py"),
        ]
    )


if __name__ == "__main__":
    main()
