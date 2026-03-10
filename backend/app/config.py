from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


ROOT_DIR = Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class AppSettings:
    host: str
    port: int
    data_dir: Path
    db_path: Path
    research_db_path: Path
    benchmark_code: str = "000300.SH"

    @classmethod
    def from_env(cls) -> "AppSettings":
        data_dir = Path(os.getenv("GUPIAO_DATA_DIR", ROOT_DIR / ".data"))
        db_path = data_dir / "gupiao.db"
        research_db_path = data_dir / "gupiao_research.db"
        return cls(
            host=os.getenv("GUPIAO_HOST", "127.0.0.1"),
            port=int(os.getenv("GUPIAO_PORT", "8000")),
            data_dir=data_dir,
            db_path=db_path,
            research_db_path=research_db_path,
        )
