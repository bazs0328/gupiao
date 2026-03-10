from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid


SCHEMA = """
CREATE TABLE IF NOT EXISTS research_run (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  provider TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  message TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_research_run_started_at ON research_run(started_at DESC);

CREATE TABLE IF NOT EXISTS research_security_state_event (
  code TEXT NOT NULL,
  event_date TEXT NOT NULL,
  event_type TEXT NOT NULL,
  name TEXT NOT NULL,
  industry TEXT NOT NULL,
  board TEXT NOT NULL,
  security_type TEXT NOT NULL,
  listing_date TEXT NOT NULL,
  delisting_date TEXT,
  is_st INTEGER NOT NULL DEFAULT 0,
  tradable INTEGER NOT NULL DEFAULT 1,
  source_quality TEXT NOT NULL DEFAULT 'estimated',
  PRIMARY KEY (code, event_date, event_type)
);

CREATE INDEX IF NOT EXISTS idx_research_state_event_date ON research_security_state_event(event_date);

CREATE TABLE IF NOT EXISTS research_price_bar (
  code TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  price_basis TEXT NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  turnover REAL NOT NULL,
  is_suspended INTEGER NOT NULL DEFAULT 0,
  source_quality TEXT NOT NULL DEFAULT 'estimated',
  PRIMARY KEY (code, trade_date, price_basis)
);

CREATE INDEX IF NOT EXISTS idx_research_price_trade_date ON research_price_bar(trade_date, price_basis);

CREATE TABLE IF NOT EXISTS research_financial_record (
  code TEXT NOT NULL,
  report_date TEXT NOT NULL,
  publish_date TEXT NOT NULL,
  publish_date_quality TEXT NOT NULL DEFAULT 'estimated',
  source_quality TEXT NOT NULL DEFAULT 'estimated',
  roe REAL NOT NULL,
  revenue_yoy REAL NOT NULL,
  profit_yoy REAL NOT NULL,
  operating_cashflow REAL NOT NULL,
  net_profit REAL NOT NULL,
  pe_ttm REAL NOT NULL,
  pb REAL NOT NULL,
  debt_ratio REAL NOT NULL,
  PRIMARY KEY (code, report_date)
);

CREATE INDEX IF NOT EXISTS idx_research_financial_publish_date ON research_financial_record(code, publish_date);

CREATE TABLE IF NOT EXISTS research_corporate_action (
  code TEXT NOT NULL,
  action_date TEXT NOT NULL,
  action_type TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  source_quality TEXT NOT NULL DEFAULT 'estimated',
  PRIMARY KEY (code, action_date, action_type)
);

CREATE TABLE IF NOT EXISTS research_sample (
  signal_date TEXT NOT NULL,
  code TEXT NOT NULL,
  name TEXT NOT NULL,
  industry TEXT NOT NULL,
  board TEXT NOT NULL,
  regime TEXT NOT NULL,
  headline_eligible INTEGER NOT NULL DEFAULT 0,
  source_quality TEXT NOT NULL DEFAULT 'estimated',
  snapshot_json TEXT NOT NULL,
  factor_scores_json TEXT NOT NULL DEFAULT '{}',
  section_scores_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  PRIMARY KEY (signal_date, code)
);

CREATE INDEX IF NOT EXISTS idx_research_sample_signal_date ON research_sample(signal_date);

CREATE TABLE IF NOT EXISTS research_parameter_version (
  version_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  as_of_date TEXT,
  sample_count INTEGER NOT NULL DEFAULT 0,
  headline_sample_count INTEGER NOT NULL DEFAULT 0,
  stability_score REAL NOT NULL DEFAULT 0,
  config_json TEXT NOT NULL,
  summary_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  approved_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_research_parameter_created_at ON research_parameter_version(created_at DESC);

CREATE TABLE IF NOT EXISTS research_diagnostic_snapshot (
  as_of_date TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  parameter_version TEXT,
  sample_count INTEGER NOT NULL DEFAULT 0,
  headline_sample_count INTEGER NOT NULL DEFAULT 0,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);
"""


class ResearchRepository:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.db_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def init_db(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _json_dump(payload: dict | list | None) -> str:
        return json.dumps({} if payload is None else payload, ensure_ascii=False)

    def start_run(self, mode: str, provider: str, payload: dict | None = None) -> str:
        run_id = str(uuid.uuid4())
        base_payload = {"stage": "queued", "warnings": [], "sample_count": 0}
        base_payload.update(payload or {})
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO research_run(run_id, mode, status, provider, started_at, payload_json)
                VALUES (?, ?, 'running', ?, ?, ?)
                """,
                (run_id, mode, provider, self._utc_now_iso(), self._json_dump(base_payload)),
            )
        return run_id

    def update_run_progress(self, run_id: str, *, stage: str, message: str = "", payload: dict | None = None) -> None:
        current = self.get_run(run_id) or {}
        current_payload = json.loads(current.get("payload_json") or "{}")
        current_payload.update(payload or {})
        current_payload["stage"] = stage
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE research_run
                SET message = ?, payload_json = ?
                WHERE run_id = ?
                """,
                (message, self._json_dump(current_payload), run_id),
            )

    def finish_run(self, run_id: str, status: str, message: str, payload: dict | None = None) -> None:
        current = self.get_run(run_id) or {}
        current_payload = json.loads(current.get("payload_json") or "{}")
        current_payload.update(payload or {})
        current_payload["stage"] = "completed" if status == "success" else "failed"
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE research_run
                SET status = ?, message = ?, payload_json = ?, finished_at = ?
                WHERE run_id = ?
                """,
                (status, message, self._json_dump(current_payload), self._utc_now_iso(), run_id),
            )

    def get_run(self, run_id: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM research_run WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

    def get_latest_run(self) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM research_run
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def get_running_run(self) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM research_run
                WHERE status = 'running'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def fail_running_runs(self, message: str) -> int:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT run_id, payload_json
                FROM research_run
                WHERE status = 'running'
                """
            ).fetchall()
            if not rows:
                return 0
            finished_at = self._utc_now_iso()
            for row in rows:
                payload = json.loads(row["payload_json"] or "{}")
                payload["stage"] = "failed"
                connection.execute(
                    """
                    UPDATE research_run
                    SET status = 'failed', message = ?, payload_json = ?, finished_at = ?
                    WHERE run_id = ?
                    """,
                    (message, self._json_dump(payload), finished_at, row["run_id"]),
                )
        return len(rows)

    def upsert_security_state_events(self, rows: list[object]) -> int:
        if not rows:
            return 0
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO research_security_state_event(
                  code, event_date, event_type, name, industry, board, security_type,
                  listing_date, delisting_date, is_st, tradable, source_quality
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, event_date, event_type) DO UPDATE SET
                  name = excluded.name,
                  industry = excluded.industry,
                  board = excluded.board,
                  security_type = excluded.security_type,
                  listing_date = excluded.listing_date,
                  delisting_date = excluded.delisting_date,
                  is_st = excluded.is_st,
                  tradable = excluded.tradable,
                  source_quality = excluded.source_quality
                """,
                [
                    (
                        row.code,
                        row.event_date,
                        row.event_type,
                        row.name,
                        row.industry,
                        row.board,
                        row.security_type,
                        row.listing_date,
                        row.delisting_date,
                        int(row.is_st),
                        int(row.tradable),
                        row.source_quality,
                    )
                    for row in rows
                ],
            )
        return len(rows)

    def upsert_price_bars(self, rows: list[object]) -> int:
        if not rows:
            return 0
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO research_price_bar(
                  code, trade_date, price_basis, open, high, low, close, volume, turnover, is_suspended, source_quality
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, trade_date, price_basis) DO UPDATE SET
                  open = excluded.open,
                  high = excluded.high,
                  low = excluded.low,
                  close = excluded.close,
                  volume = excluded.volume,
                  turnover = excluded.turnover,
                  is_suspended = excluded.is_suspended,
                  source_quality = excluded.source_quality
                """,
                [
                    (
                        row.code,
                        row.trade_date,
                        row.price_basis,
                        row.open,
                        row.high,
                        row.low,
                        row.close,
                        row.volume,
                        row.turnover,
                        int(row.is_suspended),
                        row.source_quality,
                    )
                    for row in rows
                ],
            )
        return len(rows)

    def upsert_financial_records(self, rows: list[object]) -> int:
        if not rows:
            return 0
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO research_financial_record(
                  code, report_date, publish_date, publish_date_quality, source_quality,
                  roe, revenue_yoy, profit_yoy, operating_cashflow, net_profit, pe_ttm, pb, debt_ratio
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, report_date) DO UPDATE SET
                  publish_date = excluded.publish_date,
                  publish_date_quality = excluded.publish_date_quality,
                  source_quality = excluded.source_quality,
                  roe = excluded.roe,
                  revenue_yoy = excluded.revenue_yoy,
                  profit_yoy = excluded.profit_yoy,
                  operating_cashflow = excluded.operating_cashflow,
                  net_profit = excluded.net_profit,
                  pe_ttm = excluded.pe_ttm,
                  pb = excluded.pb,
                  debt_ratio = excluded.debt_ratio
                """,
                [
                    (
                        row.code,
                        row.report_date,
                        row.publish_date,
                        row.publish_date_quality,
                        row.source_quality,
                        row.roe,
                        row.revenue_yoy,
                        row.profit_yoy,
                        row.operating_cashflow,
                        row.net_profit,
                        row.pe_ttm,
                        row.pb,
                        row.debt_ratio,
                    )
                    for row in rows
                ],
            )
        return len(rows)

    def upsert_corporate_actions(self, rows: list[object]) -> int:
        if not rows:
            return 0
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO research_corporate_action(code, action_date, action_type, payload_json, source_quality)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(code, action_date, action_type) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  source_quality = excluded.source_quality
                """,
                [
                    (row.code, row.action_date, row.action_type, row.payload_json, row.source_quality)
                    for row in rows
                ],
            )
        return len(rows)

    def get_latest_trade_date(self, price_basis: str = "adjusted") -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT MAX(trade_date) AS latest_trade_date
                FROM research_price_bar
                WHERE price_basis = ?
                """,
                (price_basis,),
            ).fetchone()
        return row["latest_trade_date"] if row and row["latest_trade_date"] else None

    def get_latest_import_date(self, table_name: str, date_column: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                f"SELECT MAX({date_column}) AS latest_date FROM {table_name}"
            ).fetchone()
        return row["latest_date"] if row and row["latest_date"] else None

    def get_trading_dates(self, *, price_basis: str = "adjusted") -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT trade_date
                FROM research_price_bar
                WHERE price_basis = ?
                ORDER BY trade_date
                """,
                (price_basis,),
            ).fetchall()
        return [row["trade_date"] for row in rows]

    def get_meta_snapshot(self, as_of_date: str) -> dict[str, dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM (
                  SELECT
                    e.*,
                    ROW_NUMBER() OVER (PARTITION BY e.code ORDER BY e.event_date DESC, e.event_type DESC) AS rn
                  FROM research_security_state_event e
                  WHERE e.event_date <= ?
                )
                WHERE rn = 1
                """,
                (as_of_date,),
            ).fetchall()
        meta: dict[str, dict] = {}
        for row in rows:
            if row["listing_date"] > as_of_date:
                continue
            if row["delisting_date"] and row["delisting_date"] <= as_of_date:
                continue
            meta[row["code"]] = {
                "code": row["code"],
                "name": row["name"],
                "industry": row["industry"],
                "board": row["board"],
                "security_type": row["security_type"],
                "listing_date": row["listing_date"],
                "is_st": row["is_st"],
                "is_suspended": 0 if row["tradable"] else 1,
                "source_quality": row["source_quality"],
            }
        return meta

    def get_price_map(self, *, as_of_date: str | None = None, price_basis: str = "adjusted") -> dict[str, list[dict]]:
        query = """
            SELECT *
            FROM research_price_bar
            WHERE price_basis = ?
        """
        params: list[object] = [price_basis]
        if as_of_date:
            query += " AND trade_date <= ?"
            params.append(as_of_date)
        query += " ORDER BY code, trade_date"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        result: dict[str, list[dict]] = {}
        for row in rows:
            result.setdefault(row["code"], []).append(dict(row))
        return result

    def get_visible_financials(self, *, as_of_date: str) -> dict[str, dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM (
                  SELECT
                    f.*,
                    ROW_NUMBER() OVER (
                      PARTITION BY f.code
                      ORDER BY f.publish_date DESC, f.report_date DESC
                    ) AS rn
                  FROM research_financial_record f
                  WHERE f.publish_date <= ?
                )
                WHERE rn = 1
                """,
                (as_of_date,),
            ).fetchall()
        return {row["code"]: dict(row) for row in rows}

    def has_high_quality_execution_window(self, code: str, signal_date: str, holding_period_days: int) -> bool:
        trading_dates = self.get_trading_dates(price_basis="raw")
        if signal_date not in trading_dates:
            return False
        index = trading_dates.index(signal_date)
        end_index = min(index + holding_period_days, len(trading_dates) - 1)
        window_dates = trading_dates[index + 1 : end_index + 1]
        if not window_dates:
            return False
        placeholders = ",".join("?" for _ in window_dates)
        params: list[object] = [code, *window_dates]
        with self.connect() as connection:
            row = connection.execute(
                f"""
                SELECT MIN(CASE WHEN source_quality = 'actual' THEN 1 ELSE 0 END) AS all_actual
                FROM research_price_bar
                WHERE code = ?
                  AND price_basis = 'raw'
                  AND trade_date IN ({placeholders})
                """,
                params,
            ).fetchone()
        return bool(row and row["all_actual"] == 1)

    def replace_samples_for_signal_date(self, signal_date: str, rows: list[dict]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM research_sample WHERE signal_date = ?", (signal_date,))
            if not rows:
                return
            connection.executemany(
                """
                INSERT INTO research_sample(
                  signal_date, code, name, industry, board, regime, headline_eligible,
                  source_quality, snapshot_json, factor_scores_json, section_scores_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        signal_date,
                        row["code"],
                        row["name"],
                        row["industry"],
                        row["board"],
                        row["regime"],
                        int(row["headline_eligible"]),
                        row["source_quality"],
                        self._json_dump(row["snapshot"]),
                        self._json_dump(row["factor_scores"]),
                        self._json_dump(row["section_scores"]),
                        self._utc_now_iso(),
                    )
                    for row in rows
                ],
            )

    def get_last_sample_date(self) -> str | None:
        with self.connect() as connection:
            row = connection.execute("SELECT MAX(signal_date) AS signal_date FROM research_sample").fetchone()
        return row["signal_date"] if row and row["signal_date"] else None

    def get_sample_signal_dates(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT signal_date FROM research_sample ORDER BY signal_date"
            ).fetchall()
        return [row["signal_date"] for row in rows]

    def get_samples_by_signal_date(self, signal_date: str) -> list[dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM research_sample
                WHERE signal_date = ?
                ORDER BY code
                """,
                (signal_date,),
            ).fetchall()
        result: list[dict] = []
        for row in rows:
            item = dict(row)
            item["headline_eligible"] = bool(item["headline_eligible"])
            item["snapshot_json"] = json.loads(item["snapshot_json"])
            item["factor_scores_json"] = json.loads(item["factor_scores_json"])
            item["section_scores_json"] = json.loads(item["section_scores_json"])
            result.append(item)
        return result

    def count_samples(self) -> int:
        with self.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM research_sample").fetchone()
        return int(row["count"]) if row else 0

    def count_headline_samples(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM research_sample WHERE headline_eligible = 1"
            ).fetchone()
        return int(row["count"]) if row else 0

    def source_quality_summary(self) -> dict[str, int]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT source_quality, COUNT(*) AS count
                FROM research_sample
                GROUP BY source_quality
                ORDER BY count DESC
                """
            ).fetchall()
        return {row["source_quality"]: int(row["count"]) for row in rows}

    def upsert_parameter_version(
        self,
        *,
        version_id: str,
        status: str,
        as_of_date: str | None,
        sample_count: int,
        headline_sample_count: int,
        stability_score: float,
        config_payload: dict,
        summary_payload: dict,
        approve: bool,
    ) -> None:
        approved_at = self._utc_now_iso() if approve else None
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO research_parameter_version(
                  version_id, status, as_of_date, sample_count, headline_sample_count, stability_score,
                  config_json, summary_json, created_at, approved_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(version_id) DO UPDATE SET
                  status = excluded.status,
                  as_of_date = excluded.as_of_date,
                  sample_count = excluded.sample_count,
                  headline_sample_count = excluded.headline_sample_count,
                  stability_score = excluded.stability_score,
                  config_json = excluded.config_json,
                  summary_json = excluded.summary_json,
                  approved_at = excluded.approved_at
                """,
                (
                    version_id,
                    status,
                    as_of_date,
                    sample_count,
                    headline_sample_count,
                    stability_score,
                    self._json_dump(config_payload),
                    self._json_dump(summary_payload),
                    self._utc_now_iso(),
                    approved_at,
                ),
            )

    def get_latest_parameter(self, *, approved_only: bool = False) -> dict | None:
        query = """
            SELECT *
            FROM research_parameter_version
        """
        params: list[object] = []
        if approved_only:
            query += " WHERE status = 'approved'"
        query += " ORDER BY created_at DESC LIMIT 1"
        with self.connect() as connection:
            row = connection.execute(query, params).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["config_json"] = json.loads(payload["config_json"])
        payload["summary_json"] = json.loads(payload["summary_json"])
        return payload

    def get_parameter_by_version(self, version_id: str, *, approved_only: bool = False) -> dict | None:
        query = """
            SELECT *
            FROM research_parameter_version
            WHERE version_id = ?
        """
        params: list[object] = [version_id]
        if approved_only:
            query += " AND status = 'approved'"
        query += " LIMIT 1"
        with self.connect() as connection:
            row = connection.execute(query, params).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["config_json"] = json.loads(payload["config_json"])
        payload["summary_json"] = json.loads(payload["summary_json"])
        return payload

    def upsert_diagnostic_snapshot(
        self,
        *,
        as_of_date: str,
        status: str,
        parameter_version: str | None,
        sample_count: int,
        headline_sample_count: int,
        payload: dict,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO research_diagnostic_snapshot(
                  as_of_date, status, parameter_version, sample_count, headline_sample_count, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(as_of_date) DO UPDATE SET
                  status = excluded.status,
                  parameter_version = excluded.parameter_version,
                  sample_count = excluded.sample_count,
                  headline_sample_count = excluded.headline_sample_count,
                  payload_json = excluded.payload_json,
                  created_at = excluded.created_at
                """,
                (
                    as_of_date,
                    status,
                    parameter_version,
                    sample_count,
                    headline_sample_count,
                    self._json_dump(payload),
                    self._utc_now_iso(),
                ),
            )

    def get_latest_diagnostic(self) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM research_diagnostic_snapshot
                ORDER BY as_of_date DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["payload_json"] = json.loads(payload["payload_json"])
        return payload
