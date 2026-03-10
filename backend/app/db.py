from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock_meta (
  code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  industry TEXT NOT NULL,
  board TEXT NOT NULL DEFAULT 'main_board',
  security_type TEXT NOT NULL DEFAULT 'equity',
  listing_date TEXT NOT NULL,
  is_st INTEGER NOT NULL DEFAULT 0,
  is_suspended INTEGER NOT NULL DEFAULT 0,
  last_sync_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_price (
  code TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  turnover REAL NOT NULL,
  is_suspended INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_price_trade_date ON daily_price(trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_price_code_trade_date ON daily_price(code, trade_date);

CREATE TABLE IF NOT EXISTS financial_snapshot (
  code TEXT NOT NULL,
  report_date TEXT NOT NULL,
  publish_date TEXT NOT NULL,
  roe REAL NOT NULL,
  revenue_yoy REAL NOT NULL,
  profit_yoy REAL NOT NULL,
  operating_cashflow REAL NOT NULL,
  net_profit REAL NOT NULL,
  pe_ttm REAL NOT NULL,
  pb REAL NOT NULL,
  debt_ratio REAL NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (code, report_date)
);

CREATE INDEX IF NOT EXISTS idx_financial_publish_date ON financial_snapshot(code, publish_date);

CREATE TABLE IF NOT EXISTS factor_snapshot (
  code TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  factor_name TEXT NOT NULL,
  factor_group TEXT NOT NULL,
  raw_value REAL NOT NULL,
  normalized_score REAL NOT NULL,
  PRIMARY KEY (code, as_of_date, factor_name)
);

CREATE INDEX IF NOT EXISTS idx_factor_snapshot_as_of_date ON factor_snapshot(as_of_date);

CREATE TABLE IF NOT EXISTS strategy_run (
  run_id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  status TEXT NOT NULL,
  provider TEXT NOT NULL,
  as_of_date TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  message TEXT,
  payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_strategy_run_type_started_at ON strategy_run(run_type, started_at DESC);

CREATE TABLE IF NOT EXISTS daily_report (
  report_date TEXT PRIMARY KEY,
  summary_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist_item (
  watchlist_id TEXT NOT NULL,
  code TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  tags_json TEXT NOT NULL DEFAULT '[]',
  target_entry REAL,
  stop_loss REAL,
  take_profit REAL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (watchlist_id, code)
);

CREATE TABLE IF NOT EXISTS validation_cache (
  as_of_date TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (as_of_date)
);

CREATE TABLE IF NOT EXISTS data_audit_snapshot (
  run_id TEXT NOT NULL,
  code TEXT NOT NULL,
  trade_date TEXT,
  severity TEXT NOT NULL,
  issue_type TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, code, issue_type)
);

CREATE TABLE IF NOT EXISTS eligibility_snapshot (
  as_of_date TEXT NOT NULL,
  code TEXT NOT NULL,
  actionable INTEGER NOT NULL DEFAULT 0,
  tier TEXT NOT NULL DEFAULT '观察',
  total_score REAL NOT NULL DEFAULT 0,
  confidence_score REAL NOT NULL DEFAULT 0,
  block_reasons_json TEXT NOT NULL DEFAULT '[]',
  soft_penalties_json TEXT NOT NULL DEFAULT '[]',
  payload_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (as_of_date, code)
);

CREATE TABLE IF NOT EXISTS model_health_snapshot (
  as_of_date TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (as_of_date)
);
"""


class Repository:
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
            self._apply_migrations(connection)

    def _table_columns(self, connection: sqlite3.Connection, table_name: str) -> set[str]:
        return {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}

    def _table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _rebuild_table(self, connection: sqlite3.Connection, table_name: str, create_sql: str, insert_sql: str | None = None) -> None:
        if not self._table_exists(connection, table_name):
            connection.execute(create_sql)
            return
        backup_name = f"{table_name}_legacy"
        connection.execute(f"DROP TABLE IF EXISTS {backup_name}")
        connection.execute(f"ALTER TABLE {table_name} RENAME TO {backup_name}")
        connection.execute(create_sql)
        if insert_sql:
            connection.execute(insert_sql)
        connection.execute(f"DROP TABLE {backup_name}")

    def _apply_migrations(self, connection: sqlite3.Connection) -> None:
        stock_columns = self._table_columns(connection, "stock_meta")
        if "board" not in stock_columns:
            connection.execute("ALTER TABLE stock_meta ADD COLUMN board TEXT NOT NULL DEFAULT 'main_board'")
            connection.execute(
                """
                UPDATE stock_meta
                SET board = CASE
                  WHEN security_type = 'index' THEN 'index'
                  WHEN code LIKE '300%' OR code LIKE '301%' THEN 'chi_next'
                  ELSE 'main_board'
                END
                """
            )

        financial_columns = self._table_columns(connection, "financial_snapshot")
        if "publish_date" not in financial_columns:
            connection.execute("ALTER TABLE financial_snapshot ADD COLUMN publish_date TEXT")
            connection.execute(
                "UPDATE financial_snapshot SET publish_date = report_date WHERE publish_date IS NULL"
            )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_financial_publish_date ON financial_snapshot(code, publish_date)"
        )
        for table_name in ("strategy_config", "paper_trade", "paper_cohort", "signal_event", "journal_entry"):
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")

        connection.execute("DELETE FROM strategy_run WHERE run_type = 'strategy_warm'")

        daily_report_columns = self._table_columns(connection, "daily_report")
        if "strategy_id" in daily_report_columns:
            self._rebuild_table(
                connection,
                "daily_report",
                """
                CREATE TABLE daily_report (
                  report_date TEXT PRIMARY KEY,
                  summary_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """,
                """
                INSERT OR REPLACE INTO daily_report(report_date, summary_json, created_at)
                SELECT report_date, summary_json, created_at
                FROM daily_report_legacy
                WHERE strategy_id = 'balanced'
                """,
            )

        validation_columns = self._table_columns(connection, "validation_cache")
        if "strategy_id" in validation_columns:
            self._rebuild_table(
                connection,
                "validation_cache",
                """
                CREATE TABLE validation_cache (
                  as_of_date TEXT PRIMARY KEY,
                  payload_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """,
                """
                INSERT OR REPLACE INTO validation_cache(as_of_date, payload_json, updated_at)
                SELECT as_of_date, payload_json, updated_at
                FROM validation_cache_legacy
                WHERE strategy_id = 'balanced'
                """,
            )

        eligibility_columns = self._table_columns(connection, "eligibility_snapshot")
        if "strategy_id" in eligibility_columns:
            self._rebuild_table(
                connection,
                "eligibility_snapshot",
                """
                CREATE TABLE eligibility_snapshot (
                  as_of_date TEXT NOT NULL,
                  code TEXT NOT NULL,
                  actionable INTEGER NOT NULL DEFAULT 0,
                  tier TEXT NOT NULL DEFAULT '观察',
                  total_score REAL NOT NULL DEFAULT 0,
                  confidence_score REAL NOT NULL DEFAULT 0,
                  block_reasons_json TEXT NOT NULL DEFAULT '[]',
                  soft_penalties_json TEXT NOT NULL DEFAULT '[]',
                  payload_json TEXT NOT NULL DEFAULT '{}',
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (as_of_date, code)
                )
                """,
                """
                INSERT OR REPLACE INTO eligibility_snapshot(
                  as_of_date, code, actionable, tier, total_score, confidence_score,
                  block_reasons_json, soft_penalties_json, payload_json, updated_at
                )
                SELECT
                  as_of_date, code, actionable, tier, total_score, confidence_score,
                  block_reasons_json, soft_penalties_json, payload_json, updated_at
                FROM eligibility_snapshot_legacy
                WHERE strategy_id = 'balanced'
                """,
            )

        model_health_columns = self._table_columns(connection, "model_health_snapshot")
        if "strategy_id" in model_health_columns:
            self._rebuild_table(
                connection,
                "model_health_snapshot",
                """
                CREATE TABLE model_health_snapshot (
                  as_of_date TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """,
                """
                INSERT OR REPLACE INTO model_health_snapshot(as_of_date, status, payload_json, updated_at)
                SELECT as_of_date, status, payload_json, updated_at
                FROM model_health_snapshot_legacy
                WHERE strategy_id = 'balanced'
                """,
            )

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _json_dump(payload: dict | list | None) -> str:
        return json.dumps({} if payload is None else payload, ensure_ascii=False)

    def upsert_stocks(self, stocks: list[object]) -> int:
        rows = [
            (
                stock.code,
                stock.name,
                stock.industry,
                getattr(stock, "board", "main_board"),
                stock.security_type,
                stock.listing_date,
                int(stock.is_st),
                int(stock.is_suspended),
                self._utc_now_iso(),
            )
            for stock in stocks
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO stock_meta(
                  code, name, industry, board, security_type, listing_date, is_st, is_suspended, last_sync_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                  name = excluded.name,
                  industry = excluded.industry,
                  board = excluded.board,
                  security_type = excluded.security_type,
                  listing_date = excluded.listing_date,
                  is_st = excluded.is_st,
                  is_suspended = excluded.is_suspended,
                  last_sync_at = excluded.last_sync_at
                """,
                rows,
            )
        return len(rows)

    def upsert_price_bars(self, price_bars: list[object]) -> int:
        rows = [
            (
                bar.code,
                bar.trade_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.turnover,
                int(bar.is_suspended),
            )
            for bar in price_bars
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO daily_price(code, trade_date, open, high, low, close, volume, turnover, is_suspended)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, trade_date) DO UPDATE SET
                  open = excluded.open,
                  high = excluded.high,
                  low = excluded.low,
                  close = excluded.close,
                  volume = excluded.volume,
                  turnover = excluded.turnover,
                  is_suspended = excluded.is_suspended
                """,
                rows,
            )
        return len(rows)

    def upsert_financials(self, financials: list[object]) -> int:
        rows = [
            (
                item.code,
                item.report_date,
                item.publish_date,
                item.roe,
                item.revenue_yoy,
                item.profit_yoy,
                item.operating_cashflow,
                item.net_profit,
                item.pe_ttm,
                item.pb,
                item.debt_ratio,
                self._utc_now_iso(),
            )
            for item in financials
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO financial_snapshot(
                  code, report_date, publish_date, roe, revenue_yoy, profit_yoy, operating_cashflow,
                  net_profit, pe_ttm, pb, debt_ratio, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, report_date) DO UPDATE SET
                  publish_date = excluded.publish_date,
                  roe = excluded.roe,
                  revenue_yoy = excluded.revenue_yoy,
                  profit_yoy = excluded.profit_yoy,
                  operating_cashflow = excluded.operating_cashflow,
                  net_profit = excluded.net_profit,
                  pe_ttm = excluded.pe_ttm,
                  pb = excluded.pb,
                  debt_ratio = excluded.debt_ratio,
                  updated_at = excluded.updated_at
                """,
                rows,
            )
        return len(rows)

    def save_market_snapshot(self, snapshot) -> tuple[int, int]:
        stock_rows = self.upsert_stocks(snapshot.stocks)
        price_rows = self.upsert_price_bars(snapshot.price_bars)
        self.upsert_financials(snapshot.financials)
        return stock_rows, price_rows

    def save_factor_snapshot(self, as_of_date: str, snapshot: dict[str, dict]) -> None:
        rows = []
        for code, item in snapshot.items():
            for factor_name, score in item["factor_scores"].items():
                raw_value = item["raw_factors"][factor_name]
                rows.append(
                    (
                        code,
                        as_of_date,
                        factor_name,
                        item["factor_groups"][factor_name],
                        raw_value,
                        score,
                    )
                )
        with self.connect() as connection:
            connection.execute("DELETE FROM factor_snapshot WHERE as_of_date = ?", (as_of_date,))
            connection.executemany(
                """
                INSERT INTO factor_snapshot(code, as_of_date, factor_name, factor_group, raw_value, normalized_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def save_data_audits(self, run_id: str, audits: list[dict]) -> None:
        rows = [
            (
                run_id,
                item["code"],
                item.get("trade_date"),
                item["severity"],
                item["issue_type"],
                item["message"],
                self._utc_now_iso(),
            )
            for item in audits
        ]
        with self.connect() as connection:
            connection.execute("DELETE FROM data_audit_snapshot WHERE run_id = ?", (run_id,))
            if rows:
                connection.executemany(
                    """
                    INSERT INTO data_audit_snapshot(
                      run_id, code, trade_date, severity, issue_type, message, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def get_audit_summary(self, run_id: str | None = None) -> dict:
        if not run_id:
            latest_run = self.get_latest_run("sync")
            run_id = latest_run["run_id"] if latest_run else None
        if not run_id:
            return {"critical_count": 0, "warning_count": 0, "items": []}
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM data_audit_snapshot
                WHERE run_id = ?
                ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, code
                """,
                (run_id,),
            ).fetchall()
        items = [dict(row) for row in rows]
        return {
            "critical_count": sum(1 for item in items if item["severity"] == "critical"),
            "warning_count": sum(1 for item in items if item["severity"] == "warning"),
            "items": items,
        }

    def get_latest_trade_date(self) -> str | None:
        with self.connect() as connection:
            row = connection.execute("SELECT MAX(trade_date) AS latest_trade_date FROM daily_price").fetchone()
        return row["latest_trade_date"] if row and row["latest_trade_date"] else None

    def get_latest_financial_dates(self, codes: list[str] | None = None) -> dict[str, dict[str, str | None]]:
        query = """
            SELECT code, report_date, publish_date
            FROM (
              SELECT
                f.code,
                f.report_date,
                f.publish_date,
                ROW_NUMBER() OVER (
                  PARTITION BY f.code
                  ORDER BY f.report_date DESC, COALESCE(f.publish_date, f.report_date) DESC
                ) AS rn
              FROM financial_snapshot f
        """
        params: list[str] = []
        if codes:
            placeholders = ",".join("?" for _ in codes)
            query += f" WHERE f.code IN ({placeholders})"
            params.extend(codes)
        query += """
            )
            WHERE rn = 1
        """
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return {
            row["code"]: {
                "report_date": row["report_date"],
                "publish_date": row["publish_date"],
            }
            for row in rows
        }

    def get_stock_meta(self) -> dict[str, dict]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM stock_meta").fetchall()
        return {row["code"]: dict(row) for row in rows}

    def count_equities(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM stock_meta WHERE security_type = 'equity'"
            ).fetchone()
        return int(row["count"]) if row else 0

    def count_watchlist_items(self, watchlist_id: str = "core") -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM watchlist_item WHERE watchlist_id = ?",
                (watchlist_id,),
            ).fetchone()
        return int(row["count"]) if row else 0

    def get_price_map(self, as_of_date: str | None = None) -> dict[str, list[dict]]:
        query = "SELECT p.* FROM daily_price p WHERE 1=1"
        params: list[str] = []
        if as_of_date:
            query += " AND p.trade_date <= ?"
            params.append(as_of_date)
        query += " ORDER BY p.code, p.trade_date"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        result: dict[str, list[dict]] = {}
        for row in rows:
            result.setdefault(row["code"], []).append(dict(row))
        return result

    def get_recent_price_map(self, as_of_date: str, days: int = 90) -> dict[str, list[dict]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM (
                  SELECT
                    p.*,
                    ROW_NUMBER() OVER (PARTITION BY p.code ORDER BY p.trade_date DESC) AS rn
                  FROM daily_price p
                  WHERE p.trade_date <= ?
                )
                WHERE rn <= ?
                ORDER BY code, trade_date
                """,
                (as_of_date, days),
            ).fetchall()
        result: dict[str, list[dict]] = {}
        for row in rows:
            item = dict(row)
            item.pop("rn", None)
            result.setdefault(item["code"], []).append(item)
        return result

    def get_latest_price_lookup(self, as_of_date: str | None = None) -> dict[str, dict]:
        as_of_date = as_of_date or self.get_latest_trade_date()
        if not as_of_date:
            return {}
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT p.*
                FROM daily_price p
                INNER JOIN (
                  SELECT code, MAX(trade_date) AS trade_date
                  FROM daily_price
                  WHERE trade_date <= ?
                  GROUP BY code
                ) latest
                  ON latest.code = p.code AND latest.trade_date = p.trade_date
                """,
                (as_of_date,),
            ).fetchall()
        return {row["code"]: dict(row) for row in rows}

    def get_price_history(self, code: str, limit: int = 90) -> list[dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT trade_date, open, close, volume
                FROM daily_price
                WHERE code = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (code, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def get_visible_financials(self, as_of_date: str | None = None) -> dict[str, dict]:
        if not as_of_date:
            as_of_date = self.get_latest_trade_date()
        if not as_of_date:
            return {}
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM (
                  SELECT
                    f.*,
                    ROW_NUMBER() OVER (
                      PARTITION BY f.code
                      ORDER BY COALESCE(f.publish_date, f.report_date) DESC, f.report_date DESC
                    ) AS rn
                  FROM financial_snapshot f
                  WHERE COALESCE(f.publish_date, f.report_date) <= ?
                )
                WHERE rn = 1
                """,
                (as_of_date,),
            ).fetchall()
        return {row["code"]: dict(row) for row in rows}

    def get_trading_dates(self, code: str | None = None, security_type: str | None = None) -> list[str]:
        query = "SELECT DISTINCT p.trade_date FROM daily_price p"
        joins = ""
        params: list[str] = []
        clauses: list[str] = []
        if security_type:
            joins = " INNER JOIN stock_meta m ON m.code = p.code"
            clauses.append("m.security_type = ?")
            params.append(security_type)
        if code:
            clauses.append("p.code = ?")
            params.append(code)
        if joins:
            query += joins
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY p.trade_date"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [row["trade_date"] for row in rows]

    def start_run(self, run_type: str, provider: str, as_of_date: str | None = None, payload: dict | None = None) -> str:
        run_id = str(uuid.uuid4())
        base_payload = {
            "stage": "queued",
            "processed_symbols": 0,
            "total_symbols": 0,
            "queued_symbols": 0,
            "skipped_symbols": 0,
            "stocks_synced": 0,
            "price_rows_synced": 0,
            "warning_count": 0,
            "warnings": [],
            "latest_trade_date": as_of_date,
            "failure_ratio": 0.0,
            "sync_mode": "auto",
        }
        base_payload.update(payload or {})
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO strategy_run(run_id, run_type, status, provider, as_of_date, started_at, payload_json)
                VALUES (?, ?, 'running', ?, ?, ?, ?)
                """,
                (run_id, run_type, provider, as_of_date, self._utc_now_iso(), self._json_dump(base_payload)),
            )
        return run_id

    def update_run_progress(
        self,
        run_id: str,
        *,
        stage: str,
        message: str = "",
        payload: dict | None = None,
        as_of_date: str | None = None,
    ) -> None:
        current = self.get_run(run_id) or {}
        current_payload = json.loads(current.get("payload_json") or "{}")
        current_payload.update(payload or {})
        current_payload["stage"] = stage
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE strategy_run
                SET message = ?, payload_json = ?, as_of_date = COALESCE(?, as_of_date)
                WHERE run_id = ?
                """,
                (message, self._json_dump(current_payload), as_of_date, run_id),
            )

    def finish_run(
        self,
        run_id: str,
        status: str,
        message: str,
        payload: dict | None = None,
        as_of_date: str | None = None,
    ) -> None:
        current = self.get_run(run_id) or {}
        current_payload = json.loads(current.get("payload_json") or "{}")
        current_payload.update(payload or {})
        current_payload["stage"] = "completed" if status in {"success", "partial"} else "failed"
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE strategy_run
                SET status = ?, message = ?, payload_json = ?, as_of_date = COALESCE(?, as_of_date), finished_at = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    message,
                    self._json_dump(current_payload),
                    as_of_date,
                    self._utc_now_iso(),
                    run_id,
                ),
            )

    def get_run(self, run_id: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM strategy_run WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

    def get_latest_run(self, run_type: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM strategy_run
                WHERE run_type = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (run_type,),
            ).fetchone()
        return dict(row) if row else None

    def fail_running_runs(self, run_type: str, message: str) -> int:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT run_id, payload_json
                FROM strategy_run
                WHERE run_type = ? AND status = 'running'
                """,
                (run_type,),
            ).fetchall()
            if not rows:
                return 0
            finished_at = self._utc_now_iso()
            for row in rows:
                payload = json.loads(row["payload_json"] or "{}")
                payload["stage"] = "failed"
                connection.execute(
                    """
                    UPDATE strategy_run
                    SET status = 'failed', message = ?, payload_json = ?, finished_at = ?
                    WHERE run_id = ?
                    """,
                    (message, self._json_dump(payload), finished_at, row["run_id"]),
                )
        return len(rows)

    def get_running_run(self, run_type: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM strategy_run
                WHERE run_type = ? AND status = 'running'
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (run_type,),
            ).fetchone()
        return dict(row) if row else None

    def upsert_validation_cache(self, as_of_date: str, payload: dict) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO validation_cache(as_of_date, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(as_of_date) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (as_of_date, self._json_dump(payload), self._utc_now_iso()),
            )

    def get_validation_cache(self, as_of_date: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json
                FROM validation_cache
                WHERE as_of_date = ?
                """,
                (as_of_date,),
            ).fetchone()
        return json.loads(row["payload_json"]) if row else None

    def replace_eligibility_snapshot(self, as_of_date: str, rows: list[dict]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM eligibility_snapshot WHERE as_of_date = ?", (as_of_date,))
            if rows:
                connection.executemany(
                    """
                    INSERT INTO eligibility_snapshot(
                      as_of_date, code, actionable, tier, total_score, confidence_score,
                      block_reasons_json, soft_penalties_json, payload_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            as_of_date,
                            row["code"],
                            int(row.get("actionable", False)),
                            row.get("tier", "观察"),
                            row.get("total_score", 0.0),
                            row.get("confidence_score", 0.0),
                            self._json_dump(row.get("block_reasons", [])),
                            self._json_dump(row.get("soft_penalties", [])),
                            self._json_dump(row),
                            self._utc_now_iso(),
                        )
                        for row in rows
                    ],
                )

    def get_eligibility_snapshot(self, as_of_date: str) -> dict[str, dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT code, payload_json
                FROM eligibility_snapshot
                WHERE as_of_date = ?
                """,
                (as_of_date,),
            ).fetchall()
        return {row["code"]: json.loads(row["payload_json"]) for row in rows}

    def upsert_model_health_snapshot(self, as_of_date: str, status: str, payload: dict) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO model_health_snapshot(as_of_date, status, payload_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(as_of_date) DO UPDATE SET
                  status = excluded.status,
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (as_of_date, status, self._json_dump(payload), self._utc_now_iso()),
            )

    def get_latest_model_health(self) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM model_health_snapshot
                ORDER BY as_of_date DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["payload_json"] = json.loads(payload["payload_json"])
        return payload

    def upsert_daily_report(self, report_date: str, payload: dict) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO daily_report(report_date, summary_json, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(report_date) DO UPDATE SET
                  summary_json = excluded.summary_json,
                  created_at = excluded.created_at
                """,
                (report_date, self._json_dump(payload), self._utc_now_iso()),
            )

    def get_daily_report(self, report_date: str | None = None) -> dict | None:
        with self.connect() as connection:
            if report_date:
                row = connection.execute("SELECT * FROM daily_report WHERE report_date = ?", (report_date,)).fetchone()
            else:
                row = connection.execute("SELECT * FROM daily_report ORDER BY report_date DESC LIMIT 1").fetchone()
        if not row:
            return None
        payload = json.loads(row["summary_json"])
        payload["generated_at"] = row["created_at"]
        return payload

    def replace_watchlist(self, watchlist_id: str, items: list[dict]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM watchlist_item WHERE watchlist_id = ?", (watchlist_id,))
            connection.executemany(
                """
                INSERT INTO watchlist_item(
                  watchlist_id, code, note, tags_json, target_entry, stop_loss, take_profit, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        watchlist_id,
                        item["code"],
                        item.get("note", ""),
                        "[]",
                        item.get("target_entry"),
                        item.get("stop_loss"),
                        item.get("take_profit"),
                        self._utc_now_iso(),
                    )
                    for item in items
                ],
            )

    def get_watchlist(self, watchlist_id: str) -> list[dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                  w.*,
                  COALESCE(m.name, '') AS name,
                  COALESCE(m.industry, '') AS industry,
                  latest.close AS last_price
                FROM watchlist_item w
                LEFT JOIN stock_meta m ON m.code = w.code
                LEFT JOIN daily_price latest
                  ON latest.code = w.code
                 AND latest.trade_date = (SELECT MAX(trade_date) FROM daily_price dp WHERE dp.code = w.code)
                WHERE w.watchlist_id = ?
                ORDER BY w.updated_at DESC
                """,
                (watchlist_id,),
            ).fetchall()
        return [
            {
                "watchlist_id": row["watchlist_id"],
                "code": row["code"],
                "note": row["note"],
                "target_entry": row["target_entry"],
                "stop_loss": row["stop_loss"],
                "take_profit": row["take_profit"],
                "name": row["name"],
                "industry": row["industry"],
                "last_price": row["last_price"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
