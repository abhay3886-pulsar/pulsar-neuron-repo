"""Database helpers for ingest jobs using psycopg2."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date
from typing import Iterable, Sequence
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool

from pulsar_neuron.providers.market_provider import (
    BreadthRow,
    FutOiRow,
    OhlcvBar,
    OptionRow,
    VixRow,
)

logger = logging.getLogger(__name__)
_pool: SimpleConnectionPool | None = None


def _normalize_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg2://"):
        return "postgresql://" + dsn.split("://", 1)[1]
    return dsn


def _build_dsn() -> str:
    dsn = os.getenv("DB_DSN")
    if dsn:
        return _normalize_dsn(dsn)
    try:
        from pulsar_neuron.config.secrets import get_db_credentials

        secret = get_db_credentials()
        return (
            "postgresql://"
            f"{secret['username']}:{secret['password']}@{secret['host']}"
            f":{secret.get('port', 5432)}/{secret['database']}"
        )
    except Exception as exc:  # pragma: no cover - optional secret path
        raise RuntimeError("DB_DSN not configured") from exc


def _connection_args(dsn: str) -> dict[str, str | int]:
    parsed = urlparse(dsn)
    if not parsed.scheme:
        return {"dsn": dsn}
    return {
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username or "",
        "password": parsed.password or "",
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
    }


def _get_pool() -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        dsn = _build_dsn()
        params = _connection_args(dsn)
        _pool = SimpleConnectionPool(1, 5, **params)
        logger.info("DB connection pool initialized host=%s", params.get("host"))
    return _pool


@contextmanager
def _get_conn():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def _execute_upsert(
    table: str,
    columns: Sequence[str],
    conflict_cols: Sequence[str],
    rows: Iterable[dict],
    *,
    where_clause: str | None = None,
) -> tuple[int, int]:
    rows_list = list(rows)
    if not rows_list:
        return 0, 0
    update_cols = [col for col in columns if col not in conflict_cols]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_cols = ", ".join(columns)
    conflict = ", ".join(conflict_cols)
    set_clause = ", ".join(f"{col}=EXCLUDED.{col}" for col in update_cols)
    query = (
        f"INSERT INTO {table} ({insert_cols}) VALUES %s ON CONFLICT ({conflict}) "
        f"DO UPDATE SET {set_clause}"
    )
    if where_clause:
        query += f" WHERE {where_clause}"
    query += " RETURNING (xmax = 0) AS inserted;"
    values = [tuple(row[col] for col in columns) for row in rows_list]
    inserted = 0
    updated = 0
    with _get_conn() as conn:
        with conn:
            with conn.cursor() as cur:
                result = execute_values(cur, query, values, template=f"({placeholders})", fetch=True)
                for record in result:
                    if record[0]:
                        inserted += 1
                    else:
                        updated += 1
    return inserted, updated


def upsert_ohlcv(rows: list[OhlcvBar]) -> None:
    columns = ["symbol", "ts_ist", "tf", "o", "h", "l", "c", "v"]
    inserted, updated = _execute_upsert("ohlcv", columns, ("symbol", "ts_ist", "tf"), rows)
    logger.info("ohlcv upsert inserted=%s updated=%s dropped=%s", inserted, updated, 0)


def upsert_fut_oi(rows: list[FutOiRow]) -> None:
    columns = ["symbol", "ts_ist", "price", "oi", "baseline_tag"]
    inserted, updated = _execute_upsert("fut_oi", columns, ("symbol", "ts_ist"), rows)
    logger.info("fut_oi upsert inserted=%s updated=%s", inserted, updated)


def insert_or_update_fut_oi_baseline(rows: list[FutOiRow], trading_day: date, tag: str) -> None:
    payload = []
    for row in rows:
        payload.append(
            {
                "symbol": row["symbol"],
                "trading_day": trading_day,
                "ts_ist": row["ts_ist"],
                "price": row["price"],
                "oi": row["oi"],
                "tag": tag,
            }
        )
    columns = ["symbol", "trading_day", "ts_ist", "price", "oi", "tag"]
    where = "fut_oi_baseline.ts_ist < EXCLUDED.ts_ist"
    inserted, updated = _execute_upsert(
        "fut_oi_baseline",
        columns,
        ("symbol", "trading_day"),
        payload,
        where_clause=where,
    )
    logger.info("fut_oi_baseline upsert inserted=%s updated=%s", inserted, updated)


def upsert_option_chain(rows: list[OptionRow]) -> None:
    columns = [
        "symbol",
        "ts_ist",
        "expiry",
        "strike",
        "side",
        "ltp",
        "iv",
        "oi",
        "doi",
        "volume",
        "delta",
        "gamma",
        "theta",
        "vega",
    ]
    inserted, updated = _execute_upsert(
        "option_chain",
        columns,
        ("symbol", "ts_ist", "expiry", "strike", "side"),
        rows,
    )
    logger.info("option_chain upsert inserted=%s updated=%s", inserted, updated)


def upsert_breadth(row: BreadthRow) -> None:
    columns = ["ts_ist", "adv", "dec", "unchanged"]
    inserted, updated = _execute_upsert("breadth", columns, ("ts_ist",), [row])
    logger.info("breadth upsert inserted=%s updated=%s", inserted, updated)


def upsert_vix(row: VixRow) -> None:
    columns = ["ts_ist", "value"]
    inserted, updated = _execute_upsert("vix", columns, ("ts_ist",), [row])
    logger.info("vix upsert inserted=%s updated=%s", inserted, updated)


__all__ = [
    "upsert_ohlcv",
    "upsert_fut_oi",
    "insert_or_update_fut_oi_baseline",
    "upsert_option_chain",
    "upsert_breadth",
    "upsert_vix",
]
