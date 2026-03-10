#!/usr/bin/env python3
"""
Shared DuckDB helpers for the wide-build pipeline.

This module centralizes connection/bootstrap logic, build-run metadata capture,
and common SQL export helpers so the DuckDB-backed builder, monitoring tools,
and certification scripts share the same runtime conventions.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb


def sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def open_build_connection(
    duckdb_path: str,
    temp_dir: str | None,
    persist_duckdb: bool,
    *,
    memory_limit: str | None = "8GB",
    threads: int = 2,
    preserve_insertion_order: bool = False,
) -> tuple[duckdb.DuckDBPyConnection, str]:
    effective_path = duckdb_path if persist_duckdb else ":memory:"
    if persist_duckdb:
        Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(effective_path)
    con.execute(f"PRAGMA threads={int(threads)};")
    con.execute(f"SET preserve_insertion_order={'true' if preserve_insertion_order else 'false'};")
    if memory_limit:
        con.execute(f"SET memory_limit={sql_quote(str(memory_limit))}")
    if temp_dir:
        temp_path = Path(temp_dir)
        temp_path.mkdir(parents=True, exist_ok=True)
        con.execute(f"PRAGMA temp_directory={sql_quote(str(temp_path))}")
    return con, effective_path


def bootstrap_build_db(con: duckdb.DuckDBPyConnection) -> None:
    for schema in ["meta", "stage", "core", "qa", "mart"]:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.build_runs (
            run_id BIGINT,
            started_at TIMESTAMP,
            input_path VARCHAR,
            dictionary_path VARCHAR,
            years_spec VARCHAR,
            lane_split BOOLEAN,
            exclude_vars VARCHAR,
            typed_output BOOLEAN,
            persist_duckdb BOOLEAN,
            config_json VARCHAR
        )
        """
    )


def record_build_run(
    con: duckdb.DuckDBPyConnection,
    *,
    input_path: str,
    dictionary_path: str | None,
    years_spec: str,
    lane_split: bool,
    exclude_vars: str | None,
    typed_output: bool,
    persist_duckdb: bool,
    config: dict,
) -> int:
    run_id = time.time_ns()
    con.execute(
        """
        INSERT INTO meta.build_runs (
            run_id,
            started_at,
            input_path,
            dictionary_path,
            years_spec,
            lane_split,
            exclude_vars,
            typed_output,
            persist_duckdb,
            config_json
        )
        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            input_path,
            dictionary_path,
            years_spec,
            lane_split,
            exclude_vars,
            typed_output,
            persist_duckdb,
            json.dumps(config, sort_keys=True),
        ],
    )
    return run_id


def copy_query_to_parquet(con: duckdb.DuckDBPyConnection, query: str, out_path: str) -> None:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {sql_quote(str(path))} (FORMAT PARQUET, COMPRESSION SNAPPY)")


def write_query_csv(con: duckdb.DuckDBPyConnection, query: str, out_path: str) -> None:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {sql_quote(str(path))} (HEADER, DELIMITER ',')")
