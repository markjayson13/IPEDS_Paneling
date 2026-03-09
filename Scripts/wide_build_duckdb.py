#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from duckdb_build_utils import (
    bootstrap_build_db,
    copy_query_to_parquet,
    open_build_connection,
    quote_ident,
    record_build_run,
    sql_quote,
    write_query_csv,
)
from wide_build_common import (
    WideBuildRuntime,
    build_disc_groups,
    build_numeric_targets,
    find_anti_garbage_hits,
    load_legacy_schema_seed_manifest,
    order_targets,
    plan_legacy_schema_seeds,
    pick_col,
    pick_optional_col,
    prepare_runtime,
    resolve_disc_names,
)

SCALAR_CONFLICT_KEY_COLS = ("UNITID", "year", "varnumber", "source_file")


def sql_upper_in(values: set[str]) -> str:
    return ", ".join(sql_quote(v) for v in sorted(values))


def build_dimension_expr(dim_sources: set[str], dim_prefixes: tuple[str, ...]) -> str:
    clauses: list[str] = []
    if dim_sources:
        clauses.append(f"source_file IN ({sql_upper_in(dim_sources)})")
    for prefix in dim_prefixes:
        clauses.append(f"SUBSTR(source_file, 1, {len(prefix)}) = {sql_quote(prefix)}")
    return " OR ".join(clauses) if clauses else "FALSE"


def build_stage_long_query(
    *,
    input_path: str,
    years: list[int],
    unitid_col: str,
    year_col: str,
    target_col: str,
    value_col: str,
    source_col: str | None,
    varnumber_col: str | None,
) -> str:
    years_sql = ", ".join(str(y) for y in years)
    source_expr = f"COALESCE(UPPER(TRIM(CAST({quote_ident(source_col)} AS VARCHAR))), '')" if source_col else "''"
    varnumber_expr = f"COALESCE(TRIM(CAST({quote_ident(varnumber_col)} AS VARCHAR)), '')" if varnumber_col else "''"
    null_tokens = ", ".join(sql_quote(x) for x in ["", ".", "nan", "none", "<na>", "na", "nat"])
    return f"""
        CREATE OR REPLACE VIEW stage.long_selected AS
        WITH src AS (
            SELECT
                TRY_CAST({quote_ident(unitid_col)} AS BIGINT) AS UNITID,
                TRY_CAST({quote_ident(year_col)} AS INTEGER) AS year,
                UPPER(TRIM(CAST({quote_ident(target_col)} AS VARCHAR))) AS varname,
                TRIM(CAST({quote_ident(value_col)} AS VARCHAR)) AS value_raw,
                {source_expr} AS source_file,
                {varnumber_expr} AS varnumber
            FROM read_parquet({sql_quote(input_path)})
            WHERE TRY_CAST({quote_ident(year_col)} AS INTEGER) IN ({years_sql})
        )
        SELECT
            UNITID,
            year,
            varname,
            CASE
                WHEN value_raw IS NULL THEN NULL
                WHEN lower(value_raw) IN ({null_tokens}) THEN NULL
                ELSE value_raw
            END AS value,
            CASE
                WHEN value_raw IS NULL THEN NULL
                WHEN lower(value_raw) IN ({null_tokens}) THEN NULL
                ELSE value_raw
            END AS value_norm,
            source_file,
            varnumber
        FROM src
        WHERE UNITID IS NOT NULL
          AND year IS NOT NULL
          AND varname IS NOT NULL
          AND varname <> ''
    """


def file_size_bytes(path: str | None) -> int:
    if not path or path == ":memory:":
        return 0
    fp = Path(path)
    if not fp.exists():
        return 0
    return int(fp.stat().st_size)


def build_where_sql(clauses: list[str]) -> str:
    clean = [c for c in clauses if c]
    if not clean:
        return ""
    return "WHERE " + " AND ".join(clean)


def build_year_base_query(
    *,
    year: int,
    extra_clauses: list[str],
    partition_sql: str,
    dedupe_order_sql: str,
) -> str:
    where_sql = build_where_sql([f"year = {int(year)}"] + extra_clauses)
    return f"""
        WITH deduped AS (
            SELECT
                UNITID,
                year,
                varname,
                value,
                value_norm,
                source_file,
                varnumber
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY {dedupe_order_sql}
                    ) AS _rn
                FROM stage.long_selected
                {where_sql}
            )
            WHERE _rn = 1
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY {dedupe_order_sql}) AS row_id,
            UNITID,
            year,
            varname,
            value,
            value_norm,
            source_file,
            varnumber
        FROM deduped
    """


def append_query_to_parquet(
    con,
    query: str,
    out_path: str,
    writer: pq.ParquetWriter | None,
    schema: pa.Schema | None,
    *,
    rows_per_batch: int = 250_000,
) -> tuple[pq.ParquetWriter | None, pa.Schema | None, bool]:
    reader = con.execute(query).fetch_record_batch(rows_per_batch)
    wrote_any = False
    for batch in reader:
        if batch.num_rows == 0:
            continue
        table = pa.Table.from_batches([batch])
        if writer is None:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            schema = table.schema
            writer = pq.ParquetWriter(out_path, schema, compression="snappy")
        elif schema is not None and table.schema != schema:
            table = table.cast(schema, safe=False)
        writer.write_table(table)
        wrote_any = True
    return writer, schema, wrote_any


def append_rowid_windowed_parquet(
    con,
    *,
    source_table: str,
    select_columns: list[str],
    out_path: str,
    writer: pq.ParquetWriter | None,
    schema: pa.Schema | None,
    rows_per_batch: int = 250_000,
) -> tuple[pq.ParquetWriter | None, pa.Schema | None, bool]:
    min_row_id, max_row_id = con.execute(
        f"SELECT MIN(row_id), MAX(row_id) FROM {source_table}"
    ).fetchone()
    if min_row_id is None or max_row_id is None:
        return writer, schema, False

    wrote_any = False
    select_sql = ", ".join(select_columns)
    start = int(min_row_id)
    stop = int(max_row_id)
    while start <= stop:
        end = start + rows_per_batch - 1
        query = f"""
            SELECT {select_sql}
            FROM {source_table}
            WHERE row_id BETWEEN {start} AND {end}
        """
        reader = con.execute(query).fetch_record_batch(rows_per_batch)
        for batch in reader:
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch])
            if writer is None:
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                schema = table.schema
                writer = pq.ParquetWriter(out_path, schema, compression="snappy")
            elif schema is not None and table.schema != schema:
                table = table.cast(schema, safe=False)
            writer.write_table(table)
            wrote_any = True
        start = end + 1
    return writer, schema, wrote_any


def build_hash_bucket_expr(
    key_cols: tuple[str, ...] | list[str],
    bucket_count: int,
    table_alias: str | None = None,
) -> str:
    if int(bucket_count) < 1:
        raise ValueError("bucket_count must be >= 1")
    prefix = f"{table_alias}." if table_alias else ""
    key_exprs: list[str] = []
    for col in key_cols:
        col_name = f"{prefix}{col}"
        if col in {"UNITID", "year"}:
            key_exprs.append(f"COALESCE(CAST({col_name} AS BIGINT), -1)")
        else:
            key_exprs.append(f"COALESCE(CAST({col_name} AS VARCHAR), '')")
    return f"CAST(HASH({', '.join(key_exprs)}) % {int(bucket_count)} AS BIGINT)"


def build_scalar_conflict_bucket_expr(bucket_count: int, table_alias: str | None = None) -> str:
    return build_hash_bucket_expr(SCALAR_CONFLICT_KEY_COLS, bucket_count, table_alias=table_alias)


def create_empty_year_scalar_conflict_keys(con) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE year_scalar_conflict_keys AS
        SELECT
            CAST(NULL AS BIGINT) AS UNITID,
            CAST(NULL AS INTEGER) AS year,
            CAST(NULL AS VARCHAR) AS varnumber,
            CAST(NULL AS VARCHAR) AS source_file,
            CAST(NULL AS BIGINT) AS distinct_values
        WHERE 1 = 0
        """
    )


def build_year_scalar_conflict_bucket_insert_sql(
    *,
    source_table: str,
    bucket_expr: str,
    bucket_id: int,
) -> str:
    return f"""
        INSERT INTO year_scalar_conflict_keys
        SELECT
            UNITID,
            year,
            varnumber,
            source_file,
            CAST(COUNT(DISTINCT value_norm) AS BIGINT) AS distinct_values
        FROM {source_table}
        WHERE value_norm IS NOT NULL
          AND ({bucket_expr}) = {int(bucket_id)}
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(DISTINCT value_norm) > 1
    """


def resolve_profile_dir(args, runtime: WideBuildRuntime) -> Path | None:
    if args.profile_year is None:
        return None
    if args.profile_dir:
        path = Path(args.profile_dir)
    elif args.qc_dir:
        path = Path(args.qc_dir) / "sql_profiles"
    else:
        path = runtime.repo_root / "Checks" / "wide_qc" / "sql_profiles"
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_explain_artifact(
    con,
    *,
    profile_dir: Path | None,
    year: int,
    label: str,
    query: str,
    analyze: bool,
) -> None:
    if profile_dir is None:
        return
    explain_prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    plan_df = con.execute(f"{explain_prefix} {query}").fetchdf()
    out_path = profile_dir / f"year_{year}_{label}_{'analyze' if analyze else 'plan'}.txt"
    with out_path.open("w", encoding="utf-8") as fh:
        fh.write("-- QUERY\n")
        fh.write(query.strip())
        fh.write("\n\n")
        fh.write(f"-- {explain_prefix}\n")
        if "explain_value" in plan_df.columns:
            for row in plan_df.to_dict("records"):
                explain_key = str(row.get("explain_key", "") or "").strip()
                explain_value = str(row.get("explain_value", "") or "").strip()
                if explain_key:
                    fh.write(explain_key)
                    fh.write("\n")
                if explain_value:
                    fh.write(explain_value)
                    fh.write("\n")
        else:
            fh.write(plan_df.to_string(index=False))
            fh.write("\n")


def compute_year_scalar_conflict_keys_bucketed(
    con,
    *,
    year: int,
    source_table: str,
    bucket_count: int,
    log_phase_fn,
    profile_dir: Path | None,
    profile_analyze: bool,
) -> int:
    create_empty_year_scalar_conflict_keys(con)
    bucket_expr = build_scalar_conflict_bucket_expr(bucket_count)
    for bucket_id in range(int(bucket_count)):
        bucket_started = time.monotonic()
        bucket_rows = scalar_int(
            con,
            f"""
            SELECT COUNT(*)
            FROM {source_table}
            WHERE value_norm IS NOT NULL
              AND ({bucket_expr}) = {bucket_id}
            """,
        )
        log_phase_fn(
            f"year {year} scalar conflict bucket {bucket_id + 1}/{bucket_count} start",
            bucket_rows=bucket_rows,
        )
        if bucket_rows:
            insert_sql = build_year_scalar_conflict_bucket_insert_sql(
                source_table=source_table,
                bucket_expr=bucket_expr,
                bucket_id=bucket_id,
            )
            if bucket_id == 0:
                conflict_select_sql = insert_sql.split("INSERT INTO year_scalar_conflict_keys", 1)[1].strip()
                write_explain_artifact(
                    con,
                    profile_dir=profile_dir,
                    year=year,
                    label="scalar_conflict_bucket_0",
                    query=conflict_select_sql,
                    analyze=profile_analyze,
                )
            before_count = scalar_int(con, "SELECT COUNT(*) FROM year_scalar_conflict_keys")
            con.execute(insert_sql)
            after_count = scalar_int(con, "SELECT COUNT(*) FROM year_scalar_conflict_keys")
            bucket_conflict_keys = after_count - before_count
        else:
            bucket_conflict_keys = 0
            after_count = scalar_int(con, "SELECT COUNT(*) FROM year_scalar_conflict_keys")
        log_phase_fn(
            f"year {year} scalar conflict bucket {bucket_id + 1}/{bucket_count} end",
            bucket_rows=bucket_rows,
            bucket_conflict_keys=bucket_conflict_keys,
            accumulator_conflict_keys=after_count,
            elapsed_seconds=f"{time.monotonic() - bucket_started:.3f}",
        )
    return scalar_int(con, "SELECT COUNT(*) FROM year_scalar_conflict_keys")


def stitch_parquet_files(part_paths: list[str], out_path: str) -> None:
    writer: pq.ParquetWriter | None = None
    try:
        for part_path in part_paths:
            table = pq.ParquetFile(part_path).read()
            if writer is None:
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def create_empty_conflicts(con) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE qa.scalar_conflicts AS
        SELECT
            CAST(NULL AS BIGINT) AS row_id,
            CAST(NULL AS BIGINT) AS UNITID,
            CAST(NULL AS INTEGER) AS year,
            CAST(NULL AS VARCHAR) AS varname,
            CAST(NULL AS VARCHAR) AS value,
            CAST(NULL AS VARCHAR) AS value_norm,
            CAST(NULL AS VARCHAR) AS varnumber,
            CAST(NULL AS VARCHAR) AS source_file,
            CAST(NULL AS BIGINT) AS distinct_values
        WHERE 1 = 0
        """
    )


def create_empty_disc_conflicts(con) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE qa.disc_conflicts AS
        SELECT
            CAST(NULL AS BIGINT) AS row_id,
            CAST(NULL AS BIGINT) AS UNITID,
            CAST(NULL AS INTEGER) AS year,
            CAST(NULL AS VARCHAR) AS varname,
            CAST(NULL AS VARCHAR) AS value,
            CAST(NULL AS VARCHAR) AS value_norm,
            CAST(NULL AS VARCHAR) AS source_file,
            CAST(NULL AS VARCHAR) AS varnumber,
            CAST(NULL AS VARCHAR) AS base,
            CAST(NULL AS VARCHAR) AS suffix,
            CAST(NULL AS BOOLEAN) AS is_active,
            CAST(NULL AS BIGINT) AS n_active
        WHERE 1 = 0
        """
    )


def create_empty_cast_report(con) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE qa.cast_report AS
        SELECT
            CAST(NULL AS INTEGER) AS year,
            CAST(NULL AS VARCHAR) AS column,
            CAST(NULL AS BIGINT) AS non_empty_tokens,
            CAST(NULL AS BIGINT) AS parsed_numeric_tokens,
            CAST(NULL AS BIGINT) AS failed_parse_tokens
        WHERE 1 = 0
        """
    )


def register_df_as_table(con, table_name: str, df: pd.DataFrame) -> None:
    temp_name = table_name.replace(".", "_") + "_df"
    con.register(temp_name, df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {temp_name}")
    con.unregister(temp_name)


def build_target_lineage_df(
    *,
    stage_df: pd.DataFrame,
    scalar_df: pd.DataFrame,
    legacy_seed_plan_df: pd.DataFrame,
    seeded_legacy_names: set[str],
    exclude_vars: set[str],
    targets_with_data: set[str],
    component_vars: set[str],
    var_to_group: dict[str, tuple[str, str]],
    disc_name_map: dict[str, str],
    anti_hits_initial: list[str],
    anti_hits_final: list[str],
    final_targets: list[str],
    drop_empty_cols: bool,
    drop_disc_components: bool,
    drop_anti_garbage_cols: bool,
) -> pd.DataFrame:
    stage_stats = {
        str(row["varname"]): {
            "stage_rows": int(row["stage_rows"]),
            "stage_non_empty_rows": int(row["stage_non_empty_rows"]),
        }
        for row in stage_df.to_dict("records")
    }
    scalar_stats = {
        str(row["varname"]): {
            "scalar_rows": int(row["scalar_rows"]),
            "scalar_non_empty_rows": int(row["scalar_non_empty_rows"]),
        }
        for row in scalar_df.to_dict("records")
    }
    legacy_seed_meta = {
        str(row["column_name"]): {
            "seed_reason": str(row["seed_reason"]),
            "dtype": str(row["dtype"]),
            "source_contract": str(row["source_contract"]),
        }
        for row in legacy_seed_plan_df.to_dict("records")
    }
    disc_output_lookup = {base: out for base, out in disc_name_map.items()}
    output_names = set(disc_name_map.values())
    anti_hits_initial_set = set(anti_hits_initial)
    anti_hits_final_set = set(anti_hits_final)
    final_rank = {name: idx + 1 for idx, name in enumerate(final_targets)}

    all_names = set(stage_stats) | set(scalar_stats) | set(component_vars) | set(final_targets) | output_names | exclude_vars | set(legacy_seed_meta)
    for base, out_name in disc_name_map.items():
        all_names.add(base)
        all_names.add(out_name)

    rows: list[dict] = []
    for name in sorted(all_names):
        stage_info = stage_stats.get(name, {})
        scalar_info = scalar_stats.get(name, {})
        seed_info = legacy_seed_meta.get(name, {})
        base, suffix = var_to_group.get(name, ("", ""))
        output_varname = disc_output_lookup.get(base, "") if base else ""
        rows.append(
            {
                "varname": name,
                "excluded_by_user": name in exclude_vars,
                "present_in_stage_long_selected": name in stage_stats,
                "stage_rows": int(stage_info.get("stage_rows", 0)),
                "stage_non_empty_rows": int(stage_info.get("stage_non_empty_rows", 0)),
                "present_after_lane_scalar_filter": name in scalar_stats,
                "scalar_rows": int(scalar_info.get("scalar_rows", 0)),
                "scalar_non_empty_rows": int(scalar_info.get("scalar_non_empty_rows", 0)),
                "non_empty_after_scalar_filter": name in targets_with_data,
                "dropped_as_globally_empty": bool(drop_empty_cols and name in scalar_stats and name not in targets_with_data),
                "present_in_disc_group_map": name in var_to_group,
                "disc_group_base": base,
                "disc_group_suffix": suffix,
                "disc_output_varname": output_varname,
                "legacy_seed_reason": seed_info.get("seed_reason", ""),
                "legacy_seed_dtype": seed_info.get("dtype", ""),
                "legacy_seed_source_contract": seed_info.get("source_contract", ""),
                "seeded_for_legacy_schema": name in seeded_legacy_names,
                "removed_as_disc_component": bool(drop_disc_components and name in component_vars and name not in final_rank),
                "added_as_disc_output": name in output_names,
                "anti_garbage_hit_initial": name in anti_hits_initial_set,
                "removed_as_anti_garbage": bool(drop_anti_garbage_cols and name in anti_hits_initial_set and name not in final_rank),
                "anti_garbage_hit_final": name in anti_hits_final_set,
                "final_in_all_targets": name in final_rank,
                "final_target_rank": final_rank.get(name),
            }
        )
    return pd.DataFrame(rows)


def build_wide_query(targets: list[str], source_table: str, spine_table: str = "stage.spine") -> str:
    if not targets:
        return """
            SELECT
                CAST(s.year AS INTEGER) AS year,
                CAST(s.UNITID AS BIGINT) AS UNITID
            FROM {spine_table} s
            ORDER BY s.year, s.UNITID
        """.format(spine_table=spine_table)
    exprs = [
        f"MAX(CASE WHEN a.varname = {sql_quote(t)} THEN a.value END) AS {quote_ident(t)}"
        for t in targets
    ]
    select_sql = ",\n                ".join(exprs)
    return f"""
        SELECT
            CAST(s.year AS INTEGER) AS year,
            CAST(s.UNITID AS BIGINT) AS UNITID,
            {select_sql}
        FROM {spine_table} s
        LEFT JOIN {source_table} a
          ON s.year = a.year
         AND s.UNITID = a.UNITID
        GROUP BY s.year, s.UNITID
        ORDER BY s.year, s.UNITID
    """


def build_typed_wide_query(targets: list[str], numeric_targets: set[str], source_table: str = "mart.panel_wide_raw") -> str:
    select_exprs = ["CAST(year AS INTEGER) AS year", "CAST(UNITID AS BIGINT) AS UNITID"]
    for target in targets:
        ident = quote_ident(target)
        if target in numeric_targets:
            select_exprs.append(f"TRY_CAST({ident} AS DOUBLE) AS {ident}")
        else:
            select_exprs.append(f"CAST({ident} AS VARCHAR) AS {ident}")
    return f"""
        SELECT
            {", ".join(select_exprs)}
        FROM {source_table}
        ORDER BY year, UNITID
    """


def build_non_null_count_query(targets: list[str], source_table: str = "mart.panel_wide") -> str | None:
    if not targets:
        return None
    exprs = [f"SUM(CASE WHEN {quote_ident(t)} IS NOT NULL THEN 1 ELSE 0 END) AS {quote_ident(t)}" for t in targets]
    return f"SELECT {', '.join(exprs)} FROM {source_table}"


def build_cast_report_query(numeric_targets: list[str], source_table: str = "mart.panel_wide_raw") -> str | None:
    if not numeric_targets:
        return None
    unions = []
    for target in numeric_targets:
        ident = quote_ident(target)
        unions.append(
            f"""
            SELECT
                year,
                {sql_quote(target)} AS column,
                CAST(COUNT({ident}) AS BIGINT) AS non_empty_tokens,
                CAST(SUM(CASE WHEN TRY_CAST({ident} AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS parsed_numeric_tokens,
                CAST(COUNT({ident}) - SUM(CASE WHEN TRY_CAST({ident} AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS failed_parse_tokens
            FROM {source_table}
            GROUP BY year
            """
        )
    return "\nUNION ALL\n".join(unions)


def log_phase(message: str, **fields) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    suffix = " ".join(f"{key}={value}" for key, value in fields.items())
    if suffix:
        print(f"[phase {stamp}] {message} {suffix}", flush=True)
    else:
        print(f"[phase {stamp}] {message}", flush=True)


def scalar_int(con, query: str) -> int:
    value = con.execute(query).fetchone()[0]
    if value is None:
        return 0
    return int(value)


def run(args) -> None:
    runtime: WideBuildRuntime = prepare_runtime(args)
    years = runtime.years
    profile_dir = resolve_profile_dir(args, runtime)
    if max(years) >= 2024:
        print(
            "[warn] 2024 is treated as provisional/schema-transition; prefer 2004:2023 for analysis releases.",
            flush=True,
        )

    dataset = ds.dataset(args.input, format="parquet")
    schema = dataset.schema

    unitid_col = pick_col(schema, ["UNITID", "unitid"])
    year_col = pick_col(schema, ["year", "academicyear"])
    target_col = pick_col(schema, ["varname", "target_var", "concept", "target"])
    value_col = pick_col(schema, ["value", "val"])
    source_col = pick_optional_col(schema, ["source_file", "source"])
    varnumber_col = pick_optional_col(schema, ["varnumber", "var_num", "number"])
    if args.lane_split and (source_col is None or varnumber_col is None):
        raise SystemExit("lane-split requires source_file and varnumber columns in long input.")

    con, effective_db_path = open_build_connection(
        args.duckdb_path,
        args.duckdb_temp_dir,
        args.persist_duckdb,
        memory_limit=args.duckdb_memory_limit,
        threads=2,
        preserve_insertion_order=False,
    )
    bootstrap_build_db(con)
    print(f"[info] DuckDB build state: {effective_db_path}", flush=True)

    config = {
        "anti_garbage_ids": args.anti_garbage_ids,
        "collapse_disc": args.collapse_disc,
        "dim_prefixes": args.dim_prefixes,
        "dim_sources": args.dim_sources,
        "drop_anti_garbage_cols": args.drop_anti_garbage_cols,
        "drop_disc_components": args.drop_disc_components,
        "drop_empty_cols": args.drop_empty_cols,
        "drop_globally_null_post": args.drop_globally_null_post,
        "exclude_vars": args.exclude_vars,
        "fail_on_anti_garbage": args.fail_on_anti_garbage,
        "fail_on_scalar_conflicts": args.fail_on_scalar_conflicts,
        "lane_split": args.lane_split,
        "duckdb_memory_limit": args.duckdb_memory_limit,
        "typed_output": args.typed_output,
        "scalar_conflict_buckets": args.scalar_conflict_buckets,
        "scalar_conflict_bucket_min_year": args.scalar_conflict_bucket_min_year,
    }
    record_build_run(
        con,
        input_path=args.input,
        dictionary_path=args.dictionary,
        years_spec=args.years,
        lane_split=args.lane_split,
        exclude_vars=args.exclude_vars,
        typed_output=args.typed_output,
        persist_duckdb=args.persist_duckdb,
        config=config,
    )

    if args.dictionary:
        con.execute(f"CREATE OR REPLACE TABLE meta.dictionary_lake AS SELECT * FROM read_parquet({sql_quote(args.dictionary)})")

    register_started = time.monotonic()
    register_db_before = file_size_bytes(effective_db_path)
    log_phase("register parquet input start", input=args.input, years=args.years)
    con.execute(
        build_stage_long_query(
            input_path=args.input,
            years=years,
            unitid_col=unitid_col,
            year_col=year_col,
            target_col=target_col,
            value_col=value_col,
            source_col=source_col,
            varnumber_col=varnumber_col,
        )
    )
    register_elapsed = time.monotonic() - register_started
    register_db_after = file_size_bytes(effective_db_path)
    register_db_growth = register_db_after - register_db_before
    log_phase(
        "register parquet input end",
        elapsed_seconds=f"{register_elapsed:.3f}",
        stage_cols=7,
        db_growth_bytes=register_db_growth,
        registered_object="view",
    )
    if args.persist_duckdb and register_elapsed > 30 and register_db_growth > 64 * 1024 * 1024:
        raise SystemExit("registration is materializing the input parquet instead of registering it lazily")
    log_phase("spine materialization start")
    con.execute("CREATE OR REPLACE TABLE stage.spine AS SELECT DISTINCT year, UNITID FROM stage.long_selected ORDER BY year, UNITID")
    log_phase("spine materialization end", spine_rows=scalar_int(con, "SELECT COUNT(*) FROM stage.spine"))

    dedupe_partition = ["UNITID", "year", "varname", "value_norm"]
    if args.lane_split:
        dedupe_partition.extend(["varnumber", "source_file"])
    partition_sql = ", ".join(dedupe_partition)
    where_sql = f"WHERE varname NOT IN ({sql_upper_in(runtime.exclude_vars)})" if runtime.exclude_vars else ""
    dedupe_order_sql = ", ".join(
        [
            "year",
            "UNITID",
            "varname",
            "source_file",
            "varnumber",
            "COALESCE(value_norm, '')",
            "COALESCE(value, '')",
        ]
    )
    log_phase("analysis base build start")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW core.analysis_long_base AS
        WITH deduped AS (
            SELECT
                UNITID,
                year,
                varname,
                value,
                value_norm,
                source_file,
                varnumber
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY {dedupe_order_sql}
                    ) AS _rn
                FROM stage.long_selected
                {where_sql}
            )
            WHERE _rn = 1
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY {dedupe_order_sql}) AS row_id,
            UNITID,
            year,
            varname,
            value,
            value_norm,
            source_file,
            varnumber
        FROM deduped
        """
    )
    log_phase("analysis base build end", materialized_object="view")

    exclude_clause = f"varname NOT IN ({sql_upper_in(runtime.exclude_vars)})" if runtime.exclude_vars else ""
    year_base_clauses = [exclude_clause] if exclude_clause else []
    dimension_expr = None
    if args.lane_split:
        log_phase(
            "lane split planning start",
            dim_sources=len(runtime.dim_sources),
            dim_prefixes=len(runtime.dim_prefixes),
        )
        dimension_expr = build_dimension_expr(runtime.dim_sources, runtime.dim_prefixes)
        log_phase(
            "lane split planning end",
            strategy="year_scoped",
            scalar_export=bool(args.scalar_long_out),
            dim_export=bool(args.dim_long_out),
        )

    create_empty_conflicts(con)
    create_empty_cast_report(con)
    create_empty_disc_conflicts(con)

    log_phase("discover targets start")
    stage_target_df = con.execute(
        """
        SELECT
            varname,
            COUNT(*) AS stage_rows,
            COUNT(*) FILTER (WHERE value_norm IS NOT NULL) AS stage_non_empty_rows
        FROM stage.long_selected
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    log_phase("non-empty scan start")
    target_scan_clauses = list(year_base_clauses)
    if args.lane_split and dimension_expr:
        target_scan_clauses.append(f"NOT ({dimension_expr})")
    target_df = con.execute(
        f"""
        SELECT
            varname,
            COUNT(*) AS scalar_rows,
            COUNT(*) FILTER (WHERE value_norm IS NOT NULL) AS non_empty_rows
        FROM stage.long_selected
        {build_where_sql(target_scan_clauses)}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    targets = target_df["varname"].tolist()
    discovered_targets = len(targets)
    targets_with_data = set(target_df.loc[target_df["non_empty_rows"] > 0, "varname"].tolist())
    legacy_seed_plan_df = pd.DataFrame(columns=["column_name", "seed_reason", "dtype", "source_contract", "present_in_target_universe", "seeded_for_compatibility"])
    seeded_legacy_df = legacy_seed_plan_df.copy()
    legacy_seed_columns: set[str] = set()
    seeded_legacy_names: set[str] = set()
    if args.lane_split and runtime.legacy_analysis_schema:
        legacy_manifest_df = load_legacy_schema_seed_manifest(runtime.legacy_schema_seed_manifest)
        legacy_seed_plan_df, seeded_legacy_df = plan_legacy_schema_seeds(legacy_manifest_df, targets)
        legacy_seed_columns = set(legacy_seed_plan_df["column_name"].tolist())
        seeded_legacy_names = set(seeded_legacy_df["column_name"].tolist())
        if seeded_legacy_names:
            targets.extend(sorted(seeded_legacy_names))
    log_phase("non-empty scan end", non_empty_targets=len(targets_with_data))
    all_targets = order_targets(targets)
    log_phase("discover targets end", discovered_targets=discovered_targets, ordered_targets=len(all_targets))

    if args.drop_empty_cols:
        before = len(all_targets)
        all_targets = [t for t in all_targets if t in targets_with_data or t in legacy_seed_columns]
        dropped = before - len(all_targets)
        if dropped > 0:
            print(
                f"[info] dropped {dropped} globally-empty variables (no non-empty values in selected years)",
                flush=True,
            )
    register_df_as_table(con, "qa.seeded_legacy_columns", seeded_legacy_df)
    if runtime.seeded_legacy_out:
        Path(runtime.seeded_legacy_out).parent.mkdir(parents=True, exist_ok=True)
        seeded_legacy_df.to_csv(runtime.seeded_legacy_out, index=False)
    if not seeded_legacy_df.empty:
        print(f"[info] seeded {len(seeded_legacy_df)} legacy compatibility columns into wide targets", flush=True)

    numeric_targets = set()
    if args.typed_output:
        log_phase("numeric target build start")
        numeric_targets = build_numeric_targets(args.dictionary, all_targets)
        log_phase("numeric target build end", numeric_targets=len(numeric_targets))
        print(
            f"[info] typed output enabled: numeric vars={len(numeric_targets)} string vars={len(all_targets) - len(numeric_targets)}",
            flush=True,
        )

    var_to_group, group_to_vars = ({}, {})
    if args.collapse_disc:
        var_to_group, group_to_vars = build_disc_groups(args.dictionary)
        if args.disc_exclude:
            excludes = {x.strip().upper() for x in args.disc_exclude.split(",") if x.strip()}
            if excludes:
                group_to_vars = {k: v for k, v in group_to_vars.items() if k.upper() not in excludes}
                var_to_group = {v: grp for v, grp in var_to_group.items() if grp[0].upper() not in excludes}
    disc_name_map = {}
    component_vars: set[str] = set()
    if args.collapse_disc and group_to_vars:
        disc_name_map = resolve_disc_names(group_to_vars, set(all_targets), suffix=args.disc_suffix)
        for base, new_name in disc_name_map.items():
            if new_name not in all_targets:
                all_targets.append(new_name)
        component_vars = {v for vs in group_to_vars.values() for v in vs}
        if args.drop_disc_components:
            all_targets = [t for t in all_targets if t not in component_vars]

    anti_hits_initial = find_anti_garbage_hits(all_targets, runtime.anti_garbage_ids)
    anti_hits = list(anti_hits_initial)
    anti_df = pd.DataFrame({"blocked_identifier_column": anti_hits_initial})
    register_df_as_table(con, "qa.anti_garbage_hits", anti_df)
    if anti_hits_initial and runtime.anti_garbage_out:
        anti_df.to_csv(runtime.anti_garbage_out, index=False)
        print(
            f"[warn] anti-garbage hits written: {runtime.anti_garbage_out} (count={len(anti_hits_initial)})",
            flush=True,
        )
    if anti_hits and args.drop_anti_garbage_cols:
        all_targets = [t for t in all_targets if t not in set(anti_hits)]
        print(f"[info] dropped {len(anti_hits)} anti-garbage identifier columns from wide targets", flush=True)
        anti_hits = find_anti_garbage_hits(all_targets, runtime.anti_garbage_ids)
        anti_df = pd.DataFrame({"blocked_identifier_column": anti_hits})
        register_df_as_table(con, "qa.anti_garbage_hits", anti_df)
    if anti_hits and args.fail_on_anti_garbage:
        raise SystemExit(f"anti-garbage gate failed: {len(anti_hits)} blocked dimension identifiers present in wide targets")

    print(f"[info] years: {years[0]}–{years[-1]} ({len(years)} total)", flush=True)
    print(f"[info] wide columns (varname): {len(all_targets)}", flush=True)

    log_phase("target lineage build start")
    scalar_lineage_df = target_df.rename(columns={"non_empty_rows": "scalar_non_empty_rows"})
    lineage_df = build_target_lineage_df(
        stage_df=stage_target_df,
        scalar_df=scalar_lineage_df,
        legacy_seed_plan_df=legacy_seed_plan_df,
        seeded_legacy_names=seeded_legacy_names,
        exclude_vars=runtime.exclude_vars,
        targets_with_data=targets_with_data,
        component_vars=component_vars,
        var_to_group=var_to_group,
        disc_name_map=disc_name_map,
        anti_hits_initial=anti_hits_initial,
        anti_hits_final=anti_hits,
        final_targets=all_targets,
        drop_empty_cols=args.drop_empty_cols,
        drop_disc_components=args.drop_disc_components,
        drop_anti_garbage_cols=args.drop_anti_garbage_cols,
    )
    register_df_as_table(con, "qa.target_lineage", lineage_df)
    if runtime.target_lineage_out:
        Path(runtime.target_lineage_out).parent.mkdir(parents=True, exist_ok=True)
        lineage_df.to_csv(runtime.target_lineage_out, index=False)
    log_phase("target lineage build end", rows=len(lineage_df), output=runtime.target_lineage_out or "")
    if args.lineage_only:
        log_phase("lineage-only exit")
        return

    if args.collapse_disc and group_to_vars:
        disc_rows = [
            {"varname": varname, "base": base, "suffix": suffix, "output_varname": disc_name_map.get(base, base)}
            for varname, (base, suffix) in var_to_group.items()
        ]
        disc_map_df = pd.DataFrame(disc_rows)
        register_df_as_table(con, "stage.disc_map", disc_map_df)
        register_df_as_table(con, "stage.disc_output_names", disc_map_df[["base", "output_varname"]].drop_duplicates())

    year_part_paths: list[str] = []
    qc_rows: list[dict] = []
    cast_report_frames: list[pd.DataFrame] = []
    scalar_conflict_frames: list[pd.DataFrame] = []
    scalar_conflict_rows_written = 0
    scalar_part_paths: list[str] = []
    dim_part_paths: list[str] = []
    scalar_parts_dir = f"{args.scalar_long_out}.parts" if args.scalar_long_out else None
    dim_parts_dir = f"{args.dim_long_out}.parts" if args.dim_long_out else None

    for year in years:
        log_phase(f"year {year} start")
        con.execute(f"CREATE OR REPLACE TEMP VIEW year_spine AS SELECT year, UNITID FROM stage.spine WHERE year = {int(year)}")
        spine_rows = scalar_int(con, "SELECT COUNT(*) FROM year_spine")
        log_phase(f"year {year} spine done", rows=spine_rows)

        log_phase(f"year {year} base query start")
        year_base_query = build_year_base_query(
            year=year,
            extra_clauses=year_base_clauses,
            partition_sql=partition_sql,
            dedupe_order_sql=dedupe_order_sql,
        )
        if args.profile_year == year:
            write_explain_artifact(
                con,
                profile_dir=profile_dir,
                year=year,
                label="year_long_base",
                query=year_base_query,
                analyze=args.profile_analyze,
            )
        con.execute(f"CREATE OR REPLACE TEMP VIEW year_long_base AS {year_base_query}")
        log_phase(f"year {year} base query end", materialized_object="view")

        year_source_table = "year_long_base"
        dim_export_clauses: list[str] | None = None
        if args.lane_split and dimension_expr:
            scalar_lane_query = build_year_base_query(
                year=year,
                extra_clauses=year_base_clauses + [f"NOT ({dimension_expr})"],
                partition_sql=partition_sql,
                dedupe_order_sql=dedupe_order_sql,
            )
            if args.profile_year == year:
                write_explain_artifact(
                    con,
                    profile_dir=profile_dir,
                    year=year,
                    label="scalar_lane_materialization",
                    query=scalar_lane_query,
                    analyze=args.profile_analyze,
                )
            log_phase(f"year {year} scalar lane materialization start")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE year_scalar_long_raw AS
                {scalar_lane_query}
                """
            )
            scalar_raw_rows = scalar_int(con, "SELECT COUNT(*) FROM year_scalar_long_raw")
            log_phase(f"year {year} scalar lane materialization end", rows=scalar_raw_rows)

            if args.dim_long_out:
                dim_export_clauses = [f"year = {int(year)}", *year_base_clauses, f"({dimension_expr})"]

            use_bucketed_conflicts = int(args.scalar_conflict_buckets) > 1 and int(year) >= int(args.scalar_conflict_bucket_min_year)
            log_phase(
                f"year {year} scalar conflict scan start",
                strategy="bucketed" if use_bucketed_conflicts else "monolithic",
                buckets=int(args.scalar_conflict_buckets) if use_bucketed_conflicts else 1,
            )
            if use_bucketed_conflicts:
                conflict_key_count = compute_year_scalar_conflict_keys_bucketed(
                    con,
                    year=year,
                    source_table="year_scalar_long_raw",
                    bucket_count=int(args.scalar_conflict_buckets),
                    log_phase_fn=log_phase,
                    profile_dir=profile_dir if args.profile_year == year else None,
                    profile_analyze=bool(args.profile_analyze),
                )
            else:
                monolithic_conflict_sql = """
                    SELECT
                        UNITID,
                        year,
                        varnumber,
                        source_file,
                        CAST(COUNT(DISTINCT value_norm) AS BIGINT) AS distinct_values
                    FROM year_scalar_long_raw
                    GROUP BY 1, 2, 3, 4
                    HAVING COUNT(DISTINCT value_norm) > 1
                """
                if args.profile_year == year:
                    write_explain_artifact(
                        con,
                        profile_dir=profile_dir,
                        year=year,
                        label="scalar_conflict_monolithic",
                        query=monolithic_conflict_sql,
                        analyze=args.profile_analyze,
                    )
                create_empty_year_scalar_conflict_keys(con)
                con.execute(f"INSERT INTO year_scalar_conflict_keys {monolithic_conflict_sql}")
                conflict_key_count = scalar_int(con, "SELECT COUNT(*) FROM year_scalar_conflict_keys")
            conflict_key_count = scalar_int(con, "SELECT COUNT(*) FROM year_scalar_conflict_keys")
            log_phase(f"year {year} scalar conflict scan end", conflict_keys=conflict_key_count)
            if conflict_key_count:
                remaining = max(int(args.scalar_conflicts_max_rows) - scalar_conflict_rows_written, 0)
                if remaining > 0:
                    conflict_df = con.execute(
                        f"""
                        SELECT
                            s.UNITID,
                            s.year,
                            s.varname,
                            s.value,
                            s.varnumber,
                            s.source_file,
                            k.distinct_values
                        FROM year_scalar_long_raw s
                        INNER JOIN year_scalar_conflict_keys k
                          ON s.UNITID = k.UNITID
                         AND s.year = k.year
                         AND s.varnumber = k.varnumber
                         AND s.source_file = k.source_file
                        ORDER BY year, UNITID, varnumber, source_file, row_id
                        LIMIT {remaining}
                        """
                    ).fetchdf()
                    if not conflict_df.empty:
                        scalar_conflict_frames.append(conflict_df)
                        scalar_conflict_rows_written += len(conflict_df)
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE year_scalar_long_clean AS
                SELECT
                    s.row_id,
                    s.UNITID,
                    s.year,
                    s.varname,
                    s.value,
                    s.value_norm,
                    s.source_file,
                    s.varnumber
                FROM year_scalar_long_raw s
                LEFT JOIN year_scalar_conflict_keys k
                  ON s.UNITID = k.UNITID
                 AND s.year = k.year
                 AND s.varnumber = k.varnumber
                 AND s.source_file = k.source_file
                WHERE k.UNITID IS NULL
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE year_analysis_source AS
                SELECT row_id, UNITID, year, varname, value, value_norm, source_file, varnumber
                FROM year_scalar_long_clean
                """
            )
            if args.scalar_long_out:
                scalar_year_path = os.path.join(str(scalar_parts_dir), f"year={year}", "part.parquet")
                _, _, wrote_scalar = append_rowid_windowed_parquet(
                    con,
                    source_table="year_scalar_long_clean",
                    select_columns=["UNITID", "year", "varname", "value", "varnumber", "source_file"],
                    out_path=scalar_year_path,
                    writer=None,
                    schema=None,
                    rows_per_batch=int(args.scan_batch_rows),
                )
                if wrote_scalar:
                    scalar_part_paths.append(scalar_year_path)
            if conflict_key_count and args.fail_on_scalar_conflicts:
                if runtime.scalar_conflicts_out and scalar_conflict_frames:
                    out_df = pd.concat(scalar_conflict_frames, ignore_index=True)
                    Path(runtime.scalar_conflicts_out).parent.mkdir(parents=True, exist_ok=True)
                    out_df.to_csv(runtime.scalar_conflicts_out, index=False)
                    register_df_as_table(con, "qa.scalar_conflicts", out_df)
                raise SystemExit(f"scalar conflict gate failed for year={year}: conflict_keys={conflict_key_count}")
            year_source_table = "year_analysis_source"
        else:
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE year_analysis_source AS
                SELECT row_id, UNITID, year, varname, value, value_norm, source_file, varnumber
                FROM year_long_base
                """
            )
            year_source_table = "year_analysis_source"

        if args.collapse_disc and group_to_vars:
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE year_disc_active AS
                SELECT
                    row_id,
                    UNITID,
                    year,
                    varname,
                    value,
                    value_norm,
                    source_file,
                    varnumber,
                    base,
                    suffix,
                    is_active
                FROM (
                    SELECT
                        a.row_id,
                        a.UNITID,
                        a.year,
                        a.varname,
                        a.value,
                        a.value_norm,
                        a.source_file,
                        a.varnumber,
                        m.base,
                        m.suffix,
                        CASE
                            WHEN a.value_norm IS NULL THEN FALSE
                            WHEN TRY_CAST(a.value_norm AS DOUBLE) IS NOT NULL THEN TRY_CAST(a.value_norm AS DOUBLE) <> 0
                            WHEN lower(a.value_norm) IN ('y', 'yes', 't', 'true') THEN TRUE
                            WHEN lower(a.value_norm) IN ('n', 'no', 'f', 'false') THEN FALSE
                            ELSE TRUE
                        END AS is_active,
                        ROW_NUMBER() OVER (
                            PARTITION BY a.UNITID, a.year, m.base, m.suffix
                            ORDER BY a.row_id
                        ) AS _rn
                    FROM {year_source_table} a
                    INNER JOIN stage.disc_map m
                      ON a.varname = m.varname
                    WHERE a.value_norm IS NOT NULL
                )
                WHERE is_active
                  AND _rn = 1
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE year_disc_choice AS
                SELECT
                    UNITID,
                    year,
                    base,
                    COUNT(DISTINCT suffix) AS n_active,
                    MIN(suffix) AS chosen_suffix
                FROM year_disc_active
                GROUP BY 1, 2, 3
                """
            )
            if args.disc_qc_dir and scalar_int(con, "SELECT COUNT(*) FROM year_disc_choice WHERE n_active > 1") > 0:
                write_query_csv(
                    con,
                    f"""
                    SELECT
                        a.UNITID,
                        a.year,
                        a.varname,
                        a.value,
                        a.source_file,
                        a.varnumber,
                        a.base,
                        a.suffix,
                        a.is_active,
                        c.n_active
                    FROM year_disc_active a
                    INNER JOIN year_disc_choice c
                      ON a.UNITID = c.UNITID
                     AND a.year = c.year
                     AND a.base = c.base
                    WHERE c.n_active > 1
                    ORDER BY a.row_id
                    """,
                    os.path.join(args.disc_qc_dir, f"disc_conflicts_{year}.csv"),
                )
            offset = scalar_int(con, f"SELECT COALESCE(MAX(row_id), 0) FROM {year_source_table}")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE year_disc_collapsed AS
                SELECT
                    {offset} + ROW_NUMBER() OVER (ORDER BY c.year, c.UNITID, c.base) AS row_id,
                    c.UNITID,
                    c.year,
                    n.output_varname AS varname,
                    c.chosen_suffix AS value,
                    c.chosen_suffix AS value_norm,
                    '' AS source_file,
                    '' AS varnumber
                FROM year_disc_choice c
                INNER JOIN stage.disc_output_names n
                  ON c.base = n.base
                WHERE c.n_active = 1
                """
            )
            component_filter = ""
            if args.drop_disc_components and component_vars:
                component_filter = f"WHERE varname NOT IN ({sql_upper_in(component_vars)})"
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW year_analysis_pre_dedup AS
                SELECT * FROM {year_source_table}
                {component_filter}
                UNION ALL
                SELECT * FROM year_disc_collapsed
                """
            )
        else:
            con.execute(f"CREATE OR REPLACE TEMP VIEW year_analysis_pre_dedup AS SELECT * FROM {year_source_table}")

        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE year_dup_groups AS
            SELECT
                year,
                UNITID,
                varname,
                COUNT(*) AS dup_rows
            FROM year_analysis_pre_dedup
            GROUP BY 1, 2, 3
            HAVING COUNT(*) > 1
            """
        )
        dup_count = scalar_int(con, "SELECT COALESCE(SUM(dup_rows), 0) FROM year_dup_groups")
        if args.dups_qc_dir and args.dups_max_rows > 0 and dup_count > 0:
            Path(args.dups_qc_dir).mkdir(parents=True, exist_ok=True)
            ext = ".csv.gz" if args.dups_qc_gzip else ".csv"
            compression = "gzip" if args.dups_qc_gzip else None
            dup_df = con.execute(
                f"""
                SELECT
                    p.UNITID,
                    p.year,
                    p.varname,
                    p.value,
                    p.varnumber,
                    p.source_file
                FROM year_analysis_pre_dedup p
                INNER JOIN year_dup_groups g
                  ON p.year = g.year
                 AND p.UNITID = g.UNITID
                 AND p.varname = g.varname
                ORDER BY p.row_id
                LIMIT {int(args.dups_max_rows)}
                """
            ).fetchdf()
            dup_df.to_csv(os.path.join(args.dups_qc_dir, f"dups_{year}{ext}"), index=False, compression=compression)

        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE year_analysis_final AS
            SELECT row_id, UNITID, year, varname, value, value_norm, source_file, varnumber
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY UNITID, year, varname
                        ORDER BY row_id
                    ) AS _rn
                FROM year_analysis_pre_dedup
            )
            WHERE _rn = 1
            """
        )
        concept_rows = scalar_int(con, "SELECT COUNT(*) FROM year_analysis_final")
        log_phase(f"year {year} concept done", rows=concept_rows)

        log_phase(f"year {year} pivot start")
        con.execute(f"CREATE OR REPLACE TEMP TABLE year_wide_raw AS {build_wide_query(all_targets, 'year_analysis_final', 'year_spine')}")
        if args.typed_output:
            con.execute(f"CREATE OR REPLACE TEMP TABLE year_wide AS {build_typed_wide_query(all_targets, numeric_targets, 'year_wide_raw')}")
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE year_wide AS SELECT * FROM year_wide_raw ORDER BY year, UNITID")
        wide_rows = scalar_int(con, "SELECT COUNT(*) FROM year_wide")
        log_phase(f"year {year} pivot end", rows=wide_rows)

        out_path = os.path.join(args.out_dir, f"year={year}", "part.parquet")
        copy_query_to_parquet(con, "SELECT * FROM year_wide ORDER BY UNITID", out_path)
        year_part_paths.append(out_path)
        print(f"[info] wrote {out_path}", flush=True)
        log_phase(f"year {year} write complete", path=out_path, rows=wide_rows)

        if args.dim_long_out and dim_export_clauses:
            dim_bucket_count = int(args.scalar_conflict_buckets) if int(args.scalar_conflict_buckets) > 1 and int(year) >= int(args.scalar_conflict_bucket_min_year) else 1
            log_phase(
                f"year {year} dim export start",
                strategy="bucketed" if dim_bucket_count > 1 else "monolithic",
                buckets=dim_bucket_count,
            )
            dim_year_path = os.path.join(str(dim_parts_dir), f"year={year}", "part.parquet")
            dim_writer = None
            dim_schema = None
            wrote_dim = False
            try:
                if dim_bucket_count > 1:
                    dim_bucket_expr = build_hash_bucket_expr(tuple(dedupe_partition), dim_bucket_count)
                    for bucket_id in range(dim_bucket_count):
                        bucket_query = f"""
                            SELECT UNITID, year, varname, value, varnumber, source_file
                            FROM (
                                SELECT DISTINCT
                                    UNITID,
                                    year,
                                    varname,
                                    value,
                                    value_norm,
                                    varnumber,
                                    source_file
                                FROM stage.long_selected
                                {build_where_sql(dim_export_clauses + [f"({dim_bucket_expr}) = {bucket_id}"])}
                            ) q
                        """
                        dim_writer, dim_schema, wrote_bucket = append_query_to_parquet(
                            con,
                            bucket_query,
                            dim_year_path,
                            writer=dim_writer,
                            schema=dim_schema,
                            rows_per_batch=int(args.scan_batch_rows),
                        )
                        wrote_dim = wrote_dim or wrote_bucket
                else:
                    dim_query = f"""
                        SELECT UNITID, year, varname, value, varnumber, source_file
                        FROM (
                            SELECT DISTINCT
                                UNITID,
                                year,
                                varname,
                                value,
                                value_norm,
                                varnumber,
                                source_file
                            FROM stage.long_selected
                            {build_where_sql(dim_export_clauses)}
                        ) q
                    """
                    dim_writer, dim_schema, wrote_dim = append_query_to_parquet(
                        con,
                        dim_query,
                        dim_year_path,
                        writer=None,
                        schema=None,
                        rows_per_batch=int(args.scan_batch_rows),
                    )
            finally:
                if dim_writer is not None:
                    dim_writer.close()
            if wrote_dim:
                dim_part_paths.append(dim_year_path)
            log_phase(f"year {year} dim export end", wrote_part=bool(wrote_dim), path=dim_year_path if wrote_dim else "")

        if args.typed_output:
            numeric_target_list = [t for t in all_targets if t in numeric_targets]
            for start_idx in range(0, len(numeric_target_list), 250):
                cast_query = build_cast_report_query(
                    numeric_target_list[start_idx : start_idx + 250],
                    source_table="year_wide_raw",
                )
                if not cast_query:
                    continue
                year_cast_df = con.execute(cast_query).fetchdf()
                if not year_cast_df.empty:
                    cast_report_frames.append(year_cast_df)

        non_empty_values = scalar_int(con, "SELECT COUNT(*) FROM year_analysis_final WHERE value_norm IS NOT NULL")
        possible = spine_rows * len(all_targets) if spine_rows and all_targets else 0
        fill_rate = (non_empty_values / possible) if possible else 0.0
        qc_rows.append(
            {
                "year": int(year),
                "rows": int(spine_rows),
                "vars": int(len(all_targets)),
                "non_empty_values": int(non_empty_values),
                "fill_rate": float(fill_rate),
                "dup_rows": int(dup_count),
            }
        )

    if scalar_part_paths and args.scalar_long_out:
        stitch_parquet_files(scalar_part_paths, args.scalar_long_out)
        print(f"[info] wrote scalar long lane: {args.scalar_long_out}", flush=True)
    if dim_part_paths and args.dim_long_out:
        stitch_parquet_files(dim_part_paths, args.dim_long_out)
        print(f"[info] wrote dimensioned long lane: {args.dim_long_out}", flush=True)

    if scalar_conflict_frames:
        scalar_conflicts_df = pd.concat(scalar_conflict_frames, ignore_index=True)
        if runtime.scalar_conflicts_out:
            Path(runtime.scalar_conflicts_out).parent.mkdir(parents=True, exist_ok=True)
            scalar_conflicts_df.to_csv(runtime.scalar_conflicts_out, index=False)
            print(f"[info] wrote scalar conflict QC: {runtime.scalar_conflicts_out}", flush=True)
        register_df_as_table(con, "qa.scalar_conflicts", scalar_conflicts_df)

    if cast_report_frames:
        cast_report_df = pd.concat(cast_report_frames, ignore_index=True)
        register_df_as_table(con, "qa.cast_report", cast_report_df)
        if runtime.cast_report_out:
            Path(runtime.cast_report_out).parent.mkdir(parents=True, exist_ok=True)
            cast_report_df.to_csv(runtime.cast_report_out, index=False)
            print(f"[info] wrote cast report QC: {runtime.cast_report_out}", flush=True)

    if qc_rows:
        qc_df = pd.DataFrame(qc_rows)
        register_df_as_table(con, "qa.wide_year_summary", qc_df)

    if args.write_single:
        Path(args.write_single).parent.mkdir(parents=True, exist_ok=True)
        drop_post_cols: set[str] = set()
        if args.drop_globally_null_post and all_targets:
            non_null_counts = {name: 0 for name in all_targets}
            for part_path in year_part_paths:
                pf = pq.ParquetFile(part_path)
                for batch in pf.iter_batches():
                    schema_names = batch.schema.names
                    for name in non_null_counts:
                        if name not in schema_names:
                            continue
                        arr = batch.column(schema_names.index(name))
                        non_null_counts[name] += int(len(arr) - arr.null_count)
            drop_post_cols = {name for name, cnt in non_null_counts.items() if cnt == 0}
            if drop_post_cols and args.qc_dir:
                qc_globally_null = Path(args.qc_dir) / "qc_globally_null_columns_dropped.csv"
                pd.DataFrame({"column": sorted(drop_post_cols)}).to_csv(qc_globally_null, index=False)
                print(f"[info] wrote globally-null drop QC: {qc_globally_null}", flush=True)
        keep_cols = ["year", "UNITID"] + [t for t in all_targets if t not in drop_post_cols]
        log_phase("stitched write start", output=args.write_single, columns=len(keep_cols))
        writer = None
        for part_path in year_part_paths:
            table = pq.ParquetFile(part_path).read()
            if drop_post_cols:
                table = table.select([c for c in table.schema.names if c not in drop_post_cols])
            if writer is None:
                writer = pq.ParquetWriter(args.write_single, table.schema, compression="snappy")
            writer.write_table(table)
        if writer is not None:
            writer.close()
        if drop_post_cols:
            print(f"[info] dropping {len(drop_post_cols)} globally-null columns in stitched output", flush=True)
        log_phase("stitched write end", output=args.write_single)

    if args.qc_dir and qc_rows:
        qc_df.to_csv(os.path.join(args.qc_dir, "wide_panel_qc_summary.csv"), index=False)
        log_phase("qc write end", qc_dir=args.qc_dir)
