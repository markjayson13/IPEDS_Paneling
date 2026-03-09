#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

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
    order_targets,
    pick_col,
    pick_optional_col,
    prepare_runtime,
    resolve_disc_names,
)


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
        CREATE OR REPLACE TABLE stage.long_selected AS
        WITH src AS (
            SELECT
                ROW_NUMBER() OVER () AS row_id,
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
            row_id,
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


def build_wide_query(targets: list[str], source_table: str) -> str:
    if not targets:
        return """
            SELECT
                CAST(s.year AS INTEGER) AS year,
                CAST(s.UNITID AS BIGINT) AS UNITID
            FROM stage.spine s
            ORDER BY s.year, s.UNITID
        """
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
        FROM stage.spine s
        LEFT JOIN {source_table} a
          ON s.year = a.year
         AND s.UNITID = a.UNITID
        GROUP BY s.year, s.UNITID
        ORDER BY s.year, s.UNITID
    """


def build_typed_wide_query(targets: list[str], numeric_targets: set[str]) -> str:
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
        FROM mart.panel_wide_raw
        ORDER BY year, UNITID
    """


def build_non_null_count_query(targets: list[str]) -> str | None:
    if not targets:
        return None
    exprs = [f"SUM(CASE WHEN {quote_ident(t)} IS NOT NULL THEN 1 ELSE 0 END) AS {quote_ident(t)}" for t in targets]
    return f"SELECT {', '.join(exprs)} FROM mart.panel_wide"


def build_cast_report_query(numeric_targets: list[str]) -> str | None:
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
                COUNT({ident}) AS non_empty_tokens,
                SUM(CASE WHEN TRY_CAST({ident} AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS parsed_numeric_tokens,
                COUNT({ident}) - SUM(CASE WHEN TRY_CAST({ident} AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS failed_parse_tokens
            FROM mart.panel_wide_raw
            GROUP BY year
            """
        )
    return "\nUNION ALL\n".join(unions)


def run(args) -> None:
    runtime: WideBuildRuntime = prepare_runtime(args)
    years = runtime.years
    if max(years) >= 2024:
        print("[warn] 2024 is treated as provisional/schema-transition; prefer 2004:2023 for analysis releases.")

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

    con, effective_db_path = open_build_connection(args.duckdb_path, args.duckdb_temp_dir, args.persist_duckdb)
    bootstrap_build_db(con)
    print(f"[info] DuckDB build state: {effective_db_path}")

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
        "typed_output": args.typed_output,
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
    con.execute("CREATE OR REPLACE TABLE stage.spine AS SELECT DISTINCT year, UNITID FROM stage.long_selected ORDER BY year, UNITID")

    dedupe_partition = ["UNITID", "year", "varname", "value_norm"]
    if args.lane_split:
        dedupe_partition.extend(["varnumber", "source_file"])
    partition_sql = ", ".join(dedupe_partition)
    where_sql = f"WHERE varname NOT IN ({sql_upper_in(runtime.exclude_vars)})" if runtime.exclude_vars else ""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE core.analysis_long_base AS
        SELECT
            row_id,
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
                    ORDER BY row_id
                ) AS _rn
            FROM stage.long_selected
            {where_sql}
        )
        WHERE _rn = 1
        """
    )

    analysis_source_table = "core.analysis_long_base"
    if args.lane_split:
        dimension_expr = build_dimension_expr(runtime.dim_sources, runtime.dim_prefixes)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE core.scalar_long_raw AS
            SELECT * FROM core.analysis_long_base
            WHERE NOT ({dimension_expr})
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TABLE core.dim_long_raw AS
            SELECT * FROM core.analysis_long_base
            WHERE {dimension_expr}
            """
        )
        if args.scalar_long_out:
            copy_query_to_parquet(
                con,
                "SELECT UNITID, year, varname, value, varnumber, source_file FROM core.scalar_long_raw ORDER BY year, row_id",
                args.scalar_long_out,
            )
            print(f"[info] wrote scalar long lane: {args.scalar_long_out}")
        if args.dim_long_out:
            copy_query_to_parquet(
                con,
                "SELECT UNITID, year, varname, value, varnumber, source_file FROM core.dim_long_raw ORDER BY year, row_id",
                args.dim_long_out,
            )
            print(f"[info] wrote dimensioned long lane: {args.dim_long_out}")

        con.execute(
            """
            CREATE OR REPLACE TABLE core.scalar_conflict_keys AS
            SELECT
                UNITID,
                year,
                varnumber,
                source_file,
                COUNT(DISTINCT value_norm) AS distinct_values
            FROM core.scalar_long_raw
            GROUP BY 1, 2, 3, 4
            HAVING COUNT(DISTINCT value_norm) > 1
            """
        )
        conflict_key_count = int(con.execute("SELECT COUNT(*) FROM core.scalar_conflict_keys").fetchone()[0])
        if conflict_key_count:
            con.execute(
                """
                CREATE OR REPLACE TABLE qa.scalar_conflicts AS
                SELECT
                    s.row_id,
                    s.UNITID,
                    s.year,
                    s.varname,
                    s.value,
                    s.value_norm,
                    s.varnumber,
                    s.source_file,
                    k.distinct_values
                FROM core.scalar_long_raw s
                INNER JOIN core.scalar_conflict_keys k
                  ON s.UNITID = k.UNITID
                 AND s.year = k.year
                 AND s.varnumber = k.varnumber
                 AND s.source_file = k.source_file
                ORDER BY s.year, s.row_id
                """
            )
            if runtime.scalar_conflicts_out:
                write_query_csv(
                    con,
                    f"""
                    SELECT
                        UNITID,
                        year,
                        varname,
                        value,
                        varnumber,
                        source_file,
                        distinct_values
                    FROM qa.scalar_conflicts
                    ORDER BY year, UNITID, varnumber, source_file, row_id
                    LIMIT {int(args.scalar_conflicts_max_rows)}
                    """,
                    runtime.scalar_conflicts_out,
                )
                print(f"[info] wrote scalar conflict QC: {runtime.scalar_conflicts_out}")
            if args.fail_on_scalar_conflicts:
                raise SystemExit(f"scalar conflict gate failed: conflict_keys={conflict_key_count}")
        else:
            create_empty_conflicts(con)

        con.execute(
            """
            CREATE OR REPLACE TABLE core.scalar_long_unique AS
            WITH marked AS (
                SELECT
                    s.*,
                    CASE WHEN k.UNITID IS NULL THEN FALSE ELSE TRUE END AS is_conflict,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.UNITID, s.year, s.varnumber, s.source_file, s.value_norm
                        ORDER BY s.row_id
                    ) AS _rn
                FROM core.scalar_long_raw s
                LEFT JOIN core.scalar_conflict_keys k
                  ON s.UNITID = k.UNITID
                 AND s.year = k.year
                 AND s.varnumber = k.varnumber
                 AND s.source_file = k.source_file
            )
            SELECT row_id, UNITID, year, varname, value, value_norm, source_file, varnumber
            FROM marked
            WHERE _rn = 1
              AND NOT is_conflict
            """
        )
        analysis_source_table = "core.scalar_long_unique"
    else:
        create_empty_conflicts(con)

    target_df = con.execute(
        f"""
        SELECT
            varname,
            COUNT(*) FILTER (WHERE value_norm IS NOT NULL) AS non_empty_rows
        FROM {analysis_source_table}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    targets = target_df["varname"].tolist()
    targets_with_data = set(target_df.loc[target_df["non_empty_rows"] > 0, "varname"].tolist())
    all_targets = order_targets(targets)

    if args.drop_empty_cols:
        before = len(all_targets)
        all_targets = [t for t in all_targets if t in targets_with_data]
        dropped = before - len(all_targets)
        if dropped > 0:
            print(f"[info] dropped {dropped} globally-empty variables (no non-empty values in selected years)")

    numeric_targets = set()
    if args.typed_output:
        numeric_targets = build_numeric_targets(args.dictionary, all_targets)
        print(f"[info] typed output enabled: numeric vars={len(numeric_targets)} string vars={len(all_targets) - len(numeric_targets)}")

    var_to_group, group_to_vars = ({}, {})
    if args.collapse_disc:
        var_to_group, group_to_vars = build_disc_groups(args.dictionary)
        if args.disc_exclude:
            excludes = {x.strip().upper() for x in args.disc_exclude.split(",") if x.strip()}
            if excludes:
                group_to_vars = {k: v for k, v in group_to_vars.items() if k.upper() not in excludes}
                var_to_group = {v: grp for v, grp in var_to_group.items() if grp[0].upper() not in excludes}
    disc_name_map = {}
    if args.collapse_disc and group_to_vars:
        disc_name_map = resolve_disc_names(group_to_vars, set(all_targets), suffix=args.disc_suffix)
        for base, new_name in disc_name_map.items():
            if new_name not in all_targets:
                all_targets.append(new_name)
        if args.drop_disc_components:
            components = {v for vs in group_to_vars.values() for v in vs}
            all_targets = [t for t in all_targets if t not in components]

    anti_hits = find_anti_garbage_hits(all_targets, runtime.anti_garbage_ids)
    anti_df = pd.DataFrame({"blocked_identifier_column": anti_hits})
    register_df_as_table(con, "qa.anti_garbage_hits", anti_df)
    if anti_hits and runtime.anti_garbage_out:
        anti_df.to_csv(runtime.anti_garbage_out, index=False)
        print(f"[warn] anti-garbage hits written: {runtime.anti_garbage_out} (count={len(anti_hits)})")
    if anti_hits and args.drop_anti_garbage_cols:
        all_targets = [t for t in all_targets if t not in set(anti_hits)]
        print(f"[info] dropped {len(anti_hits)} anti-garbage identifier columns from wide targets")
        anti_hits = find_anti_garbage_hits(all_targets, runtime.anti_garbage_ids)
        anti_df = pd.DataFrame({"blocked_identifier_column": anti_hits})
        register_df_as_table(con, "qa.anti_garbage_hits", anti_df)
    if anti_hits and args.fail_on_anti_garbage:
        raise SystemExit(f"anti-garbage gate failed: {len(anti_hits)} blocked dimension identifiers present in wide targets")

    print(f"[info] years: {years[0]}–{years[-1]} ({len(years)} total)")
    print(f"[info] wide columns (varname): {len(all_targets)}")

    if args.collapse_disc and group_to_vars:
        disc_rows = []
        for varname, (base, suffix) in var_to_group.items():
            disc_rows.append({"varname": varname, "base": base, "suffix": suffix, "output_varname": disc_name_map.get(base, base)})
        disc_map_df = pd.DataFrame(disc_rows)
        register_df_as_table(con, "stage.disc_map", disc_map_df)
        register_df_as_table(con, "stage.disc_output_names", disc_map_df[["base", "output_varname"]].drop_duplicates())
        con.execute(
            f"""
            CREATE OR REPLACE TABLE core.disc_active AS
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
                FROM {analysis_source_table} a
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
            CREATE OR REPLACE TABLE core.disc_choice AS
            SELECT
                UNITID,
                year,
                base,
                COUNT(DISTINCT suffix) AS n_active,
                MIN(suffix) AS chosen_suffix
            FROM core.disc_active
            GROUP BY 1, 2, 3
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE qa.disc_conflicts AS
            SELECT
                a.row_id,
                a.UNITID,
                a.year,
                a.varname,
                a.value,
                a.value_norm,
                a.source_file,
                a.varnumber,
                a.base,
                a.suffix,
                a.is_active,
                c.n_active
            FROM core.disc_active a
            INNER JOIN core.disc_choice c
                ON a.UNITID = c.UNITID
               AND a.year = c.year
               AND a.base = c.base
            WHERE c.n_active > 1
            ORDER BY a.year, a.row_id
            """
        )
        if args.disc_qc_dir:
            for year in years:
                cnt = int(con.execute(f"SELECT COUNT(*) FROM qa.disc_conflicts WHERE year = {int(year)}").fetchone()[0])
                if cnt > 0:
                    write_query_csv(
                        con,
                        f"SELECT UNITID, year, varname, value, source_file, varnumber, base, suffix, is_active, n_active FROM qa.disc_conflicts WHERE year = {int(year)} ORDER BY row_id",
                        os.path.join(args.disc_qc_dir, f"disc_conflicts_{year}.csv"),
                    )
        offset = int(con.execute(f"SELECT COALESCE(MAX(row_id), 0) FROM {analysis_source_table}").fetchone()[0])
        con.execute(
            f"""
            CREATE OR REPLACE TABLE core.disc_collapsed AS
            SELECT
                {offset} + ROW_NUMBER() OVER (ORDER BY c.year, c.UNITID, c.base) AS row_id,
                c.UNITID,
                c.year,
                n.output_varname AS varname,
                c.chosen_suffix AS value,
                c.chosen_suffix AS value_norm,
                '' AS source_file,
                '' AS varnumber
            FROM core.disc_choice c
            INNER JOIN stage.disc_output_names n
                ON c.base = n.base
            WHERE c.n_active = 1
            """
        )
        component_filter = ""
        if args.drop_disc_components:
            component_vars = {v for vs in group_to_vars.values() for v in vs}
            if component_vars:
                component_filter = f"WHERE varname NOT IN ({sql_upper_in(component_vars)})"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE core.analysis_long_pre_dedup AS
            SELECT * FROM {analysis_source_table}
            {component_filter}
            UNION ALL
            SELECT * FROM core.disc_collapsed
            """
        )
    else:
        create_empty_disc_conflicts(con)
        con.execute(f"CREATE OR REPLACE TABLE core.analysis_long_pre_dedup AS SELECT * FROM {analysis_source_table}")

    con.execute(
        """
        CREATE OR REPLACE TABLE qa.dup_groups AS
        SELECT
            year,
            UNITID,
            varname,
            COUNT(*) AS dup_rows
        FROM core.analysis_long_pre_dedup
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
        """
    )
    if args.dups_qc_dir and args.dups_max_rows > 0:
        Path(args.dups_qc_dir).mkdir(parents=True, exist_ok=True)
        for year in years:
            dup_count = int(con.execute(f"SELECT COALESCE(SUM(dup_rows), 0) FROM qa.dup_groups WHERE year = {int(year)}").fetchone()[0])
            if dup_count == 0:
                continue
            dup_df = con.execute(
                f"""
                SELECT p.UNITID, p.year, p.varname, p.value, p.varnumber, p.source_file
                FROM core.analysis_long_pre_dedup p
                INNER JOIN qa.dup_groups g
                  ON p.year = g.year
                 AND p.UNITID = g.UNITID
                 AND p.varname = g.varname
                WHERE p.year = {int(year)}
                ORDER BY p.row_id
                LIMIT {int(args.dups_max_rows)}
                """
            ).fetchdf()
            ext = ".csv.gz" if args.dups_qc_gzip else ".csv"
            compression = "gzip" if args.dups_qc_gzip else None
            dup_df.to_csv(os.path.join(args.dups_qc_dir, f"dups_{year}{ext}"), index=False, compression=compression)

    con.execute(
        """
        CREATE OR REPLACE TABLE core.analysis_long_final AS
        SELECT row_id, UNITID, year, varname, value, value_norm, source_file, varnumber
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY UNITID, year, varname
                    ORDER BY row_id
                ) AS _rn
            FROM core.analysis_long_pre_dedup
        )
        WHERE _rn = 1
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE qa.wide_year_summary AS
        WITH spine_counts AS (
            SELECT year, COUNT(*) AS rows
            FROM stage.spine
            GROUP BY year
        ),
        non_empty AS (
            SELECT year, COUNT(*) AS non_empty_values
            FROM core.analysis_long_final
            WHERE value_norm IS NOT NULL
            GROUP BY year
        ),
        dup_rows AS (
            SELECT year, COALESCE(SUM(dup_rows), 0) AS dup_rows
            FROM qa.dup_groups
            GROUP BY year
        )
        SELECT
            s.year,
            s.rows,
            {len(all_targets)} AS vars,
            COALESCE(n.non_empty_values, 0) AS non_empty_values,
            CASE
                WHEN s.rows > 0 AND {len(all_targets)} > 0
                    THEN COALESCE(n.non_empty_values, 0)::DOUBLE / (s.rows * {len(all_targets)})
                ELSE 0.0
            END AS fill_rate,
            COALESCE(d.dup_rows, 0) AS dup_rows
        FROM spine_counts s
        LEFT JOIN non_empty n USING (year)
        LEFT JOIN dup_rows d USING (year)
        ORDER BY s.year
        """
    )

    con.execute(f"CREATE OR REPLACE TABLE mart.panel_wide_raw AS {build_wide_query(all_targets, 'core.analysis_long_final')}")
    if args.typed_output:
        cast_query = build_cast_report_query([t for t in all_targets if t in numeric_targets])
        if cast_query:
            con.execute(f"CREATE OR REPLACE TABLE qa.cast_report AS {cast_query}")
        else:
            create_empty_cast_report(con)
        con.execute(f"CREATE OR REPLACE TABLE mart.panel_wide AS {build_typed_wide_query(all_targets, numeric_targets)}")
    else:
        create_empty_cast_report(con)
        con.execute("CREATE OR REPLACE TABLE mart.panel_wide AS SELECT * FROM mart.panel_wide_raw ORDER BY year, UNITID")

    if runtime.cast_report_out and args.typed_output and con.execute("SELECT COUNT(*) FROM qa.cast_report").fetchone()[0]:
        write_query_csv(con, 'SELECT * FROM qa.cast_report ORDER BY year, "column"', runtime.cast_report_out)
        print(f"[info] wrote cast report QC: {runtime.cast_report_out}")

    year_part_paths: list[str] = []
    for year in years:
        out_path = os.path.join(args.out_dir, f"year={year}", "part.parquet")
        copy_query_to_parquet(con, f"SELECT * FROM mart.panel_wide WHERE year = {int(year)} ORDER BY UNITID", out_path)
        year_part_paths.append(out_path)
        print(f"[info] wrote {out_path}")

    if args.write_single:
        drop_post_cols: set[str] = set()
        if args.drop_globally_null_post and all_targets:
            count_query = build_non_null_count_query(all_targets)
            if count_query:
                row = con.execute(count_query).fetchone()
                drop_post_cols = {target for target, count in zip(all_targets, row) if int(count or 0) == 0}
            if drop_post_cols and args.qc_dir:
                qc_globally_null = Path(args.qc_dir) / "qc_globally_null_columns_dropped.csv"
                pd.DataFrame({"column": sorted(drop_post_cols)}).to_csv(qc_globally_null, index=False)
                print(f"[info] wrote globally-null drop QC: {qc_globally_null}")
        keep_cols = ["year", "UNITID"] + [t for t in all_targets if t not in drop_post_cols]
        select_cols = ", ".join(quote_ident(c) for c in keep_cols)
        copy_query_to_parquet(con, f"SELECT {select_cols} FROM mart.panel_wide ORDER BY year, UNITID", args.write_single)
        if drop_post_cols:
            print(f"[info] dropping {len(drop_post_cols)} globally-null columns in stitched output")

    if args.qc_dir:
        write_query_csv(con, "SELECT * FROM qa.wide_year_summary ORDER BY year", os.path.join(args.qc_dir, "wide_panel_qc_summary.csv"))
