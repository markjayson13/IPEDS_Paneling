#!/usr/bin/env python3
"""
Legacy pandas/PyArrow implementation of the wide-panel builder.

This path is retained for parity testing against the DuckDB execution engine
and should not be the default production build path going forward.
"""
from __future__ import annotations

import os
import pathlib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from wide_build_common import (
    WideBuildRuntime,
    build_disc_groups,
    build_numeric_targets,
    coerce_types,
    ensure_all_target_cols,
    find_anti_garbage_hits,
    is_dimensioned_source_file,
    is_non_empty_value,
    load_legacy_schema_seed_manifest,
    normalize_value_tokens,
    order_targets,
    plan_legacy_schema_seeds,
    prepare_runtime,
    resolve_disc_names,
)


def active_disc_mask(series: pd.Series) -> pd.Series:
    txt = series.astype("string").str.strip()
    low = txt.str.lower()
    null_like = {"", ".", "nan", "none", "<na>", "na", "nat"}
    true_like = {"y", "yes", "t", "true"}
    false_like = {"n", "no", "f", "false"}

    non_empty = series.notna() & ~low.isin(null_like)
    nums = pd.to_numeric(txt, errors="coerce")
    is_num = nums.notna()
    active_num = is_num & (nums != 0)
    active_true = low.isin(true_like)
    inactive_false = low.isin(false_like)
    active_other = non_empty & ~is_num & ~inactive_false
    return active_num | active_true | active_other


def run(args) -> None:
    runtime: WideBuildRuntime = prepare_runtime(args)

    years = runtime.years
    if max(years) >= 2024:
        print("[warn] 2024 is treated as provisional/schema-transition; prefer 2004:2023 for analysis releases.")

    var_to_group, group_to_vars = ({}, {})
    if args.collapse_disc:
        var_to_group, group_to_vars = build_disc_groups(args.dictionary)
        if args.disc_exclude:
            excludes = {x.strip().upper() for x in args.disc_exclude.split(",") if x.strip()}
            if excludes:
                group_to_vars = {k: v for k, v in group_to_vars.items() if k.upper() not in excludes}
                var_to_group = {v: grp for v, grp in var_to_group.items() if grp[0].upper() not in excludes}

    dataset = ds.dataset(args.input, format="parquet")
    schema = dataset.schema

    from wide_build_common import pick_col, pick_optional_col

    unitid_col = pick_col(schema, ["UNITID", "unitid"])
    year_col = pick_col(schema, ["year", "academicyear"])
    target_col = pick_col(schema, ["varname", "target_var", "concept", "target"])
    value_col = pick_col(schema, ["value", "val"])
    source_col = pick_optional_col(schema, ["source_file", "source"])
    varnumber_col = pick_optional_col(schema, ["varnumber", "var_num", "number"])
    if args.lane_split and (source_col is None or varnumber_col is None):
        raise SystemExit("lane-split requires source_file and varnumber columns in long input.")

    def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            unitid_col: "UNITID",
            year_col: "year",
            target_col: "varname",
            value_col: "value",
        }
        if source_col:
            mapping[source_col] = "source_file"
        if varnumber_col:
            mapping[varnumber_col] = "varnumber"
        return df.rename(columns=mapping)

    targets = set()
    targets_with_data = set()
    scan_cols = [target_col, value_col]
    if args.lane_split and source_col:
        scan_cols.append(source_col)
    for y in years:
        filt = (ds.field(year_col) == y) & ds.field(target_col).is_valid()
        scanner = dataset.scanner(columns=scan_cols, filter=filt, batch_size=args.scan_batch_rows)
        for batch in scanner.to_batches():
            df = rename_cols(pa.Table.from_batches([batch]).to_pandas())
            if df.empty:
                continue
            df["varname"] = df["varname"].fillna("").astype(str).str.upper().str.strip()
            df = df[df["varname"] != ""]
            if df.empty:
                continue
            df["value"] = normalize_value_tokens(df["value"])
            if runtime.exclude_vars:
                df = df[~df["varname"].isin(runtime.exclude_vars)]
                if df.empty:
                    continue
            if args.lane_split:
                dim_mask = df["source_file"].map(lambda s: is_dimensioned_source_file(s, runtime.dim_sources, runtime.dim_prefixes))
                df = df[~dim_mask]
                if df.empty:
                    continue
            targets.update(df["varname"].unique().tolist())
            non_empty = is_non_empty_value(df["value"])
            if non_empty.any():
                targets_with_data.update(df.loc[non_empty, "varname"].unique().tolist())

    legacy_seed_plan_df = pd.DataFrame(columns=["column_name", "seed_reason", "dtype", "source_contract", "present_in_target_universe", "seeded_for_compatibility"])
    seeded_legacy_df = legacy_seed_plan_df.copy()
    legacy_seed_columns: set[str] = set()
    if args.lane_split and runtime.legacy_analysis_schema:
        legacy_manifest_df = load_legacy_schema_seed_manifest(runtime.legacy_schema_seed_manifest)
        legacy_seed_plan_df, seeded_legacy_df = plan_legacy_schema_seeds(legacy_manifest_df, targets)
        legacy_seed_columns = set(legacy_seed_plan_df["column_name"].tolist())
        if not seeded_legacy_df.empty:
            targets.update(seeded_legacy_df["column_name"].tolist())

    all_targets = order_targets(targets)
    if runtime.exclude_vars:
        before = len(all_targets)
        all_targets = [t for t in all_targets if t not in runtime.exclude_vars]
        dropped = before - len(all_targets)
        if dropped > 0:
            print(f"[info] excluded {dropped} variables by --exclude-vars")
    if args.drop_empty_cols:
        before = len(all_targets)
        all_targets = [t for t in all_targets if t in targets_with_data or t in legacy_seed_columns]
        dropped = before - len(all_targets)
        if dropped > 0:
            print(f"[info] dropped {dropped} globally-empty variables (no non-empty values in selected years)")
    if runtime.seeded_legacy_out:
        Path(runtime.seeded_legacy_out).parent.mkdir(parents=True, exist_ok=True)
        seeded_legacy_df.to_csv(runtime.seeded_legacy_out, index=False)
    if not seeded_legacy_df.empty:
        print(f"[info] seeded {len(seeded_legacy_df)} legacy compatibility columns into wide targets")

    numeric_targets = set()
    if args.typed_output:
        numeric_targets = build_numeric_targets(args.dictionary, all_targets)
        print(f"[info] typed output enabled: numeric vars={len(numeric_targets)} string vars={len(all_targets) - len(numeric_targets)}")

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
    if anti_hits and runtime.anti_garbage_out:
        pd.DataFrame({"blocked_identifier_column": anti_hits}).to_csv(runtime.anti_garbage_out, index=False)
        print(f"[warn] anti-garbage hits written: {runtime.anti_garbage_out} (count={len(anti_hits)})")
    if anti_hits and args.drop_anti_garbage_cols:
        all_targets = [t for t in all_targets if t not in set(anti_hits)]
        print(f"[info] dropped {len(anti_hits)} anti-garbage identifier columns from wide targets")
        anti_hits = find_anti_garbage_hits(all_targets, runtime.anti_garbage_ids)
    if anti_hits and args.fail_on_anti_garbage:
        raise SystemExit(f"anti-garbage gate failed: {len(anti_hits)} blocked dimension identifiers present in wide targets")

    schema_fields = [pa.field("year", pa.int32()), pa.field("UNITID", pa.int64())]
    for t in all_targets:
        if args.typed_output and t in numeric_targets:
            schema_fields.append(pa.field(t, pa.float64()))
        else:
            schema_fields.append(pa.field(t, pa.string()))
    schema_wide = pa.schema(schema_fields)
    year_part_paths: list[str] = []
    qc_rows: list[dict] = []
    scalar_conflict_rows: list[pd.DataFrame] = []
    cast_report_rows: list[dict] = []
    scalar_writer = None
    dim_writer = None
    scalar_schema = None
    dim_schema = None

    print(f"[info] years: {years[0]}–{years[-1]} ({len(years)} total)")
    print(f"[info] wide columns (varname): {len(all_targets)}")

    for y in years:
        print(f"[info] building wide for year={y}")
        spine_chunks = []
        spine_scanner = dataset.scanner(columns=[unitid_col, year_col], filter=(ds.field(year_col) == y), batch_size=args.scan_batch_rows)
        for batch in spine_scanner.to_batches():
            sdf = rename_cols(pa.Table.from_batches([batch]).to_pandas())
            if sdf.empty:
                continue
            sdf = sdf.dropna(subset=["UNITID", "year"])
            if sdf.empty:
                continue
            spine_chunks.append(sdf[["year", "UNITID"]])
        if spine_chunks:
            spine = pd.concat(spine_chunks, ignore_index=True).drop_duplicates(subset=["UNITID", "year"])
        else:
            spine = pd.DataFrame(columns=["year", "UNITID"])
        print(f"[info] year={y} spine rows: {len(spine)}")

        concept_cols = [unitid_col, year_col, target_col, value_col]
        if args.lane_split:
            concept_cols.extend([source_col, varnumber_col])
        concept_chunks = []
        concept_scanner = dataset.scanner(
            columns=concept_cols,
            filter=(ds.field(year_col) == y) & ds.field(target_col).is_valid(),
            batch_size=args.scan_batch_rows,
        )
        for batch in concept_scanner.to_batches():
            cdf = rename_cols(pa.Table.from_batches([batch]).to_pandas())
            if cdf.empty:
                continue
            cdf = cdf.dropna(subset=["UNITID", "year", "varname"])
            if cdf.empty:
                continue
            cdf["varname"] = cdf["varname"].astype(str).str.upper().str.strip()
            cdf = cdf[cdf["varname"] != ""]
            if cdf.empty:
                continue
            cdf["value"] = normalize_value_tokens(cdf["value"])
            if runtime.exclude_vars:
                cdf = cdf[~cdf["varname"].isin(runtime.exclude_vars)]
                if cdf.empty:
                    continue
            base_cols = ["UNITID", "year", "varname", "value"]
            if args.lane_split:
                cdf["source_file"] = cdf["source_file"].fillna("").astype(str).str.upper().str.strip()
                cdf["varnumber"] = cdf["varnumber"].fillna("").astype(str).str.strip()
                cdf = cdf.drop_duplicates(subset=["UNITID", "year", "varname", "varnumber", "source_file", "value"], keep="first")
                base_cols.extend(["varnumber", "source_file"])
            else:
                cdf = cdf.drop_duplicates(subset=["UNITID", "year", "varname", "value"], keep="first")
            concept_chunks.append(cdf[base_cols])
        if concept_chunks:
            concept = pd.concat(concept_chunks, ignore_index=True)
        else:
            concept_cols_empty = ["UNITID", "year", "varname", "value"]
            if args.lane_split:
                concept_cols_empty.extend(["varnumber", "source_file"])
            concept = pd.DataFrame(columns=concept_cols_empty)
        print(f"[info] year={y} concept rows: {len(concept)}")

        analysis_concept = concept
        if args.lane_split:
            dim_mask = analysis_concept["source_file"].map(lambda s: is_dimensioned_source_file(s, runtime.dim_sources, runtime.dim_prefixes))
            dim_long = analysis_concept[dim_mask].copy()
            scalar_long = analysis_concept[~dim_mask].copy()

            if args.scalar_long_out:
                t_scalar = pa.Table.from_pandas(scalar_long, preserve_index=False)
                if scalar_writer is None:
                    scalar_schema = t_scalar.schema
                    scalar_writer = pq.ParquetWriter(args.scalar_long_out, scalar_schema, compression="snappy")
                elif scalar_schema is not None:
                    t_scalar = t_scalar.cast(scalar_schema, safe=False)
                scalar_writer.write_table(t_scalar)
            if args.dim_long_out:
                t_dim = pa.Table.from_pandas(dim_long, preserve_index=False)
                if dim_writer is None:
                    dim_schema = t_dim.schema
                    dim_writer = pq.ParquetWriter(args.dim_long_out, dim_schema, compression="snappy")
                elif dim_schema is not None:
                    t_dim = t_dim.cast(dim_schema, safe=False)
                dim_writer.write_table(t_dim)

            key_cols = ["UNITID", "year", "varnumber", "source_file"]
            if not scalar_long.empty:
                s_tmp = scalar_long.copy()
                s_tmp["value_norm"] = normalize_value_tokens(s_tmp["value"])
                agg = s_tmp.groupby(key_cols, dropna=False).agg(n=("value_norm", "size"), dv=("value_norm", "nunique")).reset_index()
                conflicts = agg[agg["dv"] > 1].copy()
                if not conflicts.empty:
                    conflict_rows = s_tmp.merge(conflicts[key_cols], on=key_cols, how="inner")
                    conflict_rows = conflict_rows.drop(columns=["value_norm"])
                    conflict_rows["year"] = y
                    scalar_conflict_rows.append(conflict_rows.head(args.scalar_conflicts_max_rows))
                    scalar_long = scalar_long.merge(conflicts[key_cols].assign(_conflict=1), on=key_cols, how="left")
                    scalar_long = scalar_long[scalar_long["_conflict"].isna()].drop(columns=["_conflict"])
                scalar_long = scalar_long.drop_duplicates(subset=key_cols + ["value"], keep="first")

                if not conflicts.empty and args.fail_on_scalar_conflicts:
                    if runtime.scalar_conflicts_out:
                        out_df = pd.concat(scalar_conflict_rows, ignore_index=True) if scalar_conflict_rows else pd.DataFrame()
                        out_df.to_csv(runtime.scalar_conflicts_out, index=False)
                    raise SystemExit(f"scalar conflict gate failed for year={y}: conflict_keys={len(conflicts)}")

            analysis_concept = scalar_long

        if args.collapse_disc and var_to_group:
            disc_rows = analysis_concept[analysis_concept["varname"].isin(var_to_group)]
            if not disc_rows.empty:
                disc_rows = disc_rows.copy()
                disc_rows["base"] = disc_rows["varname"].map(lambda v: var_to_group.get(v, ("", ""))[0])
                disc_rows["suffix"] = disc_rows["varname"].map(lambda v: var_to_group.get(v, ("", ""))[1])
                disc_rows = disc_rows[is_non_empty_value(disc_rows["value"])]
                if not disc_rows.empty:
                    disc_rows["is_active"] = active_disc_mask(disc_rows["value"])
                    active = disc_rows[disc_rows["is_active"]].copy()
                    if not active.empty:
                        active = active.drop_duplicates(subset=["UNITID", "year", "base", "suffix"])
                        choice = active.groupby(["UNITID", "year", "base"])["suffix"].agg(lambda s: sorted(set(s))).reset_index(name="suffixes")
                        choice["n_active"] = choice["suffixes"].str.len()

                        conflict_keys = choice[choice["n_active"] > 1][["UNITID", "year", "base"]]
                        if not conflict_keys.empty and args.disc_qc_dir:
                            conflict_rows = active.merge(conflict_keys, on=["UNITID", "year", "base"], how="inner")
                            conflict_rows = conflict_rows.merge(choice[["UNITID", "year", "base", "n_active"]], on=["UNITID", "year", "base"], how="left")
                            conflict_rows.to_csv(os.path.join(args.disc_qc_dir, f"disc_conflicts_{y}.csv"), index=False)

                        ok = choice[choice["n_active"] == 1].copy()
                        if not ok.empty:
                            ok["value"] = ok["suffixes"].str[0]
                            ok["varname"] = ok["base"].map(lambda b: disc_name_map.get(b, b))
                            combined = ok[["UNITID", "year", "varname", "value"]]
                            if args.drop_disc_components:
                                components = {v for vs in group_to_vars.values() for v in vs}
                                analysis_concept = analysis_concept[~analysis_concept["varname"].isin(components)]
                            analysis_concept = pd.concat([analysis_concept, combined], ignore_index=True)

        dup_mask = analysis_concept.duplicated(subset=["UNITID", "year", "varname"], keep=False)
        dup_count = int(dup_mask.sum())
        if dup_mask.any() and args.dups_qc_dir and args.dups_max_rows > 0:
            os.makedirs(args.dups_qc_dir, exist_ok=True)
            ext = ".csv.gz" if args.dups_qc_gzip else ".csv"
            dup_path = os.path.join(args.dups_qc_dir, f"dups_{y}{ext}")
            dup_sample = analysis_concept.loc[dup_mask].head(args.dups_max_rows)
            dup_sample.to_csv(dup_path, index=False)
        analysis_concept = analysis_concept.drop_duplicates(subset=["UNITID", "year", "varname"], keep="first")

        if len(analysis_concept) > 0:
            wide = analysis_concept.pivot(index=["year", "UNITID"], columns="varname", values="value").reset_index()
        else:
            wide = spine.copy()

        wide = spine.merge(wide, on=["year", "UNITID"], how="left")
        wide = ensure_all_target_cols(wide, all_targets)
        if args.typed_output and numeric_targets:
            for col in [c for c in wide.columns if c in numeric_targets]:
                s = wide[col]
                non_empty_mask = is_non_empty_value(s)
                total_non_empty = int(non_empty_mask.sum())
                parsed = int(pd.to_numeric(s, errors="coerce").notna().sum())
                cast_report_rows.append(
                    {
                        "year": y,
                        "column": col,
                        "non_empty_tokens": total_non_empty,
                        "parsed_numeric_tokens": parsed,
                        "failed_parse_tokens": int(max(total_non_empty - parsed, 0)),
                    }
                )
        wide = coerce_types(wide, numeric_targets if args.typed_output else None)

        out_path = os.path.join(args.out_dir, f"year={y}", "part.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        tbl = pa.Table.from_pandas(wide, preserve_index=False).cast(schema_wide)
        pq.write_table(tbl, out_path)
        year_part_paths.append(out_path)
        print(f"[info] wrote {out_path}")

        if args.qc_dir:
            n_spine = int(len(spine))
            n_vars = int(len(all_targets))
            non_empty = int(is_non_empty_value(analysis_concept["value"]).sum()) if "value" in analysis_concept.columns else 0
            possible = n_spine * n_vars if n_spine and n_vars else 0
            fill_rate = (non_empty / possible) if possible else 0.0
            qc_rows.append({"year": y, "rows": n_spine, "vars": n_vars, "non_empty_values": non_empty, "fill_rate": fill_rate, "dup_rows": dup_count})

    if scalar_writer is not None:
        scalar_writer.close()
        print(f"[info] wrote scalar long lane: {args.scalar_long_out}")
    if dim_writer is not None:
        dim_writer.close()
        print(f"[info] wrote dimensioned long lane: {args.dim_long_out}")

    if scalar_conflict_rows and runtime.scalar_conflicts_out:
        pathlib.Path(runtime.scalar_conflicts_out).parent.mkdir(parents=True, exist_ok=True)
        pd.concat(scalar_conflict_rows, ignore_index=True).to_csv(runtime.scalar_conflicts_out, index=False)
        print(f"[info] wrote scalar conflict QC: {runtime.scalar_conflicts_out}")

    if cast_report_rows and runtime.cast_report_out:
        pathlib.Path(runtime.cast_report_out).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(cast_report_rows).to_csv(runtime.cast_report_out, index=False)
        print(f"[info] wrote cast report QC: {runtime.cast_report_out}")

    if args.write_single:
        pathlib.Path(args.write_single).parent.mkdir(parents=True, exist_ok=True)
        drop_post_cols: set[str] = set()
        if args.drop_globally_null_post:
            non_null_counts = {name: 0 for name in schema_wide.names if name not in {"year", "UNITID"}}
            for p in year_part_paths:
                pf = pq.ParquetFile(p)
                for batch in pf.iter_batches():
                    for name in non_null_counts:
                        if name not in batch.schema.names:
                            continue
                        arr = batch.column(batch.schema.names.index(name))
                        non_null_counts[name] += int(len(arr) - arr.null_count)
            drop_post_cols = {name for name, cnt in non_null_counts.items() if cnt == 0}
            if drop_post_cols:
                print(f"[info] dropping {len(drop_post_cols)} globally-null columns in stitched output")
                if args.qc_dir:
                    qc_globally_null = pathlib.Path(args.qc_dir) / "qc_globally_null_columns_dropped.csv"
                    pd.DataFrame({"column": sorted(drop_post_cols)}).to_csv(qc_globally_null, index=False)
                    print(f"[info] wrote globally-null drop QC: {qc_globally_null}")
        writer = None
        for p in year_part_paths:
            t = pq.ParquetFile(p).read().cast(schema_wide, safe=False)
            if drop_post_cols:
                keep_cols = [c for c in t.schema.names if c not in drop_post_cols]
                t = t.select(keep_cols)
            if writer is None:
                writer = pq.ParquetWriter(args.write_single, t.schema)
            writer.write_table(t)
        if writer is not None:
            writer.close()

    if args.qc_dir and qc_rows:
        qc_path = os.path.join(args.qc_dir, "wide_panel_qc_summary.csv")
        pd.DataFrame(qc_rows).to_csv(qc_path, index=False)
