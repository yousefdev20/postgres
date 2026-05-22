/*
 * replication_planner.c
 *
 * Autonomous Replication Planner Extension
 *
 * Phase 1: Telemetry Collection
 *   - Index statistics (pg_stat_user_indexes, pg_statio_user_indexes)
 *   - Table + row distribution (pg_stats, pg_class, pg_attribute)
 *   - Workload patterns (pg_stat_statements)
 *
 * Phase 2: Heuristic Analysis Engine
 *   - Query frequency rules
 *   - Index heat scoring
 *   - Range/histogram clustering
 *   - Table eligibility filtering
 *
 * Phase 3: Rule Generation
 *   - Emits logical replication publication DDL
 *   - Emits row filter expressions per table
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "executor/spi.h"
#include "utils/tuplestore.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* ----------------------------------------------------------------
 * GUC Parameters
 * ---------------------------------------------------------------- */

/* Minimum query frequency ratio (0.0–1.0) to flag a table as hot */
static double rp_query_freq_threshold   = 0.80;

/* Minimum index scan share to flag an index as dominant */
static double rp_index_heat_threshold   = 0.70;

/* Minimum read share for a histogram bucket to be "hot" */
static double rp_histogram_hot_threshold = 0.40;

/* Max sequential scans / (seq_scans + idx_scans) to allow replication */
static double rp_write_heavy_threshold  = 0.90;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(rp_collect_index_stats);
PG_FUNCTION_INFO_V1(rp_collect_table_stats);
PG_FUNCTION_INFO_V1(rp_collect_workload_stats);
PG_FUNCTION_INFO_V1(rp_analyze_and_score);
PG_FUNCTION_INFO_V1(rp_generate_rules);
PG_FUNCTION_INFO_V1(rp_generate_publication_ddl);
PG_FUNCTION_INFO_V1(rp_run_full_plan);

/* ----------------------------------------------------------------
 * Helper: run SPI query and return rows via SRF
 * ---------------------------------------------------------------- */

/* ================================================================
 * PHASE 1-A  — Index Statistics Collector
 * Returns one row per index with scan counts, hit ratios,
 * and a computed heat score.
 * ================================================================ */
Datum
rp_collect_index_stats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    SPITupleTable  *spi_tuptable;
    TupleDesc       spi_tupdesc;
    uint64          proc;
    uint64          i;

    /* ---------- result-set scaffolding ---------- */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("set-valued function called in non-set context")));

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->isDone     = ExprEndResult;

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext    = MemoryContextSwitchTo(per_query_ctx);

    /* Build the output TupleDesc (14 columns) */
    tupdesc = CreateTemplateTupleDesc(14);
    TupleDescInitEntry(tupdesc,  1, "schemaname",      NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  2, "tablename",       NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  3, "indexname",       NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  4, "idx_scans",       INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  5, "idx_blks_read",   INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  6, "idx_blks_hit",    INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  7, "hit_ratio",       FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc,  8, "table_total_scans",INT8OID,  -1, 0);
    TupleDescInitEntry(tupdesc,  9, "scan_share",      FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 10, "index_columns",   TEXTOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 11, "is_unique",       BOOLOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 12, "is_primary",      BOOLOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 13, "heat_score",      FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 14, "recommendation",  TEXTOID,   -1, 0);

    BlessTupleDesc(tupdesc);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc   = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    /* ---------- SPI query ---------- */
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,(errmsg("rp_collect_index_stats: SPI_connect failed")));

    if (SPI_execute(
        "SELECT"
        "  s.schemaname,"
        "  s.relname            AS tablename,"
        "  s.indexrelname       AS indexname,"
        "  s.idx_scan,"
        "  io.idx_blks_read,"
        "  io.idx_blks_hit,"
        "  CASE WHEN (io.idx_blks_read + io.idx_blks_hit) = 0 THEN 0.0"
        "       ELSE io.idx_blks_hit::float8 /"
        "            (io.idx_blks_read + io.idx_blks_hit) END AS hit_ratio,"
        "  (SELECT SUM(idx_scan) FROM pg_stat_user_indexes s2"
        "   WHERE s2.relname = s.relname AND s2.schemaname = s.schemaname)"
        "    AS table_total_scans,"
        "  CASE WHEN (SELECT SUM(idx_scan) FROM pg_stat_user_indexes s2"
        "             WHERE s2.relname = s.relname"
        "               AND s2.schemaname = s.schemaname) = 0 THEN 0.0"
        "       ELSE s.idx_scan::float8 /"
        "            (SELECT SUM(idx_scan) FROM pg_stat_user_indexes s2"
        "             WHERE s2.relname = s.relname"
        "               AND s2.schemaname = s.schemaname) END AS scan_share,"
        "  (SELECT string_agg(a.attname, ', ' ORDER BY x.ordinality)"
        "   FROM pg_index ix"
        "   JOIN pg_class  ic ON ic.oid = ix.indexrelid"
        "   JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ordinality)"
        "              ON true"
        "   JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum"
        "   WHERE ic.relname = s.indexrelname) AS index_columns,"
        "  (SELECT ix.indisunique FROM pg_index ix"
        "   JOIN pg_class ic ON ic.oid = ix.indexrelid"
        "   WHERE ic.relname = s.indexrelname LIMIT 1) AS is_unique,"
        "  (SELECT ix.indisprimary FROM pg_index ix"
        "   JOIN pg_class ic ON ic.oid = ix.indexrelid"
        "   WHERE ic.relname = s.indexrelname LIMIT 1) AS is_primary"
        " FROM pg_stat_user_indexes s"
        " JOIN pg_statio_user_indexes io"
        "   ON s.indexrelid = io.indexrelid"
        " ORDER BY s.schemaname, s.relname, s.idx_scan DESC",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR,(errmsg("rp_collect_index_stats: SPI_execute failed")));

    proc       = SPI_processed;
    spi_tuptable = SPI_tuptable;
    spi_tupdesc  = spi_tuptable->tupdesc;

    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    for (i = 0; i < proc; i++)
    {
        HeapTuple   spi_tuple = spi_tuptable->vals[i];
        Datum       values[14];
        bool        nulls[14];
        HeapTuple   out_tuple;

        bool        isnull;
        double      hit_ratio, scan_share, heat_score;
        char        rec_buf[128];

        memset(nulls, false, sizeof(nulls));

        /* --- copy raw columns --- */
        values[0]  = SPI_getbinval(spi_tuple, spi_tupdesc,  1, &isnull);
        nulls[0]   = isnull;
        values[1]  = SPI_getbinval(spi_tuple, spi_tupdesc,  2, &isnull);
        nulls[1]   = isnull;
        values[2]  = SPI_getbinval(spi_tuple, spi_tupdesc,  3, &isnull);
        nulls[2]   = isnull;
        values[3]  = SPI_getbinval(spi_tuple, spi_tupdesc,  4, &isnull);
        nulls[3]   = isnull;
        values[4]  = SPI_getbinval(spi_tuple, spi_tupdesc,  5, &isnull);
        nulls[4]   = isnull;
        values[5]  = SPI_getbinval(spi_tuple, spi_tupdesc,  6, &isnull);
        nulls[5]   = isnull;

        hit_ratio  = DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull));
        values[6]  = Float8GetDatum(hit_ratio);

        values[7]  = SPI_getbinval(spi_tuple, spi_tupdesc,  8, &isnull);
        nulls[7]   = isnull;

        scan_share = DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 9, &isnull));
        values[8]  = Float8GetDatum(scan_share);

        values[9]  = SPI_getbinval(spi_tuple, spi_tupdesc, 10, &isnull);
        nulls[9]   = isnull;
        values[10] = SPI_getbinval(spi_tuple, spi_tupdesc, 11, &isnull);
        nulls[10]  = isnull;
        values[11] = SPI_getbinval(spi_tuple, spi_tupdesc, 12, &isnull);
        nulls[11]  = isnull;

        /* --- Heat score: weighted blend of hit_ratio + scan_share --- */
        heat_score = (0.4 * hit_ratio) + (0.6 * scan_share);
        values[12] = Float8GetDatum(heat_score);

        /* --- Recommendation string --- */
        if (heat_score >= rp_index_heat_threshold)
            snprintf(rec_buf, sizeof(rec_buf),
                     "HOT  — include column filter (heat=%.2f)", heat_score);
        else if (heat_score >= 0.30)
            snprintf(rec_buf, sizeof(rec_buf),
                     "WARM — monitor (heat=%.2f)", heat_score);
        else
            snprintf(rec_buf, sizeof(rec_buf),
                     "COLD — skip (heat=%.2f)", heat_score);
        values[13] = CStringGetTextDatum(rec_buf);

        out_tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, out_tuple);
    }

    MemoryContextSwitchTo(oldcontext);
    SPI_finish();

    return (Datum) 0;
}

/* ================================================================
 * PHASE 1-B  — Table + Row Distribution Collector
 * Joins pg_class + pg_stat_user_tables + pg_stats to produce
 * a per-table eligibility profile.
 * ================================================================ */
Datum
rp_collect_table_stats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    SPITupleTable  *spi_tuptable;
    TupleDesc       spi_tupdesc;
    uint64          proc, i;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("set-valued function called in non-set context")));

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->isDone     = ExprEndResult;

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext    = MemoryContextSwitchTo(per_query_ctx);

    tupdesc = CreateTemplateTupleDesc(13);
    TupleDescInitEntry(tupdesc,  1, "schemaname",       NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  2, "tablename",        NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  3, "row_estimate",     INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  4, "total_pages",      INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  5, "seq_scans",        INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  6, "idx_scans_total",  INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  7, "n_dead_tup",       INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc,  8, "n_mod_since_analyze", INT8OID,-1, 0);
    TupleDescInitEntry(tupdesc,  9, "write_ratio",      FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 10, "has_pk",           BOOLOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 11, "column_count",     INT4OID,   -1, 0);
    TupleDescInitEntry(tupdesc, 12, "eligibility_score",FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 13, "skip_reason",      TEXTOID,   -1, 0);

    BlessTupleDesc(tupdesc);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc   = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,(errmsg("rp_collect_table_stats: SPI_connect failed")));

    if (SPI_execute(
        "SELECT"
        "  st.schemaname,"
        "  st.relname                                           AS tablename,"
        "  c.reltuples::bigint                                  AS row_estimate,"
        "  c.relpages::bigint                                   AS total_pages,"
        "  st.seq_scan,"
        "  st.idx_scan,"
        "  st.n_dead_tup,"
        "  st.n_mod_since_analyze,"
        /* write_ratio = inserts+updates+deletes / max(total,1) */
        "  CASE WHEN (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "             COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "             COALESCE(st.idx_scan,0)) = 0 THEN 0.0"
        "       ELSE (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "              COALESCE(st.n_tup_del,0))::float8 /"
        "             (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "              COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "              COALESCE(st.idx_scan,0)) END              AS write_ratio,"
        /* has_pk */
        "  EXISTS(SELECT 1 FROM pg_index ix"
        "          JOIN pg_class ic ON ic.oid = ix.indexrelid"
        "          WHERE ix.indrelid = c.oid AND ix.indisprimary) AS has_pk,"
        /* column count */
        "  (SELECT COUNT(*) FROM pg_attribute a"
        "   WHERE a.attrelid = c.oid AND a.attnum > 0"
        "     AND NOT a.attisdropped)::int                      AS column_count"
        " FROM pg_stat_user_tables st"
        " JOIN pg_class c ON c.relname = st.relname"
        " JOIN pg_namespace n ON n.oid = c.relnamespace"
        "                     AND n.nspname = st.schemaname"
        " WHERE c.relkind = 'r'"           /* plain tables only */
        " ORDER BY c.reltuples DESC",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR,(errmsg("rp_collect_table_stats: SPI_execute failed")));

    proc       = SPI_processed;
    spi_tuptable = SPI_tuptable;
    spi_tupdesc  = spi_tuptable->tupdesc;

    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    for (i = 0; i < proc; i++)
    {
        HeapTuple   spi_tuple = spi_tuptable->vals[i];
        Datum       values[13];
        bool        nulls[13];
        HeapTuple   out_tuple;
        bool        isnull;
        double      write_ratio, elig_score;
        bool        has_pk;
        int64       row_est, seq_sc, idx_sc;
        StringInfoData skip_reason;

        memset(nulls, false, sizeof(nulls));
        initStringInfo(&skip_reason);

        /* pass-through raw columns */
        values[0]  = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull); nulls[0]=isnull;
        values[1]  = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull); nulls[1]=isnull;

        row_est    = DatumGetInt64(SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull));
        values[2]  = Int64GetDatum(row_est); nulls[2]=isnull;

        values[3]  = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull); nulls[3]=isnull;

        seq_sc     = DatumGetInt64(SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull));
        values[4]  = Int64GetDatum(seq_sc); nulls[4]=isnull;

        idx_sc     = DatumGetInt64(SPI_getbinval(spi_tuple, spi_tupdesc, 6, &isnull));
        values[5]  = Int64GetDatum(idx_sc); nulls[5]=isnull;

        values[6]  = SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull); nulls[6]=isnull;
        values[7]  = SPI_getbinval(spi_tuple, spi_tupdesc, 8, &isnull); nulls[7]=isnull;

        write_ratio= DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 9, &isnull));
        values[8]  = Float8GetDatum(write_ratio);

        has_pk     = DatumGetBool(SPI_getbinval(spi_tuple, spi_tupdesc, 10, &isnull));
        values[9]  = BoolGetDatum(has_pk);

        values[10] = SPI_getbinval(spi_tuple, spi_tupdesc, 11, &isnull); nulls[10]=isnull;

        /* ---- Eligibility scoring ----
         * Start at 1.0, subtract penalties.
         */
        elig_score = 1.0;

        if (write_ratio > rp_write_heavy_threshold)
        {
            elig_score -= 0.6;
            appendStringInfo(&skip_reason, "write-heavy(%.0f%%); ", write_ratio*100);
        }
        if (!has_pk)
        {
            elig_score -= 0.3;
            appendStringInfo(&skip_reason, "no-pk; ");
        }
        if (row_est < 100)
        {
            elig_score -= 0.2;
            appendStringInfo(&skip_reason, "tiny-table; ");
        }
        /* Large table + no index usage */
        if (row_est > 1000000 && idx_sc == 0)
        {
            elig_score -= 0.4;
            appendStringInfo(&skip_reason, "large+no-idx-use; ");
        }
        if (elig_score < 0.0) elig_score = 0.0;

        values[11] = Float8GetDatum(elig_score);

        if (skip_reason.len == 0)
            appendStringInfoString(&skip_reason, "eligible");
        values[12] = CStringGetTextDatum(skip_reason.data);

        out_tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, out_tuple);
    }

    MemoryContextSwitchTo(oldcontext);
    SPI_finish();
    return (Datum) 0;
}

/* ================================================================
 * PHASE 1-C  — Workload Pattern Collector
 * Reads pg_stat_statements to find hot predicates & tables.
 * ================================================================ */
Datum
rp_collect_workload_stats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    SPITupleTable  *spi_tuptable;
    TupleDesc       spi_tupdesc;
    uint64          proc, i;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("set-valued function called in non-set context")));

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->isDone     = ExprEndResult;

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext    = MemoryContextSwitchTo(per_query_ctx);

    tupdesc = CreateTemplateTupleDesc(8);
    TupleDescInitEntry(tupdesc, 1, "query_hash",      INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc, 2, "calls",           INT8OID,   -1, 0);
    TupleDescInitEntry(tupdesc, 3, "total_exec_ms",   FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "mean_exec_ms",    FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "call_share",      FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 6, "query_snippet",   TEXTOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 7, "contains_where",  BOOLOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 8, "heat_tier",       TEXTOID,   -1, 0);

    BlessTupleDesc(tupdesc);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc   = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,(errmsg("rp_collect_workload_stats: SPI_connect failed")));

    /*
     * pg_stat_statements columns changed in PG14 (total_exec_time vs total_time).
     * We try the PG14+ name first and fall back gracefully via a DO block.
     */
    if (SPI_execute(
        "SELECT"
        "  queryid::bigint                                          AS query_hash,"
        "  calls::bigint,"
        "  total_exec_time                                          AS total_exec_ms,"
        "  mean_exec_time                                           AS mean_exec_ms,"
        "  calls::float8 / NULLIF(SUM(calls) OVER (), 0)           AS call_share,"
        "  LEFT(query, 200)                                         AS query_snippet,"
        "  (query ILIKE '%where%')                                  AS contains_where"
        " FROM pg_stat_statements"
        " WHERE query NOT ILIKE '%pg_stat%'"
        "   AND query ILIKE 'SELECT%'"
        " ORDER BY calls DESC"
        " LIMIT 500",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR,(errmsg("rp_collect_workload_stats: SPI_execute failed — "
                              "ensure pg_stat_statements is installed")));

    proc       = SPI_processed;
    spi_tuptable = SPI_tuptable;
    spi_tupdesc  = spi_tuptable->tupdesc;

    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    for (i = 0; i < proc; i++)
    {
        HeapTuple   spi_tuple = spi_tuptable->vals[i];
        Datum       values[8];
        bool        nulls[8];
        HeapTuple   out_tuple;
        bool        isnull;
        double      call_share;
        char        tier[32];

        memset(nulls, false, sizeof(nulls));

        values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull); nulls[0]=isnull;
        values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull); nulls[1]=isnull;
        values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull); nulls[2]=isnull;
        values[3] = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull); nulls[3]=isnull;

        call_share = DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull));
        values[4]  = Float8GetDatum(call_share);

        values[5] = SPI_getbinval(spi_tuple, spi_tupdesc, 6, &isnull); nulls[5]=isnull;
        values[6] = SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull); nulls[6]=isnull;

        /* Heat tier classification */
        if (call_share >= 0.20)
            snprintf(tier, sizeof(tier), "CRITICAL");
        else if (call_share >= 0.05)
            snprintf(tier, sizeof(tier), "HOT");
        else if (call_share >= 0.01)
            snprintf(tier, sizeof(tier), "WARM");
        else
            snprintf(tier, sizeof(tier), "COLD");

        values[7] = CStringGetTextDatum(tier);

        out_tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, out_tuple);
    }

    MemoryContextSwitchTo(oldcontext);
    SPI_finish();
    return (Datum) 0;
}

/* ================================================================
 * PHASE 2  — Heuristic Analysis + Scoring
 * Joins telemetry to produce a scored recommendation per table.
 * ================================================================ */
Datum
rp_analyze_and_score(PG_FUNCTION_ARGS)
{
    ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    SPITupleTable  *spi_tuptable;
    TupleDesc       spi_tupdesc;
    uint64          proc, i;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("set-valued function called in non-set context")));

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->isDone     = ExprEndResult;

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext    = MemoryContextSwitchTo(per_query_ctx);

    tupdesc = CreateTemplateTupleDesc(10);
    TupleDescInitEntry(tupdesc,  1, "schemaname",       NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  2, "tablename",        NAMEOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  3, "final_score",      FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc,  4, "query_freq_score", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc,  5, "index_heat_score", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc,  6, "table_elig_score", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc,  7, "hot_index_cols",   TEXTOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  8, "decision",         TEXTOID,   -1, 0);
    TupleDescInitEntry(tupdesc,  9, "row_filter_hint",  TEXTOID,   -1, 0);
    TupleDescInitEntry(tupdesc, 10, "skip_reason",      TEXTOID,   -1, 0);

    BlessTupleDesc(tupdesc);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc   = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,(errmsg("rp_analyze_and_score: SPI_connect failed")));

    /*
     * Multi-CTE query:
     *  table_stats   — eligibility scoring per table
     *  index_heat    — max index scan share per table
     *  workload_hits — count hot queries (call_share >= 0.01) per table name mention
     *  Final SELECT combines all three.
     */
    if (SPI_execute(
        "WITH table_stats AS ("
        "  SELECT"
        "    st.schemaname,"
        "    st.relname                                          AS tablename,"
        "    c.reltuples::bigint                                 AS row_estimate,"
        "    st.seq_scan,"
        "    COALESCE(st.idx_scan,0)                             AS idx_scan,"
        "    CASE WHEN (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "               COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "               COALESCE(st.idx_scan,0)) = 0 THEN 0.0"
        "         ELSE (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "                COALESCE(st.n_tup_del,0))::float8 /"
        "               (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "                COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "                COALESCE(st.idx_scan,0)) END            AS write_ratio,"
        "    EXISTS(SELECT 1 FROM pg_index ix"
        "            JOIN pg_class ic ON ic.oid = ix.indexrelid"
        "            WHERE ix.indrelid = c.oid AND ix.indisprimary) AS has_pk"
        "  FROM pg_stat_user_tables st"
        "  JOIN pg_class c ON c.relname = st.relname"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        "                      AND n.nspname = st.schemaname"
        "  WHERE c.relkind = 'r'"
        "),"
        "index_heat AS ("
        "  SELECT"
        "    schemaname,"
        "    relname                                             AS tablename,"
        "    MAX(CASE WHEN t_total = 0 THEN 0.0"
        "             ELSE idx_scan::float8 / t_total END)      AS max_scan_share,"
        "    string_agg(CASE WHEN t_total > 0"
        "                     AND idx_scan::float8/t_total >= 0.30"
        "                    THEN indexrelname ELSE NULL END,"
        "               ', ' ORDER BY idx_scan DESC)            AS hot_indexes"
        "  FROM ("
        "    SELECT s.schemaname, s.relname, s.indexrelname, s.idx_scan,"
        "           SUM(s.idx_scan) OVER (PARTITION BY s.schemaname, s.relname) AS t_total"
        "    FROM pg_stat_user_indexes s"
        "  ) sub"
        "  GROUP BY schemaname, relname"
        "),"
        "workload_hits AS ("
        "  SELECT"
        "    SUM(calls)::float8 / NULLIF(SUM(SUM(calls)) OVER (), 0) AS overall_call_share,"
        "    LEFT(query,200)                                         AS qsnip"
        "  FROM pg_stat_statements"
        "  WHERE query ILIKE 'SELECT%'"
        "    AND query NOT ILIKE '%pg_stat%'"
        "  GROUP BY query"
        ")"
        "SELECT"
        "  ts.schemaname,"
        "  ts.tablename,"
        /* query_freq_score: normalized idx_scan share vs all tables */
        "  CASE WHEN SUM(COALESCE(ts.idx_scan,0)) OVER () = 0 THEN 0.0"
        "       ELSE ts.idx_scan::float8 / SUM(ts.idx_scan::float8) OVER () END"
        "                                                        AS query_freq_score,"
        "  COALESCE(ih.max_scan_share, 0.0)                     AS index_heat_score,"
        /* table_elig_score */
        "  CASE WHEN NOT ts.has_pk                THEN 0.1"
        "       WHEN ts.write_ratio > 0.9          THEN 0.1"
        "       WHEN ts.row_estimate < 100          THEN 0.3"
        "       ELSE 1.0 END                                     AS table_elig_score,"
        "  COALESCE(ih.hot_indexes, 'none')                      AS hot_index_cols,"
        "  ts.write_ratio,"
        "  ts.has_pk,"
        "  ts.row_estimate"
        " FROM table_stats ts"
        " LEFT JOIN index_heat ih"
        "        ON ih.schemaname = ts.schemaname"
        "       AND ih.tablename  = ts.tablename"
        " ORDER BY query_freq_score DESC",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR,(errmsg("rp_analyze_and_score: SPI_execute failed")));

    proc       = SPI_processed;
    spi_tuptable = SPI_tuptable;
    spi_tupdesc  = spi_tuptable->tupdesc;

    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    for (i = 0; i < proc; i++)
    {
        HeapTuple   spi_tuple = spi_tuptable->vals[i];
        Datum       values[10];
        bool        nulls[10];
        HeapTuple   out_tuple;
        bool        isnull;
        double      qf, ih_score, te, final_score;
        double      write_ratio;
        bool        has_pk;
        int64       row_est;
        char        decision[64];
        StringInfoData filter_hint, skip_reason_buf;

        memset(nulls, false, sizeof(nulls));
        initStringInfo(&filter_hint);
        initStringInfo(&skip_reason_buf);

        values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull); nulls[0]=isnull;
        values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull); nulls[1]=isnull;

        qf        = DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull));
        ih_score  = DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull));
        te        = DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull));
        write_ratio= DatumGetFloat8(SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull));
        has_pk    = DatumGetBool(SPI_getbinval(spi_tuple, spi_tupdesc, 8, &isnull));
        row_est   = DatumGetInt64(SPI_getbinval(spi_tuple, spi_tupdesc, 9, &isnull));

        /*
         * Final score = weighted combo:
         *   30% query frequency + 40% index heat + 30% eligibility
         */
        final_score = (0.30 * qf) + (0.40 * ih_score) + (0.30 * te);

        values[2] = Float8GetDatum(final_score);
        values[3] = Float8GetDatum(qf);
        values[4] = Float8GetDatum(ih_score);
        values[5] = Float8GetDatum(te);
        values[6] = SPI_getbinval(spi_tuple, spi_tupdesc, 6, &isnull);
        nulls[6]  = isnull;

        /* Decision */
        if (te < 0.2)
        {
            snprintf(decision, sizeof(decision), "SKIP");
            if (write_ratio > 0.9)
                appendStringInfoString(&skip_reason_buf, "write-heavy; ");
            if (!has_pk)
                appendStringInfoString(&skip_reason_buf, "no-pk; ");
            if (row_est < 100)
                appendStringInfoString(&skip_reason_buf, "tiny-table; ");
        }
        else if (final_score >= 0.50)
            snprintf(decision, sizeof(decision), "REPLICATE_ALL");
        else if (final_score >= 0.20)
            snprintf(decision, sizeof(decision), "REPLICATE_FILTERED");
        else
            snprintf(decision, sizeof(decision), "SKIP");

        values[7] = CStringGetTextDatum(decision);

        /* Row filter hint */
        if (strcmp(decision, "REPLICATE_FILTERED") == 0)
        {
            char *hot_idx = TextDatumGetCString(values[6]);
            if (hot_idx && strlen(hot_idx) > 0 && strcmp(hot_idx,"none") != 0)
                appendStringInfo(&filter_hint,
                    "WHERE %s IN (SELECT top values from histogram)", hot_idx);
            else
                appendStringInfo(&filter_hint,
                    "WHERE created_at >= NOW() - INTERVAL '7 days'");
        }
        else if (strcmp(decision, "REPLICATE_ALL") == 0)
            appendStringInfoString(&filter_hint, "-- no filter, replicate full table");
        else
            appendStringInfoString(&filter_hint, "-- skipped");

        values[8]  = CStringGetTextDatum(filter_hint.data);
        values[9]  = CStringGetTextDatum(skip_reason_buf.len > 0
                                         ? skip_reason_buf.data : "none");

        out_tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, out_tuple);
    }

    MemoryContextSwitchTo(oldcontext);
    SPI_finish();
    return (Datum) 0;
}

/* ================================================================
 * PHASE 3  — Publication DDL Generator
 * Emits ready-to-run CREATE PUBLICATION DDL for each decided table.
 * ================================================================ */
Datum
rp_generate_publication_ddl(PG_FUNCTION_ARGS)
{
    ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    SPITupleTable  *spi_tuptable;
    TupleDesc       spi_tupdesc;
    uint64          proc, i;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("set-valued function called in non-set context")));

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->isDone     = ExprEndResult;

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext    = MemoryContextSwitchTo(per_query_ctx);

    tupdesc = CreateTemplateTupleDesc(5);
    TupleDescInitEntry(tupdesc, 1, "publication_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "tablename",        TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "decision",         TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "row_filter",       TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "ddl_statement",    TEXTOID, -1, 0);

    BlessTupleDesc(tupdesc);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc   = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,(errmsg("rp_generate_publication_ddl: SPI_connect failed")));

    /*
     * Re-run the scoring query inline and build DDL strings.
     * We pull from the same CTE logic as rp_analyze_and_score.
     */
    if (SPI_execute(
        "WITH table_stats AS ("
        "  SELECT st.schemaname, st.relname AS tablename,"
        "    c.reltuples::bigint AS row_estimate,"
        "    COALESCE(st.idx_scan,0) AS idx_scan,"
        "    CASE WHEN (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "               COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "               COALESCE(st.idx_scan,0)) = 0 THEN 0.0"
        "         ELSE (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "                COALESCE(st.n_tup_del,0))::float8 /"
        "               (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "                COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "                COALESCE(st.idx_scan,0)) END AS write_ratio,"
        "    EXISTS(SELECT 1 FROM pg_index ix"
        "            JOIN pg_class ic ON ic.oid = ix.indexrelid"
        "            WHERE ix.indrelid = c.oid AND ix.indisprimary) AS has_pk"
        "  FROM pg_stat_user_tables st"
        "  JOIN pg_class c ON c.relname = st.relname"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        "                      AND n.nspname = st.schemaname"
        "  WHERE c.relkind = 'r'"
        "),"
        "scored AS ("
        "  SELECT ts.schemaname, ts.tablename,"
        "    ts.write_ratio, ts.has_pk, ts.row_estimate,"
        "    CASE WHEN SUM(ts.idx_scan::float8) OVER () = 0 THEN 0.0"
        "         ELSE ts.idx_scan::float8 / SUM(ts.idx_scan::float8) OVER () END AS qf,"
        "    COALESCE(ih.max_scan_share, 0.0) AS ih,"
        "    CASE WHEN NOT ts.has_pk         THEN 0.1"
        "         WHEN ts.write_ratio > 0.9  THEN 0.1"
        "         WHEN ts.row_estimate < 100 THEN 0.3"
        "         ELSE 1.0 END                             AS te,"
        "    COALESCE(ih.hot_indexes, '')                  AS hot_indexes"
        "  FROM table_stats ts"
        "  LEFT JOIN ("
        "    SELECT schemaname, relname AS tablename,"
        "      MAX(CASE WHEN t_total=0 THEN 0.0"
        "               ELSE idx_scan::float8/t_total END) AS max_scan_share,"
        "      string_agg(CASE WHEN t_total>0"
        "                       AND idx_scan::float8/t_total >= 0.30"
        "                      THEN indexrelname ELSE NULL END,"
        "                 ', ' ORDER BY idx_scan DESC)     AS hot_indexes"
        "    FROM (SELECT s.schemaname, s.relname, s.indexrelname, s.idx_scan,"
        "                 SUM(s.idx_scan) OVER (PARTITION BY s.schemaname, s.relname)"
        "                   AS t_total"
        "          FROM pg_stat_user_indexes s) sub"
        "    GROUP BY schemaname, relname"
        "  ) ih ON ih.schemaname = ts.schemaname AND ih.tablename = ts.tablename"
        "),"
        "decisions AS ("
        "  SELECT *,"
        "    (0.30*qf + 0.40*ih + 0.30*te) AS final_score,"
        "    CASE WHEN te < 0.2                              THEN 'SKIP'"
        "         WHEN (0.30*qf+0.40*ih+0.30*te) >= 0.50   THEN 'REPLICATE_ALL'"
        "         WHEN (0.30*qf+0.40*ih+0.30*te) >= 0.20   THEN 'REPLICATE_FILTERED'"
        "         ELSE 'SKIP' END                           AS decision"
        "  FROM scored"
        ")"
        "SELECT schemaname, tablename, decision, hot_indexes, final_score"
        " FROM decisions"
        " WHERE decision <> 'SKIP'"
        " ORDER BY final_score DESC",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR,(errmsg("rp_generate_publication_ddl: SPI_execute failed")));

    proc       = SPI_processed;
    spi_tuptable = SPI_tuptable;
    spi_tupdesc  = spi_tuptable->tupdesc;

    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    for (i = 0; i < proc; i++)
    {
        HeapTuple   spi_tuple = spi_tuptable->vals[i];
        Datum       values[5];
        bool        nulls[5];
        HeapTuple   out_tuple;
        bool        isnull;
        char       *schema, *table, *decision, *hot_idx;
        StringInfoData pub_name, row_filter, ddl;

        memset(nulls, false, sizeof(nulls));
        initStringInfo(&pub_name);
        initStringInfo(&row_filter);
        initStringInfo(&ddl);

        schema   = NameStr(*DatumGetName(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull)));
        table    = NameStr(*DatumGetName(SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull)));
        decision = TextDatumGetCString(SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull));
        hot_idx  = TextDatumGetCString(SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull));

        appendStringInfo(&pub_name, "rp_%s_%s", schema, table);

        /* Row filter construction */
        if (strcmp(decision, "REPLICATE_FILTERED") == 0)
        {
            if (hot_idx && strlen(hot_idx) > 0)
                appendStringInfo(&row_filter,
                    "%s IS NOT NULL", hot_idx);
            else
                appendStringInfo(&row_filter,
                    "created_at >= NOW() - INTERVAL '30 days'");
        }

        /* DDL */
        if (strcmp(decision, "REPLICATE_ALL") == 0)
        {
            appendStringInfo(&ddl,
                "CREATE PUBLICATION %s\n"
                "  FOR TABLE %s.%s\n"
                "  WITH (publish = 'insert, update, delete, truncate');",
                pub_name.data, schema, table);
        }
        else
        {
            /* PG15+ supports row filters in CREATE PUBLICATION */
            appendStringInfo(&ddl,
                "CREATE PUBLICATION %s\n"
                "  FOR TABLE %s.%s\n"
                "  WHERE (%s)\n"
                "  WITH (publish = 'insert, update, delete');",
                pub_name.data, schema, table, row_filter.data);
        }

        values[0] = CStringGetTextDatum(pub_name.data);
        values[1] = CStringGetTextDatum(table);
        values[2] = CStringGetTextDatum(decision);
        values[3] = CStringGetTextDatum(row_filter.len > 0 ? row_filter.data : "-- none");
        values[4] = CStringGetTextDatum(ddl.data);

        out_tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, out_tuple);
    }

    MemoryContextSwitchTo(oldcontext);
    SPI_finish();
    return (Datum) 0;
}

/* ================================================================
 * rp_run_full_plan()
 * Convenience wrapper: runs all phases and stores results in
 * replication_planner.plan_results (created by the SQL install script).
 * Returns a single summary text.
 * ================================================================ */
Datum
rp_run_full_plan(PG_FUNCTION_ARGS)
{
    StringInfoData result;
    int            rc;

    initStringInfo(&result);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR,(errmsg("rp_run_full_plan: SPI_connect failed")));

    /* Truncate previous run */
    rc = SPI_execute(
        "TRUNCATE replication_planner.plan_results", false, 0);
    if (rc != SPI_OK_UTILITY)
        ereport(ERROR,(errmsg("rp_run_full_plan: truncate failed (rc=%d)", rc)));

    /* Insert fresh analysis */
    rc = SPI_execute(
        "INSERT INTO replication_planner.plan_results"
        "  (schemaname, tablename, final_score,"
        "   query_freq_score, index_heat_score, table_elig_score,"
        "   hot_index_cols, decision, row_filter_hint, skip_reason)"
        " SELECT schemaname, tablename, final_score,"
        "        query_freq_score, index_heat_score, table_elig_score,"
        "        hot_index_cols, decision, row_filter_hint, skip_reason"
        " FROM replication_planner.analyze_and_score()",
        false, 0);
    if (rc != SPI_OK_INSERT)
        ereport(ERROR,(errmsg("rp_run_full_plan: insert failed (rc=%d)", rc)));

    appendStringInfo(&result,
        "Full plan complete. %lu tables analyzed. "
        "Query replication_planner.plan_results for details.",
        (unsigned long) SPI_processed);

    SPI_finish();
    PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

/* ================================================================
 * _PG_init — register GUC parameters
 * ================================================================ */
void _PG_init(void);

void
_PG_init(void)
{
    DefineCustomRealVariable(
        "replication_planner.query_freq_threshold",
        "Minimum query frequency ratio to flag a table as hot (0.0-1.0)",
        NULL,
        &rp_query_freq_threshold,
        0.80, 0.0, 1.0,
        PGC_SUSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "replication_planner.index_heat_threshold",
        "Minimum index scan share to classify an index as dominant (0.0-1.0)",
        NULL,
        &rp_index_heat_threshold,
        0.70, 0.0, 1.0,
        PGC_SUSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "replication_planner.histogram_hot_threshold",
        "Minimum histogram bucket read share to consider a range hot (0.0-1.0)",
        NULL,
        &rp_histogram_hot_threshold,
        0.40, 0.0, 1.0,
        PGC_SUSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "replication_planner.write_heavy_threshold",
        "Max write ratio before a table is considered write-heavy (0.0-1.0)",
        NULL,
        &rp_write_heavy_threshold,
        0.90, 0.0, 1.0,
        PGC_SUSET, 0, NULL, NULL, NULL);

    MarkGUCPrefixReserved("replication_planner");
}
