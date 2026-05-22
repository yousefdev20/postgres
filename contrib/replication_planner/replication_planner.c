/*
 * replication_planner.c
 *
 * Autonomous Replication Planner Extension
 *
 * Phase 1: Telemetry Collection
 *   - Index statistics (pg_stat_user_indexes, pg_statio_user_indexes)
 *   - Table + row distribution (pg_class, pg_stat_user_tables)
 *   - Workload patterns (pg_stat_statements)
 *
 * Phase 2: Heuristic Analysis Engine
 *   - Query frequency rules
 *   - Index heat scoring
 *   - Table eligibility filtering
 *
 * Phase 3: Rule Generation
 *   - Emits logical replication publication DDL
 *   - Emits row filter expressions per table (PG15+)
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
static double rp_index_heat_threshold  = 0.70;
static double rp_write_heavy_threshold = 0.90;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(rp_collect_index_stats);
PG_FUNCTION_INFO_V1(rp_collect_table_stats);
PG_FUNCTION_INFO_V1(rp_collect_workload_stats);
PG_FUNCTION_INFO_V1(rp_analyze_and_score);
PG_FUNCTION_INFO_V1(rp_generate_publication_ddl);
PG_FUNCTION_INFO_V1(rp_run_full_plan);

/* ================================================================
 * PHASE 1-A  — Index Statistics Collector
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
    uint64          proc, i;

    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    tupdesc       = rsinfo->setDesc;
    tupstore      = rsinfo->setResult;
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("rp_collect_index_stats: SPI_connect failed")));

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
        "   WHERE s2.relid = s.relid)                          AS table_total_scans,"
        "  CASE WHEN (SELECT SUM(idx_scan) FROM pg_stat_user_indexes s2"
        "             WHERE s2.relid = s.relid) = 0 THEN 0.0"
        "       ELSE s.idx_scan::float8 /"
        "            (SELECT SUM(idx_scan) FROM pg_stat_user_indexes s2"
        "             WHERE s2.relid = s.relid) END             AS scan_share,"
        "  (SELECT string_agg(a.attname, ', ' ORDER BY x.ord)"
        "   FROM pg_index ix"
        "   JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord)"
        "        ON x.attnum > 0"
        "   JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum"
        "   WHERE ix.indexrelid = s.indexrelid)                 AS index_columns,"
        "  (SELECT ix.indisunique  FROM pg_index ix WHERE ix.indexrelid = s.indexrelid) AS is_unique,"
        "  (SELECT ix.indisprimary FROM pg_index ix WHERE ix.indexrelid = s.indexrelid) AS is_primary"
        " FROM pg_stat_user_indexes s"
        " JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid"
        " ORDER BY s.schemaname, s.relname, s.idx_scan DESC",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("rp_collect_index_stats: SPI_execute failed")));

    proc         = SPI_processed;
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
        double      hit_ratio = 0.0, scan_share = 0.0, heat_score;
        char        rec_buf[128];
        int         j;

        memset(nulls, false, sizeof(nulls));

        for (j = 0; j < 6; j++)
        {
            values[j] = SPI_getbinval(spi_tuple, spi_tupdesc, j + 1, &isnull);
            nulls[j]  = isnull;
        }

        values[6] = SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull);
        nulls[6]  = isnull;
        if (!isnull) hit_ratio = DatumGetFloat8(values[6]);

        values[7] = SPI_getbinval(spi_tuple, spi_tupdesc, 8, &isnull);
        nulls[7]  = isnull;

        values[8] = SPI_getbinval(spi_tuple, spi_tupdesc, 9, &isnull);
        nulls[8]  = isnull;
        if (!isnull) scan_share = DatumGetFloat8(values[8]);

        for (j = 9; j < 12; j++)
        {
            values[j] = SPI_getbinval(spi_tuple, spi_tupdesc, j + 1, &isnull);
            nulls[j]  = isnull;
        }

        heat_score = (0.4 * hit_ratio) + (0.6 * scan_share);
        values[12] = Float8GetDatum(heat_score);

        if (heat_score >= rp_index_heat_threshold)
            snprintf(rec_buf, sizeof(rec_buf),
                     "HOT  - include column filter (heat=%.2f)", heat_score);
        else if (heat_score >= 0.30)
            snprintf(rec_buf, sizeof(rec_buf),
                     "WARM - monitor (heat=%.2f)", heat_score);
        else
            snprintf(rec_buf, sizeof(rec_buf),
                     "COLD - skip (heat=%.2f)", heat_score);
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

    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    tupdesc       = rsinfo->setDesc;
    tupstore      = rsinfo->setResult;
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("rp_collect_table_stats: SPI_connect failed")));

    if (SPI_execute(
        "SELECT"
        "  st.schemaname,"
        "  st.relname                       AS tablename,"
        "  c.reltuples::bigint              AS row_estimate,"
        "  c.relpages::bigint               AS total_pages,"
        "  st.seq_scan,"
        "  st.idx_scan,"
        "  st.n_dead_tup,"
        "  st.n_mod_since_analyze,"
        "  CASE WHEN (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "             COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "             COALESCE(st.idx_scan,0)) = 0 THEN 0.0"
        "       ELSE (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "              COALESCE(st.n_tup_del,0))::float8 /"
        "             (COALESCE(st.n_tup_ins,0)+COALESCE(st.n_tup_upd,0)+"
        "              COALESCE(st.n_tup_del,0)+COALESCE(st.seq_scan,0)+"
        "              COALESCE(st.idx_scan,0)) END               AS write_ratio,"
        "  EXISTS(SELECT 1 FROM pg_index ix"
        "          WHERE ix.indrelid = c.oid AND ix.indisprimary) AS has_pk,"
        "  (SELECT COUNT(*) FROM pg_attribute a"
        "   WHERE a.attrelid = c.oid AND a.attnum > 0"
        "     AND NOT a.attisdropped)::int                        AS column_count"
        " FROM pg_stat_user_tables st"
        " JOIN pg_class c ON c.oid = st.relid"
        " WHERE c.relkind = 'r'"
        " ORDER BY c.reltuples DESC",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("rp_collect_table_stats: SPI_execute failed")));

    proc         = SPI_processed;
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
        double      write_ratio = 0.0, elig_score;
        bool        has_pk;
        int64       row_est = 0, idx_sc = 0;
        StringInfoData skip_reason;

        memset(nulls, false, sizeof(nulls));
        initStringInfo(&skip_reason);

        values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull); nulls[0] = isnull;
        values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull); nulls[1] = isnull;

        values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull);
        nulls[2]  = isnull;
        if (!isnull) row_est = DatumGetInt64(values[2]);

        values[3] = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull); nulls[3] = isnull;
        values[4] = SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull); nulls[4] = isnull;

        values[5] = SPI_getbinval(spi_tuple, spi_tupdesc, 6, &isnull);
        nulls[5]  = isnull;
        if (!isnull) idx_sc = DatumGetInt64(values[5]);

        values[6] = SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull); nulls[6] = isnull;
        values[7] = SPI_getbinval(spi_tuple, spi_tupdesc, 8, &isnull); nulls[7] = isnull;

        values[8] = SPI_getbinval(spi_tuple, spi_tupdesc, 9, &isnull);
        nulls[8]  = isnull;
        if (!isnull) write_ratio = DatumGetFloat8(values[8]);

        has_pk    = DatumGetBool(SPI_getbinval(spi_tuple, spi_tupdesc, 10, &isnull));
        values[9] = BoolGetDatum(has_pk);

        values[10] = SPI_getbinval(spi_tuple, spi_tupdesc, 11, &isnull); nulls[10] = isnull;

        elig_score = 1.0;

        if (write_ratio > rp_write_heavy_threshold)
        {
            elig_score -= 0.6;
            appendStringInfo(&skip_reason, "write-heavy(%.0f%%); ", write_ratio * 100);
        }
        if (!has_pk)
        {
            elig_score -= 0.3;
            appendStringInfoString(&skip_reason, "no-pk; ");
        }
        if (row_est < 100)
        {
            elig_score -= 0.2;
            appendStringInfoString(&skip_reason, "tiny-table; ");
        }
        if (row_est > 1000000 && idx_sc == 0)
        {
            elig_score -= 0.4;
            appendStringInfoString(&skip_reason, "large+no-idx-use; ");
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
 * Requires pg_stat_statements (PG13+; we use total_exec_time / mean_exec_time).
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

    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    tupdesc       = rsinfo->setDesc;
    tupstore      = rsinfo->setResult;
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("rp_collect_workload_stats: SPI_connect failed")));

    if (SPI_execute(
        "SELECT"
        "  queryid::bigint                                  AS query_hash,"
        "  calls::bigint,"
        "  total_exec_time                                  AS total_exec_ms,"
        "  mean_exec_time                                   AS mean_exec_ms,"
        "  calls::float8 / NULLIF(SUM(calls) OVER (), 0)    AS call_share,"
        "  LEFT(query, 200)                                 AS query_snippet,"
        "  (query ILIKE '%where%')                          AS contains_where"
        " FROM pg_stat_statements"
        " WHERE query NOT ILIKE '%pg_stat%'"
        "   AND query ILIKE 'SELECT%'"
        " ORDER BY calls DESC"
        " LIMIT 500",
        true, 0) != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("rp_collect_workload_stats: SPI_execute failed - "
                               "ensure pg_stat_statements is installed")));

    proc         = SPI_processed;
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
        double      call_share = 0.0;
        char        tier[32];

        memset(nulls, false, sizeof(nulls));

        values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull); nulls[0] = isnull;
        values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull); nulls[1] = isnull;
        values[2] = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull); nulls[2] = isnull;
        values[3] = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull); nulls[3] = isnull;

        values[4] = SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull);
        nulls[4]  = isnull;
        if (!isnull) call_share = DatumGetFloat8(values[4]);

        values[5] = SPI_getbinval(spi_tuple, spi_tupdesc, 6, &isnull); nulls[5] = isnull;
        values[6] = SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull); nulls[6] = isnull;

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
 *
 * Final score = 0.30*query_freq + 0.40*index_heat + 0.30*table_elig.
 * Decision:
 *   te < 0.2                  -> SKIP   (no-pk / write-heavy)
 *   final_score >= 0.50       -> REPLICATE_ALL
 *   final_score >= 0.20       -> REPLICATE_FILTERED
 *   otherwise                 -> SKIP
 *
 * hot_columns / row_filter_expr are derived from the columns of the
 * dominant index per table (not from index names) so the resulting
 * filter is valid SQL.
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
    StringInfoData  sql;
    uint64          proc, i;

    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    tupdesc       = rsinfo->setDesc;
    tupstore      = rsinfo->setResult;
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("rp_analyze_and_score: SPI_connect failed")));

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "WITH table_stats AS ("
        "  SELECT"
        "    st.relid,"
        "    st.schemaname,"
        "    st.relname AS tablename,"
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
        "            WHERE ix.indrelid = c.oid AND ix.indisprimary) AS has_pk"
        "  FROM pg_stat_user_tables st"
        "  JOIN pg_class c ON c.oid = st.relid"
        "  WHERE c.relkind = 'r'"
        "),"
        "top_index_per_table AS ("
        "  SELECT DISTINCT ON (s.relid)"
        "    s.relid, s.indexrelid, s.idx_scan,"
        "    SUM(s.idx_scan) OVER (PARTITION BY s.relid) AS total_idx_scan"
        "  FROM pg_stat_user_indexes s"
        "  ORDER BY s.relid, s.idx_scan DESC NULLS LAST"
        "),"
        "index_heat AS ("
        "  SELECT"
        "    tipt.relid,"
        "    CASE WHEN tipt.total_idx_scan = 0 THEN 0.0"
        "         ELSE tipt.idx_scan::float8 / tipt.total_idx_scan END AS max_scan_share,"
        "    (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY x.ord)"
        "     FROM pg_index ix"
        "     JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord)"
        "          ON x.attnum > 0"
        "     JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum"
        "     WHERE ix.indexrelid = tipt.indexrelid) AS hot_columns,"
        "    (SELECT string_agg(format('%%I IS NOT NULL', a.attname), ' AND ' ORDER BY x.ord)"
        "     FROM pg_index ix"
        "     JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord)"
        "          ON x.attnum > 0"
        "     JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum"
        "     WHERE ix.indexrelid = tipt.indexrelid) AS row_filter_expr"
        "  FROM top_index_per_table tipt"
        ")"
        "SELECT"
        "  ts.schemaname,"
        "  ts.tablename,"
        "  CASE WHEN SUM(ts.idx_scan::float8) OVER () = 0 THEN 0.0"
        "       ELSE ts.idx_scan::float8 / SUM(ts.idx_scan::float8) OVER () END AS query_freq_score,"
        "  COALESCE(ih.max_scan_share, 0.0)                AS index_heat_score,"
        "  CASE WHEN NOT ts.has_pk                THEN 0.1"
        "       WHEN ts.write_ratio > %f          THEN 0.1"
        "       WHEN ts.row_estimate < 100        THEN 0.3"
        "       ELSE 1.0 END                                AS table_elig_score,"
        "  COALESCE(ih.hot_columns, 'none')                AS hot_columns,"
        "  COALESCE(ih.row_filter_expr, '')                AS row_filter_expr,"
        "  ts.write_ratio,"
        "  ts.has_pk,"
        "  ts.row_estimate"
        " FROM table_stats ts"
        " LEFT JOIN index_heat ih ON ih.relid = ts.relid"
        " ORDER BY query_freq_score DESC",
        rp_write_heavy_threshold);

    if (SPI_execute(sql.data, true, 0) != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("rp_analyze_and_score: SPI_execute failed")));

    proc         = SPI_processed;
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
        double      qf = 0.0, ih_score = 0.0, te = 0.0, final_score;
        double      write_ratio = 0.0;
        bool        has_pk = false;
        int64       row_est = 0;
        char       *row_filter_expr;
        char        decision[64];
        StringInfoData filter_hint, skip_reason_buf;

        memset(nulls, false, sizeof(nulls));
        initStringInfo(&filter_hint);
        initStringInfo(&skip_reason_buf);

        values[0] = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull); nulls[0] = isnull;
        values[1] = SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull); nulls[1] = isnull;

        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull);
            if (!isnull) qf = DatumGetFloat8(d);
        }
        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull);
            if (!isnull) ih_score = DatumGetFloat8(d);
        }
        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 5, &isnull);
            if (!isnull) te = DatumGetFloat8(d);
        }
        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 8, &isnull);
            if (!isnull) write_ratio = DatumGetFloat8(d);
        }
        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 9, &isnull);
            if (!isnull) has_pk = DatumGetBool(d);
        }
        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 10, &isnull);
            if (!isnull) row_est = DatumGetInt64(d);
        }

        final_score = (0.30 * qf) + (0.40 * ih_score) + (0.30 * te);

        values[2] = Float8GetDatum(final_score);
        values[3] = Float8GetDatum(qf);
        values[4] = Float8GetDatum(ih_score);
        values[5] = Float8GetDatum(te);
        values[6] = SPI_getbinval(spi_tuple, spi_tupdesc, 6, &isnull);
        nulls[6]  = isnull;

        if (te < 0.2)
        {
            snprintf(decision, sizeof(decision), "SKIP");
            if (write_ratio > rp_write_heavy_threshold)
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

        {
            Datum d;
            d = SPI_getbinval(spi_tuple, spi_tupdesc, 7, &isnull);
            row_filter_expr = isnull ? NULL : TextDatumGetCString(d);
        }

        if (strcmp(decision, "REPLICATE_FILTERED") == 0)
        {
            if (row_filter_expr && row_filter_expr[0] != '\0')
                appendStringInfo(&filter_hint, "WHERE (%s)", row_filter_expr);
            else
                appendStringInfoString(&filter_hint,
                    "WHERE created_at >= NOW() - INTERVAL '7 days'");
        }
        else if (strcmp(decision, "REPLICATE_ALL") == 0)
            appendStringInfoString(&filter_hint, "-- no filter, replicate full table");
        else
            appendStringInfoString(&filter_hint, "-- skipped");

        values[8] = CStringGetTextDatum(filter_hint.data);
        values[9] = CStringGetTextDatum(skip_reason_buf.len > 0
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
 * Identifiers are quoted via quote_identifier().
 * Row filters use the dominant index's columns (PG15+ syntax).
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
    StringInfoData  sql;
    uint64          proc, i;

    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    tupdesc       = rsinfo->setDesc;
    tupstore      = rsinfo->setResult;
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("rp_generate_publication_ddl: SPI_connect failed")));

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "WITH table_stats AS ("
        "  SELECT st.relid, st.schemaname, st.relname AS tablename,"
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
        "            WHERE ix.indrelid = c.oid AND ix.indisprimary) AS has_pk"
        "  FROM pg_stat_user_tables st"
        "  JOIN pg_class c ON c.oid = st.relid"
        "  WHERE c.relkind = 'r'"
        "),"
        "top_index_per_table AS ("
        "  SELECT DISTINCT ON (s.relid)"
        "    s.relid, s.indexrelid, s.idx_scan,"
        "    SUM(s.idx_scan) OVER (PARTITION BY s.relid) AS total_idx_scan"
        "  FROM pg_stat_user_indexes s"
        "  ORDER BY s.relid, s.idx_scan DESC NULLS LAST"
        "),"
        "index_heat AS ("
        "  SELECT tipt.relid,"
        "    CASE WHEN tipt.total_idx_scan = 0 THEN 0.0"
        "         ELSE tipt.idx_scan::float8 / tipt.total_idx_scan END AS max_scan_share,"
        "    (SELECT string_agg(format('%%I IS NOT NULL', a.attname), ' AND ' ORDER BY x.ord)"
        "     FROM pg_index ix"
        "     JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord)"
        "          ON x.attnum > 0"
        "     JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = x.attnum"
        "     WHERE ix.indexrelid = tipt.indexrelid) AS row_filter_expr"
        "  FROM top_index_per_table tipt"
        "),"
        "scored AS ("
        "  SELECT ts.schemaname, ts.tablename,"
        "    ts.write_ratio, ts.has_pk, ts.row_estimate,"
        "    CASE WHEN SUM(ts.idx_scan::float8) OVER () = 0 THEN 0.0"
        "         ELSE ts.idx_scan::float8 / SUM(ts.idx_scan::float8) OVER () END AS qf,"
        "    COALESCE(ih.max_scan_share, 0.0) AS ih,"
        "    CASE WHEN NOT ts.has_pk                THEN 0.1"
        "         WHEN ts.write_ratio > %f          THEN 0.1"
        "         WHEN ts.row_estimate < 100        THEN 0.3"
        "         ELSE 1.0 END                       AS te,"
        "    COALESCE(ih.row_filter_expr, '')       AS row_filter_expr"
        "  FROM table_stats ts"
        "  LEFT JOIN index_heat ih ON ih.relid = ts.relid"
        "),"
        "decisions AS ("
        "  SELECT *,"
        "    (0.30*qf + 0.40*ih + 0.30*te) AS final_score,"
        "    CASE WHEN te < 0.2                          THEN 'SKIP'"
        "         WHEN (0.30*qf+0.40*ih+0.30*te) >= 0.50 THEN 'REPLICATE_ALL'"
        "         WHEN (0.30*qf+0.40*ih+0.30*te) >= 0.20 THEN 'REPLICATE_FILTERED'"
        "         ELSE 'SKIP' END AS decision"
        "  FROM scored"
        ")"
        "SELECT schemaname, tablename, decision, row_filter_expr, final_score"
        " FROM decisions"
        " WHERE decision <> 'SKIP'"
        " ORDER BY final_score DESC",
        rp_write_heavy_threshold);

    if (SPI_execute(sql.data, true, 0) != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("rp_generate_publication_ddl: SPI_execute failed")));

    proc         = SPI_processed;
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
        char       *schema, *table, *decision, *row_filter_expr;
        const char *qschema, *qtable;
        StringInfoData pub_name, qpub_name, row_filter, ddl;

        memset(nulls, false, sizeof(nulls));
        initStringInfo(&pub_name);
        initStringInfo(&qpub_name);
        initStringInfo(&row_filter);
        initStringInfo(&ddl);

        schema   = NameStr(*DatumGetName(SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull)));
        table    = NameStr(*DatumGetName(SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull)));
        decision = TextDatumGetCString(SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull));
        {
            Datum d = SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull);
            row_filter_expr = isnull ? NULL : TextDatumGetCString(d);
        }

        /* Publication name: rp_<schema>_<table> (raw identifier form
         * for display) and a quoted form for the DDL statement. */
        appendStringInfo(&pub_name, "rp_%s_%s", schema, table);
        appendStringInfoString(&qpub_name, quote_identifier(pub_name.data));
        qschema = quote_identifier(schema);
        qtable  = quote_identifier(table);

        if (strcmp(decision, "REPLICATE_FILTERED") == 0)
        {
            if (row_filter_expr && row_filter_expr[0] != '\0')
                appendStringInfoString(&row_filter, row_filter_expr);
            else
                appendStringInfoString(&row_filter,
                    "created_at >= NOW() - INTERVAL '30 days'");
        }

        if (strcmp(decision, "REPLICATE_ALL") == 0)
        {
            appendStringInfo(&ddl,
                "CREATE PUBLICATION %s\n"
                "  FOR TABLE %s.%s\n"
                "  WITH (publish = 'insert, update, delete, truncate');",
                qpub_name.data, qschema, qtable);
        }
        else
        {
            appendStringInfo(&ddl,
                "CREATE PUBLICATION %s\n"
                "  FOR TABLE %s.%s\n"
                "  WHERE (%s)\n"
                "  WITH (publish = 'insert, update, delete');",
                qpub_name.data, qschema, qtable, row_filter.data);
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
 * TRUNCATE plan_results, then refill from analyze_and_score().
 * Returns a single summary text.
 * ================================================================ */
Datum
rp_run_full_plan(PG_FUNCTION_ARGS)
{
    StringInfoData result;
    int            rc;
    uint64         processed;

    initStringInfo(&result);

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("rp_run_full_plan: SPI_connect failed")));

    rc = SPI_execute("TRUNCATE replication_planner.plan_results", false, 0);
    if (rc != SPI_OK_UTILITY)
        ereport(ERROR, (errmsg("rp_run_full_plan: truncate failed (rc=%d)", rc)));

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
        ereport(ERROR, (errmsg("rp_run_full_plan: insert failed (rc=%d)", rc)));

    processed = SPI_processed;

    SPI_finish();

    appendStringInfo(&result,
        "Full plan complete. %lu tables analyzed. "
        "Query replication_planner.plan_results for details.",
        (unsigned long) processed);

    PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

/* ================================================================
 * _PG_init - register GUC parameters
 * ================================================================ */
void _PG_init(void);

void
_PG_init(void)
{
    DefineCustomRealVariable(
        "replication_planner.index_heat_threshold",
        "Min heat score (0-1) to classify an index as HOT in collect_index_stats.",
        NULL,
        &rp_index_heat_threshold,
        0.70, 0.0, 1.0,
        PGC_SUSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "replication_planner.write_heavy_threshold",
        "Write ratio (0-1) above which a table is penalised as write-heavy.",
        NULL,
        &rp_write_heavy_threshold,
        0.90, 0.0, 1.0,
        PGC_SUSET, 0, NULL, NULL, NULL);

    MarkGUCPrefixReserved("replication_planner");
}
