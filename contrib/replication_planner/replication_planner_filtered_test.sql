-- =============================================================
-- test/replication_planner_filtered_test.sql
--
-- Goal: demonstrate the REPLICATE_FILTERED decision (which the
-- original smoke test never reaches, because every eligible table
-- there scores >= 0.50 and lands in REPLICATE_ALL).
--
-- Scoring recap (see replication_planner.c):
--   final = 0.30*query_freq + 0.40*index_heat + 0.30*table_elig
--   te < 0.2            -> SKIP
--   final >= 0.50       -> REPLICATE_ALL
--   final >= 0.20       -> REPLICATE_FILTERED
--   else                -> SKIP
--
-- How we hit each band:
--   rf_hot_users    -> REPLICATE_ALL      : PK + one selective index
--                                           queried heavily. index_heat
--                                           ~1.0 so final ~0.70.
--   rf_cold_archive -> REPLICATE_FILTERED : PK, read-heavy, >100 rows
--                                           (te=1.0) BUT every read is a
--                                           sequential scan, so the table
--                                           gets ZERO index scans.
--                                           => query_freq=0, index_heat=0
--                                           => final = 0.30*1.0 = 0.30,
--                                              which falls in [0.20,0.50).
--   rf_nopk_log     -> SKIP               : no primary key => te=0.1 < 0.2.
--
-- Run:
--   psql -v ON_ERROR_STOP=1 -f replication_planner_filtered_test.sql
-- =============================================================

\set ON_ERROR_STOP on

\echo '=== replication_planner REPLICATE_FILTERED demo ==='

-- -------------------------------------------------------------
-- 0. Prerequisites
-- -------------------------------------------------------------
\echo '--- 0. Setup prerequisites ---'

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN
        RAISE NOTICE 'pg_stat_statements not installed — installing now';
        CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    END IF;
END;
$$;

DROP EXTENSION IF EXISTS replication_planner CASCADE;
CREATE EXTENSION replication_planner;

-- -------------------------------------------------------------
-- 1. Fixture data
-- -------------------------------------------------------------
\echo '--- 1. Creating fixture tables ---'

DROP TABLE IF EXISTS rf_hot_users    CASCADE;
DROP TABLE IF EXISTS rf_cold_archive CASCADE;
DROP TABLE IF EXISTS rf_nopk_log     CASCADE;

-- (a) HOT table -> will become REPLICATE_ALL.
CREATE TABLE rf_hot_users
(
    id         BIGSERIAL PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO rf_hot_users (email, created_at)
SELECT 'user_' || g || '@example.com',
       now() - (random() * INTERVAL '365 days')
FROM generate_series(1, 10000) g;

ANALYZE rf_hot_users;

-- (b) COLD-but-eligible table -> will become REPLICATE_FILTERED.
--     Has a PK (te eligible) and >100 rows, and we will read it
--     heavily — but ONLY via sequential scans (predicate on an
--     unindexed column), so it accumulates no index scans at all.
CREATE TABLE rf_cold_archive
(
    id         BIGSERIAL PRIMARY KEY,
    region     TEXT           NOT NULL DEFAULT 'EU',
    status     TEXT           NOT NULL DEFAULT 'archived',
    amount     NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);
-- NOTE: deliberately NO secondary indexes here. The only index is
-- the PK, and we never query by id, so idx_scan stays 0.

INSERT INTO rf_cold_archive (region, status, amount, created_at)
SELECT CASE WHEN random() < 0.6 THEN 'EU' ELSE 'US' END,
       (ARRAY['archived', 'closed', 'void'])[ceil(random() * 3)::int],
       (random() * 1000)::numeric(12, 2),
       now() - (random() * INTERVAL '365 days')
FROM generate_series(1, 5000);

ANALYZE rf_cold_archive;

-- (c) NO-PK table -> will become SKIP.
CREATE TABLE rf_nopk_log
(
    event_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    message  TEXT        NOT NULL
);

INSERT INTO rf_nopk_log (message)
SELECT 'event ' || g
FROM generate_series(1, 3000) g;

ANALYZE rf_nopk_log;

-- -------------------------------------------------------------
-- 2. Simulate workload
-- -------------------------------------------------------------
\echo '--- 2. Simulating workload ---'

DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..5000 LOOP
        -- HOT: selective equality on the indexed email column.
        -- This drives index scans on rf_hot_users -> high index_heat.
        PERFORM id FROM rf_hot_users
        WHERE email = 'user_' || i || '@example.com';

        -- COLD: predicate on the UNINDEXED `status` column forces a
        -- sequential scan every time. These count as reads (so the
        -- table is read-heavy and stays eligible, te=1.0) but add
        -- ZERO index scans -> query_freq=0 and index_heat=0.
        IF i <= 3000 THEN
            PERFORM count(*) FROM rf_cold_archive
            WHERE status = 'archived';
        END IF;
    END LOOP;
END;
$$;

ANALYZE rf_hot_users, rf_cold_archive, rf_nopk_log;

-- -------------------------------------------------------------
-- 3. Confirm the raw signals that drive the score
-- -------------------------------------------------------------
\echo '--- 3. Table signals (eligibility) ---'

SELECT tablename,
       row_estimate,
       round(write_ratio::numeric, 3) AS write_ratio,
       has_pk,
       round(eligibility_score::numeric, 3) AS elig,
       skip_reason
FROM replication_planner.collect_table_stats()
WHERE tablename IN ('rf_hot_users', 'rf_cold_archive', 'rf_nopk_log')
ORDER BY eligibility_score DESC;

\echo '--- 3b. Index scan counts (cold table should have idx_scan = 0) ---'

SELECT relname AS tablename,
       indexrelname AS indexname,
       idx_scan
FROM pg_stat_user_indexes
WHERE relname IN ('rf_hot_users', 'rf_cold_archive')
ORDER BY relname, idx_scan DESC;

-- -------------------------------------------------------------
-- 4. Phase 2: Heuristic analysis -> here REPLICATE_FILTERED appears
-- -------------------------------------------------------------
\echo '--- 4. Phase 2: Heuristic analysis (expect REPLICATE_FILTERED for rf_cold_archive) ---'

SELECT tablename,
       round(final_score::numeric, 4)      AS score,
       round(query_freq_score::numeric, 4) AS qf,
       round(index_heat_score::numeric, 4) AS ih,
       round(table_elig_score::numeric, 4) AS te,
       decision,
       row_filter_hint,
       skip_reason
FROM replication_planner.analyze_and_score()
WHERE tablename IN ('rf_hot_users', 'rf_cold_archive', 'rf_nopk_log')
ORDER BY final_score DESC;

-- -------------------------------------------------------------
-- 5. Phase 3: Publication DDL (the FILTERED row carries a WHERE clause)
-- -------------------------------------------------------------
-- The row_filter is now a DATA-DRIVEN value predicate: the planner picks
-- the lowest-cardinality string column (here `region`, 2 distinct values)
-- and its most-common value from pg_stats, yielding e.g. region = 'EU'
-- instead of the old degenerate `id IS NOT NULL`.
\echo '--- 5. Phase 3: Publication DDL (expect WHERE (region = ...) ) ---'

SELECT publication_name,
       tablename,
       decision,
       row_filter,
       ddl_statement
FROM replication_planner.generate_publication_ddl()
WHERE tablename IN ('rf_hot_users', 'rf_cold_archive')
ORDER BY decision;

-- -------------------------------------------------------------
-- 5b. EXECUTE the generated FILTERED DDL to prove it actually runs
--     (a value-based row filter on a non-PK column requires
--     REPLICA IDENTITY FULL under PG15 — the DDL now emits that).
-- -------------------------------------------------------------
\echo '--- 5b. Executing the generated FILTERED DDL ---'

DO $$
DECLARE
    stmt text;
BEGIN
    SELECT ddl_statement INTO stmt
    FROM replication_planner.generate_publication_ddl()
    WHERE tablename = 'rf_cold_archive'
      AND decision = 'REPLICATE_FILTERED'
    LIMIT 1;

    IF stmt IS NULL THEN
        RAISE EXCEPTION 'No FILTERED DDL was generated for rf_cold_archive';
    END IF;

    EXECUTE stmt;   -- raises if the row filter / replica identity is invalid
    RAISE NOTICE 'OK: generated DDL executed successfully';
    EXECUTE 'DROP PUBLICATION IF EXISTS rp_public_rf_cold_archive';
END;
$$;

-- -------------------------------------------------------------
-- 6. Assertion: at least one REPLICATE_FILTERED must exist
-- -------------------------------------------------------------
\echo '--- 6. Assert REPLICATE_FILTERED was produced ---'

DO $$
DECLARE
    n INT;
BEGIN
    SELECT count(*) INTO n
    FROM replication_planner.analyze_and_score()
    WHERE tablename = 'rf_cold_archive'
      AND decision = 'REPLICATE_FILTERED';

    IF n = 0 THEN
        RAISE EXCEPTION 'FAILED: rf_cold_archive did not produce REPLICATE_FILTERED';
    END IF;
    RAISE NOTICE 'OK: rf_cold_archive -> REPLICATE_FILTERED as expected';
END;
$$;

-- -------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------
\echo '--- Cleanup ---'

DROP TABLE rf_hot_users    CASCADE;
DROP TABLE rf_cold_archive CASCADE;
DROP TABLE rf_nopk_log     CASCADE;

\echo '=== REPLICATE_FILTERED demo complete ==='
