-- =============================================================
-- test/replication_planner_test.sql
-- Regression / smoke tests for replication_planner
-- Run:
--   psql -v ON_ERROR_STOP=1 -f replication_planner_test.sql
-- =============================================================

\set
ON_ERROR_STOP on

\echo '=== replication_planner regression tests ==='

---

-- 0. Prerequisites

---

\echo '--- 0. Setup prerequisites ---'

DO $$
BEGIN
IF
NOT EXISTS (
SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
) THEN
RAISE NOTICE 'pg_stat_statements not installed — installing now';
CREATE
EXTENSION IF NOT EXISTS pg_stat_statements;
END IF;
END;
$$;

DROP
EXTENSION IF EXISTS replication_planner CASCADE;
CREATE
EXTENSION replication_planner;

---

-- 1. Fixture data

---

\echo
'--- 1. Creating fixture tables ---'

DROP TABLE IF EXISTS test_orders CASCADE;
DROP TABLE IF EXISTS test_users CASCADE;
DROP TABLE IF EXISTS test_tmp_junk CASCADE;

CREATE TABLE test_orders
(
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT         NOT NULL,
    region     TEXT           NOT NULL DEFAULT 'EU',
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    amount     NUMERIC(12, 2) NOT NULL,
    status     TEXT           NOT NULL DEFAULT 'pending'
);

CREATE INDEX idx_orders_region ON test_orders (region);
CREATE INDEX idx_orders_created_at ON test_orders (created_at);
CREATE INDEX idx_orders_user_id ON test_orders (user_id);

INSERT INTO test_orders (user_id, region, created_at, amount, status)
SELECT (random() * 9999 + 1)::int, CASE WHEN random() < 0.6 THEN 'EU' ELSE 'US' END,
       now() - (random() * INTERVAL '30 days'),
       (random() * 1000)::numeric(12,2), (ARRAY['pending', 'shipped', 'delivered'])[ceil(random()*3)::int]
FROM generate_series(1, 50000);

ANALYZE
test_orders;

CREATE TABLE test_users
(
    id         BIGSERIAL PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO test_users (email, created_at)
SELECT 'user_' || g || '@example.com',
       now() - (random() * INTERVAL '365 days')
FROM generate_series(1, 10000) g;

ANALYZE
test_users;

CREATE TABLE test_tmp_junk
(
    val TEXT
);

INSERT INTO test_tmp_junk
VALUES ('a'),
       ('b');

---

-- 2. Simulate read workload

---

\echo
'--- 2. Simulating read workload ---'

DO $$
DECLARE
i INT;
BEGIN
FOR i IN 1..3000 LOOP
PERFORM COUNT(*) FROM test_orders WHERE region = 'EU';


PERFORM
COUNT(*) FROM test_orders
  WHERE created_at >= now() - INTERVAL '7 days';

PERFORM
id FROM test_users
  WHERE email = 'user_' || i || '@example.com';

END LOOP;
END;
$$;

ANALYZE
test_orders, test_users, test_tmp_junk;

---

-- 3. Phase 1-A: Index Statistics

---

\echo
'--- 3. Phase 1-A: Index Statistics ---'

\echo '3a. Raw results'
SELECT indexname, idx_scans, hit_ratio, scan_share, heat_score, recommendation
FROM replication_planner.collect_index_stats()
WHERE tablename IN ('test_orders', 'test_users')
ORDER BY heat_score DESC;

\echo
'3b. Heat view'
SELECT tablename, indexname, heat_score, recommendation
FROM replication_planner.v_index_heat
WHERE tablename IN ('test_orders', 'test_users') LIMIT 10;

---

-- 4. Phase 1-B: Table Statistics

---

\echo
'--- 4. Phase 1-B: Table Statistics ---'

SELECT tablename,
       row_estimate,
       write_ratio,
       has_pk,
       eligibility_score,
       skip_reason
FROM replication_planner.collect_table_stats()
WHERE tablename IN ('test_orders', 'test_users', 'test_tmp_junk')
ORDER BY eligibility_score DESC;

---

-- 5. Phase 1-C: Workload patterns

---

\echo
'--- 5. Phase 1-C: Workload patterns ---'

SELECT heat_tier,
       calls,
       call_pct,
       mean_ms,
       contains_where, LEFT (query_snippet, 80)
FROM replication_planner.v_hot_queries
    LIMIT 5;

---

-- 6. Phase 2: Heuristic Analysis

---

\echo
'--- 6. Phase 2: Heuristic Analysis ---'

SELECT tablename,
       round(final_score::numeric, 4) AS score,
       decision,
       hot_index_cols,
       row_filter_hint,
       skip_reason
FROM replication_planner.analyze_and_score()
WHERE tablename IN ('test_orders', 'test_users', 'test_tmp_junk')
ORDER BY final_score DESC;

---

-- 7. Phase 3: Publication DDL

---

\echo
'--- 7. Phase 3: Publication DDL ---'

SELECT publication_name,
       tablename,
       decision,
       row_filter,
       ddl_statement
FROM replication_planner.generate_publication_ddl();

---

-- 8. Full pipeline

---

\echo
'--- 8. Full pipeline (run_full_plan) ---'

SELECT replication_planner.run_full_plan();

SELECT tablename,
       round(final_score::numeric, 4) AS score,
       decision,
       row_filter_hint
FROM replication_planner.plan_results
ORDER BY final_score DESC;

SELECT replication_planner.save_publication_ddl();

SELECT publication_name,
       tablename,
       decision,
       row_filter
FROM replication_planner.generated_ddl;

---

-- 9. Column histograms

---

\echo
'--- 9. Column histograms ---'

SELECT tablename,
       column_name,
       n_distinct,
       correlation, LEFT (top_values, 60) AS top_vals
FROM replication_planner.v_column_histograms
WHERE tablename IN ('test_orders'
    , 'test_users')
  AND column_name IN ('region'
    , 'status'
    , 'user_id')
ORDER BY tablename, column_name;

---

-- 10. GUC settings

---

\echo
'--- 10. GUC settings ---'

SELECT name, setting, short_desc
FROM replication_planner.v_settings;

SET
replication_planner.index_heat_threshold = 0.50;

SELECT name, setting
FROM replication_planner.v_settings
WHERE name = 'replication_planner.index_heat_threshold';

---

-- Cleanup

---

\echo
'--- Cleanup ---'

DROP TABLE test_orders CASCADE;
DROP TABLE test_users CASCADE;
DROP TABLE test_tmp_junk CASCADE;

\echo
'=== All tests passed ==='
