-- =============================================================
-- test/replication_planner_workload_filter_test.sql
--
-- Demonstrates a WORKLOAD-DRIVEN row filter:
--   the filter COLUMN is the one the workload filters on most often
--   (mined from pg_stat_statements), and the VALUE is the column's
--   most-common value from pg_stats.
--
-- Schema + workload mirror the user's example:
--   orders(id, user_id, number, ...)
--   select * from orders where id = 1;
--   select * from orders where user_id = 1;
--   select * from orders where user_id = 1 and number = 3;
--
-- user_id appears in 2 of the 3 query patterns => it is the most
-- frequently filtered column => the generated filter is:
--   WHERE (user_id = <most common user_id value>)
--
-- Two things make this demo deterministic:
--   1. pg_stat_statements.track = 'all'  -> the SELECTs executed inside
--      the DO loop are actually recorded (default 'top' would skip them).
--   2. enable_indexscan/bitmapscan = off during the workload -> every
--      read is a SEQ scan, so `orders` accumulates ZERO index scans.
--      That keeps query_freq=0 and index_heat=0, so orders scores
--      final = 0.30*te = 0.30 and lands in REPLICATE_FILTERED rather
--      than REPLICATE_ALL.  (The column-mining reads query TEXT, not
--      scan counts, so it still works.)
--
-- Run:
--   psql -v ON_ERROR_STOP=1 -f replication_planner_workload_filter_test.sql
-- =============================================================

\set ON_ERROR_STOP on

\echo '=== replication_planner workload-driven filter demo ==='

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
-- 1. Fixture schema (users / orders / products / order_product)
-- -------------------------------------------------------------
\echo '--- 1. Creating fixture tables ---'

DROP TABLE IF EXISTS order_product CASCADE;
DROP TABLE IF EXISTS orders        CASCADE;
DROP TABLE IF EXISTS products      CASCADE;
DROP TABLE IF EXISTS users         CASCADE;

CREATE TABLE users
(
    id     BIGSERIAL PRIMARY KEY,
    email  TEXT NOT NULL,
    name   TEXT,
    phone  TEXT,
    region TEXT
);

CREATE TABLE orders
(
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT      NOT NULL,
    number     INT         NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products
(
    id   BIGSERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE order_product
(
    id         BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    order_id   BIGINT NOT NULL
);

-- orders: user_id is deliberately skewed so value 1 is the most common
-- (so the emitted filter is the familiar `user_id = 1`).
INSERT INTO orders (user_id, number, created_at)
SELECT CASE WHEN random() < 0.5 THEN 1 ELSE (random() * 199 + 2)::int END,
       (random() * 9 + 1)::int,
       now() - (random() * INTERVAL '30 days')
FROM generate_series(1, 5000);

INSERT INTO users (email, name, phone, region)
SELECT 'user_' || g || '@example.com', 'name ' || g, '555-' || g,
       CASE WHEN random() < 0.6 THEN 'EU' ELSE 'US' END
FROM generate_series(1, 2000) g;

INSERT INTO products (name)
SELECT 'product ' || g FROM generate_series(1, 500) g;

INSERT INTO order_product (product_id, order_id)
SELECT (random() * 499 + 1)::int, (random() * 4999 + 1)::int
FROM generate_series(1, 8000);

ANALYZE users, orders, products, order_product;

-- -------------------------------------------------------------
-- 2. Simulate the read workload
-- -------------------------------------------------------------
\echo '--- 2. Simulating workload (orders filtered by id / user_id / number) ---'

-- Record statements executed inside the DO block (default 'top' skips them).
SET pg_stat_statements.track = 'all';
SELECT pg_stat_statements_reset();

-- Force sequential scans so `orders` records no index scans -> stays cold
-- in the score -> REPLICATE_FILTERED (see header note).
SET enable_indexscan = off;
SET enable_bitmapscan = off;

DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..2000 LOOP
        PERFORM * FROM orders WHERE id = i;
        PERFORM * FROM orders WHERE user_id = 2;
        PERFORM * FROM orders WHERE user_id = 2 AND number = 10;
    END LOOP;
END;
$$;

RESET enable_indexscan;
RESET enable_bitmapscan;

ANALYZE users, orders, products, order_product;

-- -------------------------------------------------------------
-- 3. What the workload tells us (predicate columns per query)
-- -------------------------------------------------------------
\echo '--- 3. Recorded SELECT workload on orders ---'

SELECT calls, LEFT(query_snippet, 70) AS query
FROM replication_planner.collect_workload_stats()
WHERE query_snippet ILIKE '%orders%'
ORDER BY calls DESC;

-- -------------------------------------------------------------
-- 4. Heuristic analysis: orders -> REPLICATE_FILTERED on user_id
-- -------------------------------------------------------------
\echo '--- 4. Heuristic analysis (expect orders -> REPLICATE_FILTERED, filter on user_id) ---'

SELECT tablename,
       round(final_score::numeric, 4) AS score,
       decision,
       row_filter_hint
FROM replication_planner.analyze_and_score()
WHERE tablename = 'orders';

-- -------------------------------------------------------------
-- 5. Publication DDL (value-based filter on the hottest column)
-- -------------------------------------------------------------
\echo '--- 5. Publication DDL ---'

SELECT publication_name,
       tablename,
       decision,
       row_filter,
       ddl_statement
FROM replication_planner.generate_publication_ddl()
WHERE tablename = 'orders';

-- -------------------------------------------------------------
-- 5b. Execute the generated DDL to prove it runs
-- -------------------------------------------------------------
\echo '--- 5b. Executing the generated DDL ---'

DO $$
DECLARE
    stmt text;
BEGIN
    SELECT ddl_statement INTO stmt
    FROM replication_planner.generate_publication_ddl()
    WHERE tablename = 'orders' AND decision = 'REPLICATE_FILTERED'
    LIMIT 1;

    IF stmt IS NULL THEN
        RAISE EXCEPTION 'No FILTERED DDL was generated for orders';
    END IF;

    EXECUTE stmt;   -- raises if the row filter / replica identity is invalid
    RAISE NOTICE 'OK: generated DDL executed successfully';
    EXECUTE 'DROP PUBLICATION IF EXISTS rp_public_orders';
END;
$$;

-- -------------------------------------------------------------
-- 6. Assertions
-- -------------------------------------------------------------
\echo '--- 6. Assert orders -> REPLICATE_FILTERED on user_id ---'

DO $$
DECLARE
    rec record;
BEGIN
    SELECT decision, row_filter_hint INTO rec
    FROM replication_planner.analyze_and_score()
    WHERE tablename = 'orders';

    IF rec.decision <> 'REPLICATE_FILTERED' THEN
        RAISE EXCEPTION 'FAILED: orders decision = % (expected REPLICATE_FILTERED)', rec.decision;
    END IF;
    IF rec.row_filter_hint NOT ILIKE '%user_id%' THEN
        RAISE EXCEPTION 'FAILED: filter does not use user_id: %', rec.row_filter_hint;
    END IF;
    RAISE NOTICE 'OK: orders -> REPLICATE_FILTERED, filter = %', rec.row_filter_hint;
END;
$$;

-- -------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------
\echo '--- Cleanup ---'

DROP TABLE order_product CASCADE;
DROP TABLE orders        CASCADE;
DROP TABLE products      CASCADE;
DROP TABLE users         CASCADE;

\echo '=== workload-driven filter demo complete ==='
