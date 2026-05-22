# replication_planner

**Autonomous Replication Planner** — A PostgreSQL extension that analyses
index statistics, table distribution, and workload query patterns to
automatically generate optimal logical replication rules.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                  replication_planner                         │
│                                                              │
│  Phase 1 — Telemetry Collection (C SRF functions)           │
│  ┌─────────────────┐ ┌──────────────────┐ ┌──────────────┐  │
│  │ collect_index_  │ │ collect_table_   │ │ collect_     │  │
│  │ stats()         │ │ stats()          │ │ workload_    │  │
│  │                 │ │                  │ │ stats()      │  │
│  │ pg_stat_user_   │ │ pg_class         │ │ pg_stat_     │  │
│  │ indexes         │ │ pg_stat_user_    │ │ statements   │  │
│  │ pg_statio_user_ │ │ tables           │ │              │  │
│  │ indexes         │ │ pg_attribute     │ │              │  │
│  └────────┬────────┘ └────────┬─────────┘ └──────┬───────┘  │
│           │                  │                   │           │
│           └──────────────────┼───────────────────┘           │
│                              ▼                               │
│  Phase 2 — Heuristic Engine                                  │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  analyze_and_score()                                  │   │
│  │                                                       │   │
│  │  Rule 1: query_freq_score  (30% weight)               │   │
│  │  Rule 2: index_heat_score  (40% weight)               │   │
│  │  Rule 3: table_elig_score  (30% weight)               │   │
│  │                                                       │   │
│  │  final_score = 0.30*qf + 0.40*ih + 0.30*te           │   │
│  │  Decision:                                            │   │
│  │    ≥ 0.50  → REPLICATE_ALL                            │   │
│  │    ≥ 0.20  → REPLICATE_FILTERED                       │   │
│  │    < 0.20  → SKIP                                     │   │
│  └───────────────────┬───────────────────────────────────┘   │
│                      ▼                                       │
│  Phase 3 — Rule Generation                                   │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  generate_publication_ddl()                           │   │
│  │                                                       │   │
│  │  CREATE PUBLICATION rp_public_orders                  │   │
│  │    FOR TABLE public.orders                            │   │
│  │    WHERE (region IS NOT NULL)                         │   │
│  │    WITH (publish = 'insert, update, delete');         │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

---

## Requirements

| Component | Minimum version |
|---|---|
| PostgreSQL | 15 (REPLICATE_FILTERED publications use PG15 row-filter syntax) |
| pg_stat_statements | any version bundled with PG |
| Build tools | `pg_config`, GCC / Clang, `make` |

---

## Installation

### 1. Build from source

```bash
# Ensure pg_config is on PATH
export PATH=/usr/lib/postgresql/16/bin:$PATH

cd replication_planner
make
sudo make install
```

### 2. Enable pg_stat_statements

Add to `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
```

Then restart PostgreSQL.

### 3. Create the extension

```sql
-- Must be superuser
CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION replication_planner;
```

---

## Usage

### Quick start — run the full pipeline

```sql
-- Analyse everything and store results
SELECT replication_planner.run_full_plan();

-- View recommendations
SELECT tablename, score, decision, row_filter_hint
FROM   replication_planner.v_replicate_targets;

-- Generate and save the DDL
SELECT replication_planner.save_publication_ddl();

-- Inspect generated statements
SELECT publication_name, ddl_statement
FROM   replication_planner.generated_ddl
ORDER  BY generated_at DESC;

-- Apply them (review carefully first!)
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT ddl_statement FROM replication_planner.generated_ddl
           WHERE NOT applied ORDER BY id
  LOOP
    EXECUTE r.ddl_statement;
    UPDATE replication_planner.generated_ddl
    SET    applied = true
    WHERE  ddl_statement = r.ddl_statement;
  END LOOP;
END;
$$;
```

---

### Phase 1 — Telemetry

#### Index statistics

```sql
SELECT indexname, idx_scans, hit_ratio, scan_share, heat_score, recommendation
FROM   replication_planner.collect_index_stats()
ORDER  BY heat_score DESC;
```

| Column | Description |
|---|---|
| `hit_ratio` | Buffer cache hit rate for this index (0–1) |
| `scan_share` | Fraction of the table's total index scans through this index |
| `heat_score` | `0.4 × hit_ratio + 0.6 × scan_share` |
| `recommendation` | HOT / WARM / COLD + threshold used |

#### Table statistics

```sql
SELECT tablename, write_ratio, has_pk, eligibility_score, skip_reason
FROM   replication_planner.collect_table_stats()
ORDER  BY eligibility_score DESC;
```

#### Workload patterns

```sql
SELECT heat_tier, calls, call_pct, mean_ms, query_snippet
FROM   replication_planner.v_hot_queries;
```

---

### Phase 2 — Analysis

```sql
SELECT tablename, final_score, decision, row_filter_hint, skip_reason
FROM   replication_planner.analyze_and_score()
ORDER  BY final_score DESC;
```

#### Scoring rules

| Rule | Signal | Weight |
|---|---|---|
| **Query frequency** | Index scan share of this table vs all tables | 30% |
| **Index heat** | Dominant index scan share within the table | 40% |
| **Table eligibility** | PK presence, write ratio, row count | 30% |

#### Eligibility penalties

| Condition | Penalty |
|---|---|
| No primary key | −0.30 |
| Write ratio > 90% | −0.60 |
| Row count < 100 | −0.20 |
| > 1M rows + zero index usage | −0.40 |

---

### Phase 3 — DDL Generation

```sql
SELECT publication_name, decision, row_filter, ddl_statement
FROM   replication_planner.generate_publication_ddl();
```

**Example output:**

```sql
-- REPLICATE_ALL (high-traffic table, full copy)
CREATE PUBLICATION rp_public_orders
  FOR TABLE public.orders
  WITH (publish = 'insert, update, delete, truncate');

-- REPLICATE_FILTERED (warm table, hot rows only)
CREATE PUBLICATION rp_public_events
  FOR TABLE public.events
  WHERE (created_at IS NOT NULL)
  WITH (publish = 'insert, update, delete');
```

---

## Configuration (GUC Parameters)

Set in `postgresql.conf` or per-session:

```sql
-- Per-session override
SET replication_planner.index_heat_threshold = 0.60;
```

| Parameter | Default | Description |
|---|---|---|
| `replication_planner.index_heat_threshold` | `0.70` | Heat score above which an index is classified HOT in `collect_index_stats` |
| `replication_planner.write_heavy_threshold` | `0.90` | Write ratio above which a table is penalised in scoring + eligibility |

```sql
-- View all current settings
SELECT name, setting, short_desc
FROM   replication_planner.v_settings;
```

---

## Views Reference

| View | Description |
|---|---|
| `v_replicate_targets` | Tables recommended for replication, sorted by score |
| `v_index_heat` | Index heat leaderboard across all user tables |
| `v_hot_queries` | Top-50 hottest SELECT queries (CRITICAL + HOT tiers) |
| `v_column_histograms` | pg_stats histograms and MCVs for all user columns |
| `v_settings` | Current GUC parameter values |

---

## Histogram-Based Row Filtering

Use `v_column_histograms` to manually refine generated row filters:

```sql
-- Find the top histogram buckets for created_at
SELECT
    histogram_bounds,
    top_values,
    top_freqs
FROM replication_planner.v_column_histograms
WHERE tablename = 'orders'
  AND column_name = 'created_at';

-- Example result:
--   histogram_bounds → {"2024-11-01","2024-12-01","2025-01-01",...}
--   Manually craft the filter:
--     WHERE created_at >= '2024-12-01'   -- last 2 buckets = ~62% of reads

-- Update the saved DDL
UPDATE replication_planner.generated_ddl
SET ddl_statement = replace(
    ddl_statement,
    'created_at IS NOT NULL',
    'created_at >= NOW() - INTERVAL ''7 days'''
)
WHERE tablename = 'orders'
  AND NOT applied;
```

---

## Testing

```bash
psql -U postgres -f replication_planner_test.sql
```

The test script:
1. Creates fixture tables (`test_orders`, `test_users`, `test_tmp_junk`)
2. Inserts 50 000+ rows and simulates read workloads
3. Runs all three phases and validates output
4. Verifies GUC overrides work correctly
5. Cleans up all fixtures

---

## Internals

### C functions (SRF — Set-Returning Functions)

| C symbol | SQL function | Phase |
|---|---|---|
| `rp_collect_index_stats` | `collect_index_stats()` | 1-A |
| `rp_collect_table_stats` | `collect_table_stats()` | 1-B |
| `rp_collect_workload_stats` | `collect_workload_stats()` | 1-C |
| `rp_analyze_and_score` | `analyze_and_score()` | 2 |
| `rp_generate_publication_ddl` | `generate_publication_ddl()` | 3 |
| `rp_run_full_plan` | `run_full_plan()` | pipeline |

All C functions use `SPI_execute` internally and return rows via
`tuplestore_puttuple` (the standard PG materialise-mode SRF pattern).

### Scoring formula

```
final_score = 0.30 × query_freq_score
            + 0.40 × index_heat_score
            + 0.30 × table_elig_score

REPLICATE_ALL      if final_score ≥ 0.50 AND table_elig_score ≥ 0.2
REPLICATE_FILTERED if final_score ≥ 0.20 AND table_elig_score ≥ 0.2
SKIP               otherwise
```

---

## Versioning / Upgrade

```sql
-- Upgrade from 1.0 to 1.1 (once replication_planner--1.0--1.1.sql exists)
ALTER EXTENSION replication_planner UPDATE TO '1.1';
```

---

## License

MIT — see LICENSE file.
