The short answer: with your current scoring formula, any "eligible" table is mathematically almost guaranteed to land in REPLICATE_ALL. The
REPLICATE_FILTERED band is effectively dead code. Let me show you the math from your own code.

The decision logic

In rp_generate_publication_ddl (and identically in rp_analyze_and_score):

final_score = 0.30*qf + 0.40*ih + 0.30*te
te < 0.2                  -> SKIP
final_score >= 0.50       -> REPLICATE_ALL
final_score >= 0.20       -> REPLICATE_FILTERED
else                      -> SKIP

And te (table_elig_score) is only ever one of three values:

WHEN NOT has_pk         THEN 0.1
WHEN write_ratio > 0.90 THEN 0.1
WHEN row_estimate < 100 THEN 0.3
ELSE 1.0

Why you always get REPLICATE_ALL

Your test_orders and test_users both have a primary key, are read-heavy (your test runs 3000 read loops, no writes), and have >100 rows. So for both:

te = 1.0   →   contributes 0.30 * 1.0 = 0.30 to final_score, a fixed floor

That floor alone is 0.30. To get REPLICATE_FILTERED instead of REPLICATE_ALL, you'd need:

final_score < 0.50
0.30*qf + 0.40*ih + 0.30 < 0.50
0.30*qf + 0.40*ih < 0.20

Now look at ih (index_heat_score). It is not an absolute heat measure — it's max_scan_share, the dominant index's share of scans within that same table:

CASE WHEN total_idx_scan = 0 THEN 0.0
ELSE idx_scan / total_idx_scan END   -- per-table normalized

For any table that is actually queried, this is large by construction:
- test_users — one index (the unique email index) does all the scans → ih ≈ 1.0 → 0.40*ih = 0.40. Already 0.40 + 0.30 = 0.70 ≥ 0.50 before qf even
  counts.
- test_orders — scans split across idx_orders_region / idx_orders_created_at, so the top index gets ih ≈ 0.5 → 0.40*0.5 = 0.20, plus te 0.30 = 0.50, plus
  the qf term pushes it over.

So the inequality 0.30*qf + 0.40*ih < 0.20 essentially cannot be satisfied for an eligible table, because a dominant index's share is almost always ≥
0.5. That's why every eligible table is REPLICATE_ALL.

Meanwhile the only table that could be filtered (test_tmp_junk, tiny → te=0.3) has no PK, so it falls into te=0.1 → SKIP. You never even see it.

The root cause in one sentence

te=1.0 injects a fixed 0.30 floor, and ih is a per-table normalized share that is near 1.0 for any queried table — so the base score for any eligible
table is ~0.70, far above the 0.50 threshold. The [0.20, 0.50) filtered window is unreachable for normal tables.

What you can do

The fix depends on what you actually want REPLICATE_FILTERED to mean. A few options:

1. Raise the REPLICATE_ALL threshold (e.g. to 0.85) so only truly hot tables qualify and most eligible tables fall into the filtered band. Cheapest
   change, but somewhat arbitrary.
2. Make ih an absolute, cross-table heat score instead of a within-table share. Right now even a barely-used table scores ih≈1.0 if it has one index.
   Normalize index scans against the busiest table in the system, so cold-but-eligible tables get a low ih and land in REPLICATE_FILTERED.
3. Decouple the filter decision from the score. Use the score for SKIP vs replicate, then choose ALL vs FILTERED based on a concrete signal — e.g. table
   size (big tables → filtered to cut volume) or whether a selective indexed column exists.

Want me to make this an explicit choice and implement it? If so, tell me what REPLICATE_FILTERED should capture in your thesis model (e.g. "large but
warm tables" vs "tables with a selective time/region column"), and I'll recalibrate the weights/thresholds and update both the C scoring path and the
test expectations.
