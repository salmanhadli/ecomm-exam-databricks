# Results

What the first end-to-end run on Databricks Free Edition measured. Every number here was
recorded by the run itself in `exam_ecommerce.ops.measurements`, or read from
`system.query.history`. Nothing is copied from a notebook by hand.

| Run | Job | Outcome |
|---|---|---|
| `173130120921254` | `ecomm_99_orchestrator` | green after two repairs (see the [break log](#break-log)) |
| `508687608649418` | `ecomm_35_evolution` | succeeded first time |
| `320400503467415` | `ecomm_60_audit` | succeeded first time |

Run on 2026-10-04 (UTC). The workspace numbers match the local parity run
(`tests/parity/local_parity.py`) exactly, layer by layer.

## Acceptance: 12 of 12

| # | Criterion | Status | Evidence |
|---|---|---|---|
| A1 | Two independent feeds | PASS | 4,032 clickstream micro-batch files; a separate Parquet price-interval feed |
| A2 | ≥ 200 products with a > 15% change | PASS | 991 products |
| A3 | Own sessions, disagreement quantified | PASS | 321,828 sessions; 12.16% merge several raw `user_session` ids |
| A4 | ≥ 2 temporal-join optimisations compared | PASS | 3 exact strategies, 0 disagreements |
| A5 | `price_match` reported and explained | PASS | 99.999% exact, 100% within one cent |
| A6 | Schema evolution rewrites no files | PASS | 1 file before and after; all existing rows read NULL in the new columns |
| A7 | Clustering (partition) evolution rewrites no files | PASS | keys changed, 1 file before and after, identical query results |
| A8 | Compaction measured | PASS | OPTIMIZE ran; nothing to compact (see [findings](#where-databricks-differed-from-the-briefs-expectations)) |
| A9 | Q1: windows normalised, floor stated, confounders named | PASS | floors 1 / 5 / 10 / 30 reported; six confounders |
| A10 | Q2: both revenue definitions, with a recommendation | PASS | $495.53 at cart price (headline) vs $357.67 at the old price |
| A11 | Q3: frozen copy, four-reading proof, retention verified | PASS | frozen copy unchanged after DML; 0 files deletable |
| A12 | Serving: statistics, materialized view, three-way comparison | PASS | see [Serving](#serving) |

## Evidence pack

The brief's 20 items, with the Databricks counterpart where the brief names an AWS service.

| # | Item | Value |
|---|---|---|
| 1 | Compute | Databricks Free Edition, serverless jobs; orchestrator run `173130120921254` |
| 2 | Dataset; storage | Cosmetics variant; Unity Catalog managed Delta tables and a UC volume (not S3 Tables) |
| 3 | Micro-batches; simulated days | 4,032 JSON files, one per 5-minute interval; 14 days, 2019-10-01 00:00:00 → 2019-10-14 23:59:57 UTC |
| 4 | Price intervals; products changing > 15% | 39,775 intervals (38,343 products); 991 products |
| 5 | `category_code` NULL %; largest category share | 98.41% NULL. Largest `category_id`: 4.61% of events. `category_code` NULL: 97.76% of views |
| 6 | Sessions; median events per session; disagreement | 321,828; 2; 39,124 own sessions (12.16%) merge several raw ids, 17,152 raw ids split across own sessions |
| 7 | Sessionization skew | 12,863 vs 9,141 rows per task (max vs median), **1.41×** |
| 8 | Naive range join | not executed, by design ([D-02](decisions.md#d-02--build-the-solution-directly)) |
| 9 | Optimised joins | see [Temporal join](#temporal-join) |
| 10 | `price_match` rate | 99.999% exact; 100.000% within one cent; 2,385 clicks with no catalog price |
| 11 | Files before/after `ADD COLUMN` | 1 → 1 data file; Delta records version 3 with no files added or removed |
| 12 | Files before/after the clustering change | 1 → 1 |
| 13 | Partition specs | Delta has none; clustering keys `event_time, user_id` → `event_time, user_id, product_id` |
| 14 | Files and size before/after compaction | every silver and gold table: 1 → 1 file (6.8 MB to 56.7 MB); no manifests in Delta |
| 15 | Q1 qualifying; median elasticity; floor | floor 5: 13 of 832 price drops, median +4.938 (an artefact, see below); floor 30: 0 |
| 16 | Q2 abandoned sessions; lost revenue; top 3 | 58 sessions, 82 carts; $495.53 vs $357.67; top 3 by `category_id` below |
| 17 | Frozen reference | `audit.product_conversion_hourly_black_friday_2026_final`, cloned from gold version 2 (committed 2026-10-04 22:05:03 UTC), 365-day retention |
| 18 | Immutability proof | four readings below |
| 19 | Retention health check | `VACUUM … DRY RUN`: 0 deletable files; both tables keep 365 days; time travel reaches version 0 |
| 20 | Three serving paths | see [Serving](#serving) |

## Layer by layer

### Harness and bronze

| | |
|---|---:|
| Events replayed | 1,960,971 |
| Micro-batch files landed (one per 5-minute interval) | 4,032 |
| Price intervals: initial / increase / decrease | 38,343 / 424 / 1,008 |
| Events excluded from the price catalog (price ≤ 0 or NULL) | 2,390 |
| Rows in `bronze.clickstream` | 1,960,971 |
| **Delta files in `bronze.clickstream`** | **2** (17 MB average), from 4,032 input files |
| Rows rescued by Auto Loader | 0 |

The pipeline's expectations, read from its event log. Bronze keeps every row; these rules warn
([D-15](decisions.md#d-15--bronze-is-lossless-expectations-warn-silver-filters)):

| Expectation | Failed rows |
|---|---:|
| `category_code_present` | 1,929,785 (98.41%) |
| `brand_present` | 801,841 (40.89%) |
| `positive_price` | 2,390 (0.12%) |
| `user_session_present` | 183 (0.01%) |
| `event_time_present`, `product_present`, `user_present`, `known_event_type`, `nothing_rescued` | 0 |
| `file_in_micro_batch_layout` (fails the update) | 0 |

### Silver

| | |
|---|---:|
| Rows after dedup | 1,855,165 (105,806 duplicates removed, 5.40%) |
| Sessions | 321,828 (median 2 events, mean 5.76, max 681) |
| Worked example | raw `user_session` `d237750b-…` spans **78** of our sessions |
| Events per user | median 3, max 2,023 |

### Temporal join

The price-interval table is 368 KB, well under the 64 MB broadcast guard. All three strategies
bound the same price to every one of the 1,855,165 clicks (0 disagreements). Time and bytes
read come from `system.query.history`, per statement.

| Strategy | Operators in the plan | Time | Bytes read |
|---|---|---:|---:|
| A · broadcast (production) | `PhotonBroadcastHashJoin` | 1.6 s | 68.9 MB |
| D · `RANGE_JOIN` hint | `PhotonBroadcastHashJoin` (same plan) | 1.4 s | 68.9 MB |
| C · as-of window | no join: a window over each product | 3.4 s | 59.0 MB |

Each plan also shows a `PhotonBroadcastNestedLoopJoin`. That is the one-row cross join that
closes the open intervals, not the price lookup. `system.query.history` reports 0 shuffle bytes
for all three, including the as-of window, which must shuffle. That column isn't reliable for
serverless notebook queries, so no shuffle figures are claimed.

### Gold

| | |
|---|---:|
| `gold.product_conversion_hourly` rows | 952,322 |
| Views / carts / purchases | 863,384 / 617,684 / 110,085 |
| Revenue | 551,158.40 |
| Rows with a NULL rate | 0 |

**Q1: elasticity after a price drop of more than 15%.** There are 1,059 changes above 15%
(832 drops and 227 increases) across 991 products; 1,058 have at least an hour after them.

| Floor (baseline purchases) | Drops qualifying | Median elasticity | Drops with no purchase in the 2 h after |
|---:|---:|---:|---:|
| 1 | 128 | +3.340 | 89.8% |
| **5 (headline)** | **13** | **+4.938** | **76.9%** |
| 10 | 3 | +4.938 | 100% |
| 30 (the brief's example) | 0 | — | — |

**Q2: carts abandoned within 30 minutes of an increase.** There were 227 increases above 15%.
93 carts followed one within 30 minutes, and 82 of those were never bought, across 58 sessions
and 30 categories.

| `category_id` | Sessions | Carts | Lost at cart price | Lost at old price | Gap |
|---|---:|---:|---:|---:|---:|
| 1487580004857414477 | 17 | 27 | 157.57 | 101.17 | 56.40 |
| 1542195323827388674 | 1 | 2 | 78.26 | 54.76 | 23.50 |
| 1487580012642042013 | 3 | 7 | 33.79 | 27.70 | 6.09 |
| **All 30 categories** | **58** | **82** | **495.53** | **357.67** | **137.86** |

None of the 30 categories has a `category_code`, so they can only be named by id.

### Q3 audit

The frozen copy is a DEEP CLONE of gold version 2. The live table then received an UPDATE
(42,580 rows), a DELETE (69,572 rows, the first day) and an INSERT (one sentinel row).

| Reading | Rows | Purchases | Revenue |
|---|---:|---:|---:|
| 1 · frozen copy | 952,322 | 110,085 | 551,158.40 |
| 3 · frozen copy, after the DML | 952,322 | 110,085 | 551,158.40 |
| 4 · live table, after the DML | 882,751 | 40,162,618 | 1,507,686.46 |

Time travel to version 2 on the live table equals reading 1. `RESTORE` then returned the live
table to version 2's data, committed as version 7.

### Evolution

| Version of `silver.clickstream` | Operation | Files added / removed |
|---:|---|---|
| 3 | `ADD COLUMNS` (`promotional_tag`, `price_change_pct AFTER catalog_price`) | none |
| 4 | `RENAME COLUMN category_code TO category_path` | none |
| 5 | `RENAME COLUMN` back | none |
| 6 | `CLUSTER BY` adding `product_id` | none |

All 1,855,165 existing rows read NULL in the new columns. The same 30,116 non-null category
values read under the new name.

### Serving

The same question, conversion and revenue per category and day, answered three ways. All
measurements are from `system.query.history` and the warehouse's query history, with the result
cache ruled out (see finding 7).

| Path | Time | Bytes read | Rows read |
|---|---:|---:|---:|
| Serverless Spark (notebook), gold table | 0.7 s | 3.48 MB | 952,322 |
| SQL warehouse, gold table directly | 0.7 s | 3.48 MB | 952,322 |
| SQL warehouse, materialized view | 1.3 s | **0.17 MB** | **6,191** |

The materialized view `gold.mv_category_daily` holds 6,191 rows, one per category and day. It
reads **95% less data** than aggregating gold, but at a million rows it is not faster: the
warehouse's fixed per-query cost dominates, and times vary run to run (0.7–2.1 s for the direct
query across this session). Bytes read is the comparison that scales. The view's refresh after
its first build was planned as `NO_OP`, because gold had not changed.

**Behind a dashboard refreshing every five minutes,** use the materialized view. Each dashboard
query then reads 6,191 precomputed rows instead of re-aggregating gold. The cost moves to one
refresh per pipeline run (12.9 s here), and staleness is bounded by the orchestrator's cadence
([D-21](decisions.md#d-21--serving-sql-runs-on-the-warehouse-through-the-sdk-the-mv-refreshes-per-run)).

## Where Databricks differed from the brief's expectations

1. **The small-file problem never reached a table.** The brief predicts many small files and a
   compaction to fix them. Auto Loader turned 4,032 input files into 2 bronze files. Every silver
   and gold table is a single 7–57 MB file: Unity Catalog managed tables get optimized writes, and
   gold's history shows an automatic `OPTIMIZE` commit after each write (versions 2 and 6). The
   maintenance job's `OPTIMIZE` had nothing left to do, and a one-day probe read the same single
   file before and after. Compaction only becomes measurable at far larger volumes, or with many
   small appends.
2. **The `RANGE_JOIN` hint changed nothing.** The broadcast and range-join variants produced the
   same Photon plan. With a 368 KB interval table, Photon broadcasts anyway. The data-driven bin
   size was also unhelpful: most intervals are open-ended and span the whole window, so the median
   interval is 1,219,931 s (14 days).
3. **The skewed key was real, and salting still didn't pay.** On `category_code`, the NULL key put
   937,605 rows in one task against a median of 1,259: **745×**, the brief's skew scenario. The
   salted rollup took 0.96 s and the plain one 0.90 s, with identical results. The aggregation
   combines rows before the shuffle, so the hot task stays small. Sessionization, by contrast,
   showed no skew (1.41×).
4. **Statistics existed before `ANALYZE` ran.** The optimizer already had the row count (952K)
   and per-column distinct counts. Databricks collected them on write, so `ANALYZE` added nothing.
5. **Q1's headline median is an artefact, not demand.** At floor 5, 76.9% of the qualifying drops
   had no purchase at all in the 2 hours after. Then the purchase change is −100% and elasticity is
   exactly −1 ÷ the price change, a positive number for a drop. That is why only 23.1% show the
   sign theory predicts. The brief's floor of 30 leaves no product to measure.
6. **Delta versions every schema change.** Iceberg's proof is "no new snapshot". On Delta, each
   `ADD COLUMNS`, `RENAME` and `CLUSTER BY` is a new version, so the proof is that no data file
   was added or removed.
7. **The warehouse result cache can impersonate a benchmark.** The first run's "warehouse, direct"
   time was the cache: `system.query.history` showed `from_result_cache = true` and 0 bytes read,
   because a retried task had re-sent the same query. Tagging each query with a unique SQL comment
   did not help, as the cache ignores comments (measured: still `from_result_cache = true`). A
   run-unique literal in a predicate, `WHERE '<run id>' IS NOT NULL`, did: both queries then read
   their real bytes. The benchmark now uses that.
8. **Time zones at the driver.** The local parity run printed the replay window as 04:00 → 03:59,
   while the workspace recorded 00:00 → 23:59 UTC. These are the same instants: collecting a
   timestamp into Python renders it in the machine's zone (UTC+4 on the laptop). This is why the
   harness computes its replay bound inside Spark.

## Break log

The brief asks for three genuine failures, with real error text, a diagnosis and a fix. One must
come from the temporal join, and one from partition evolution or tagging. Evolution and tagging
both passed first time, so that last requirement has no entry.

**1 · First deploy: `CATALOG_DOES_NOT_EXIST`**

```
Error: cannot create resources.pipelines.ecomm_bronze: Catalog 'exam_ecommerce' does not exist. (404 CATALOG_DOES_NOT_EXIST)
```

*Diagnosis.* A Lakeflow pipeline's target catalog must exist when the pipeline is created. Here
the catalog is created by the `setup` task at run time, on purpose, so that no deploy can drop
data (D-04). *Fix.* Bootstrap once: deploy, run `ecomm_00_setup --only setup`, deploy again
([D-23](decisions.md#d-23--first-deploy-creates-unity-catalog-objects-before-the-pipeline)).

**2 · Temporal join: `ARITHMETIC_OVERFLOW`**

```
ArithmeticException: [ARITHMETIC_OVERFLOW] overflow. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error. SQLSTATE: 22003
```

*Diagnosis.* All three join strategies failed at the same line: the timing helper summed a 64-bit
hash of every row to force full computation. Over 1.85M rows the sum overflows BIGINT, and
Spark 4's ANSI mode raises instead of wrapping. *Fix.* Combine the hashes with `bit_xor`, which
cannot overflow; regression test in `tests/unit/test_perf.py`. Repaired run: 3 strategies, 0
disagreements.

**3 · Serving: `'ListQueriesResponse' object is not iterable`**

```
TypeError: 'ListQueriesResponse' object is not iterable
  File .../src/ecomm/warehouse.py:61, in Warehouse.metrics
```

*Diagnosis.* The Databricks SDK on serverless returns a response object with the records in
`res`, not an iterator. *Fix.* Accept both shapes (`ecomm.warehouse._queries`); tests in
`tests/unit/test_warehouse.py`.

## Reproduce

All evidence from one end-to-end run:

```sql
SELECT step, metric, value, note
FROM exam_ecommerce.ops.measurements
WHERE orchestration_run_id = '173130120921254'
ORDER BY recorded_at
```

The warehouse's own record of the serving benchmark:

```sql
SELECT statement_text, total_duration_ms, read_bytes, from_result_cache
FROM system.query.history
WHERE statement_text LIKE '%ecomm three_way run%'
ORDER BY start_time DESC
```

`system.query.history` updates in batches, typically a few minutes behind; *Query History* in the
SQL workspace shows the same queries immediately.
