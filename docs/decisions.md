# Decision log

The brief says *"Ambiguity is deliberate. Decide, document the assumption, defend it."*
This file records each decision in one place: the context, what was decided, and what it
costs. Numbers come from runs; where the AWS build measured something first, it is cited.

| # | Decision | Status |
|---|---|---|
| D-01 | Delta Lake, not Iceberg | applied |
| D-02 | Build the solution directly, skip the brief's deliberate mistakes | applied |
| D-03 | One job per layer, chained by an orchestrator | applied |
| D-04 | Storage objects are created by a notebook, not by the bundle | applied |
| D-05 | Keep the micro-batch harness, and make re-runs safe | applied |
| D-06 | Compare prices in integer cents | applied |
| D-07 | Every window has a deterministic sort order | applied |
| D-08 | The newest price interval stays open-ended (`NULL`) | applied |
| D-09 | Never shorten table history | applied |
| D-10 | The evidence pack is a table | applied |
| D-11 | Logic in a library, notebooks stay thin | applied |
| D-12 | Cosmetics dataset variant | applied |
| D-13 | Group Q2 by `category_id`, not `category_code` | applied |
| D-14 | Broadcast join as the production temporal join | applied |
| D-15 | Bronze is lossless: expectations warn, silver filters | applied |
| D-16 | `_batch_id` is the source micro-batch, read from the file path | applied |
| D-17 | Performance evidence without a Spark UI | applied |
| D-18 | Tables are declared once and overwritten in place | applied |
| D-19 | Q1 windows at event precision, normalised by their real length | applied |
| D-20 | Q3 is a deep clone that is never replaced, plus retention | applied |
| D-21 | Serving SQL runs on the warehouse through the SDK; the MV refreshes per run | applied |
| D-22 | Acceptance criteria are data, and the last task enforces them | applied |
| D-23 | First deploy creates Unity Catalog objects before the pipeline | applied |
| D-24 | External callers start the orchestrator as a service principal | applied |

---

### D-01 · Delta Lake, not Iceberg

**Context.** The brief is written for Iceberg on EMR. This build targets Databricks, and
the goal is to learn the platform.
**Decision.** Use Delta Lake throughout. Every Iceberg mechanism the brief tests has a
Delta counterpart:

| Brief (Iceberg) | Delta on Databricks |
|---|---|
| `hours(ts)` + `bucket(32, user_id)` partitioning, `WRITE ORDERED BY` | liquid clustering `CLUSTER BY (event_time, user_id)` |
| `ADD PARTITION FIELD` (partition evolution) | `ALTER TABLE … CLUSTER BY (…)`, which rewrites no existing files |
| rename via column IDs | column mapping (`delta.columnMapping.mode = 'name'`) |
| `rewrite_data_files` / `rewrite_manifests` | `OPTIMIZE`, predictive optimization |
| tag `RETAIN 365 DAYS` + `expire_snapshots` | `DEEP CLONE … VERSION AS OF n` + retention properties (D-09) |
| `.files` / `.snapshots` metadata tables | `DESCRIBE DETAIL` / `DESCRIBE HISTORY` |
| Redshift Spectrum / MV | SQL warehouse / Databricks SQL materialized view |

**Consequence.** Delta has no partition specs and no named snapshot tags. The proofs for
steps 5 and 7 use the Delta equivalents above, and say so.

### D-02 · Build the solution directly

**Context.** The brief has you make some mistakes on purpose (run the naive range join,
tag without retention) so you can measure the fix. The AWS build already did that.
**Decision.** Build the correct version first:
- no naive range join is executed
- retention is set when each table is created
- `.cache()` is not used, because it raises on serverless

**Kept on purpose:** the micro-batch harness (D-05). It is the input the platform really
receives, not a mistake.

### D-03 · One job per layer, chained by an orchestrator

**Context.** One long job hides the layer boundaries, and a failure means re-running everything.
**Decision.** Use jobs `ecomm_00_setup`, `ecomm_10_harness`, and so on. `ecomm_99_orchestrator`
runs them in order with `run_job_task` and passes its run id down as `orchestration_run_id`.
The evolution demo and the audit are manual-only jobs, because the audit deliberately runs
DML against a live table.
**Consequence.** Any layer can be re-run alone. The evidence rows from one end-to-end run
can be joined on `orchestration_run_id`.

### D-04 · Storage objects are created by a notebook, not by the bundle

**Context.** Bundles can declare schemas and volumes, but `bundle destroy` then drops them,
along with every table inside.
**Decision.** The notebook `01_create_uc_objects` creates the catalog, schemas, volume and
evidence table idempotently. The bundle only declares jobs, pipelines and dashboards.
**Consequence.** No deployment command can delete data.

### D-05 · Keep the micro-batch harness, and make re-runs safe

**Context.** The brief requires one JSON file per 5-minute interval, over at least 14 days.
Spark names output files with a new UUID on every write, and Auto Loader tracks input files
by path.
**Decision.** Replay once. When the landing folder is already populated, the harness skips
(`replay_mode=skip_if_present`).
**Consequence.** A re-run cannot make Auto Loader see ~4,000 "new" files and double bronze.
The replay can still be forced with `replay_mode=overwrite`, before bronze exists.

### D-06 · Compare prices in integer cents

**Context.** The brief: *"a 0.01 difference is rounding, not a repricing."* The AWS build
compared DOUBLE prices against `0.01`. `100.00 - 99.99` evaluates to `0.010000000000005116`,
which is greater than `0.01`, while `10.00 - 9.99` lands just below it. Over every one-cent
step up to 199.99, 43.3% compare as greater than 0.01. The threshold was a coin flip.
**Decision.** Convert to integer cents first (`round(price * 100)`). A 1-cent step is noise,
2 cents or more is a change.
**Consequence (measured).** On the same data the build produces 39,775 intervals. The AWS
logic produces 39,801, so 26 spurious one-cent "repricings" disappear. Products with a
change above 15% (A2) are unchanged at 991. Covered by
`tests/unit/test_harness.py::test_one_cent_step_is_noise_…`.

### D-07 · Every window has a deterministic sort order

**Context.** Two events of one product can share a timestamp. `orderBy("event_time")`
alone leaves their order to chance, so run ids and prices can change between runs.
**Decision.**
- The price-run window orders by `(event_time, price_cents)`.
- The interval window orders by `(effective_start, run_id)`.
- The opening price is `min_by(price, struct(event_time, price))`.
- The silver dedup (phase 4) extends the brief's `(_ingested_at, _src_file)` tiebreak to a
  total order. On AWS, 267 groups were still tied without it.

**Consequence.** Same input gives the same output, byte for byte. Covered by
`test_equal_timestamps_resolve_the_same_way_whatever_the_input_order`.

### D-08 · The newest price interval stays open-ended

**Context.** The brief: close intervals with `NULL` or a far-future sentinel, and be consistent.
**Decision.** Use `NULL` in the feed. Each consumer closes it for its own purpose. For example,
the temporal join uses the last observed event time: exploding a year-2999 sentinel to an
hourly grid would generate billions of rows.
**Consequence.** All 38,343 products have exactly one open interval.

### D-09 · Never shorten table history

**Context.** The AWS build ran `expire_snapshots(retain_last=1)` after every write, and deleted
old metadata on each commit. No table could time-travel, and a later Redshift step failed
because the metadata it needed was gone. On Delta, the same habit would be
`VACUUM … RETAIN 0 HOURS` or a shortened retention property.
**Decision.** Keep history, and enforce that:

| Layer | Time-travel horizon | Why |
|---|---|---|
| bronze, silver | 30 days | rebuildable from the landing volume; 30 days covers debugging and re-processing |
| gold, audit, ops | 365 days | Q3 asks for numbers "months later" |

- `ecomm/history.py` is the only place retention is defined.
- Every table is created with both `delta.logRetentionDuration` and
  `delta.deletedFileRetentionDuration` set to the horizon, so the whole window stays readable.
- No code runs VACUUM with a RETAIN clause. Clean-up is left to predictive optimization, which
  honours these properties.
- `tests/unit/test_history_policy.py` scans all source and fails on any VACUUM … RETAIN,
  snapshot expiry, retention override or disabled retention check.
- `history.violations()` lets a job check live tables and fail if a table's history was shortened.

**Consequence.** More storage: files replaced by rewrites are kept for up to a year. At this
data size (a few GB) that is negligible.

### D-10 · The evidence pack is a table

**Context.** The brief ends with 20 evidence items. On AWS they were copied from job stdout.
**Decision.** Every notebook records measurements through `ecomm.evidence.Evidence` into
`ops.measurements`, tagged with the step, run id and orchestration run id.
**Consequence.** The evidence pack is one SQL query. It also feeds the dashboard in phase 8.

### D-11 · Logic in a library, notebooks stay thin

**Context.** Logic written inside notebook cells can't be unit-tested or reused.
**Decision.** Transformations live in `src/ecomm/transforms` as pure DataFrame functions.
Notebooks handle parameters, I/O and evidence. The same functions run on serverless and in
local pytest.
**Consequence.** `tests/unit` runs in about 7 seconds locally. `tests/parity` runs the harness
on the full data and matches the AWS counts, except for the 26 intervals D-06 removes.

### D-12 · Cosmetics dataset variant

**Context.** The brief allows the Electronics or Cosmetics variant, and says to state which.
**Decision.** Use Cosmetics. In Electronics, 0 of 53,453 products ever change price, so Q1
and Q2 would have nothing to measure.
**Consequence.** 991 products change price by more than 15% (A2 requires 200).
`category_code` is 98.41% NULL in this variant (D-13).

### D-13 · Group Q2 by `category_id`, not `category_code`

**Context.** `category_code` is NULL on 98.41% of rows. On AWS, bucketing it as `'unknown'`
collapsed Q2's per-category answer into a single row, so "top 3 categories" could not be answered.
**Decision.** Q2, the skew analysis and the serving view aggregate by `category_id`, which is
always present. `category_code` is shown as a label where known.
**Consequence (measured locally).** Q2's answer spans **30 categories** instead of one. The
totals are unchanged from AWS: 82 abandoned carts in 58 sessions, $495.53 at cart price vs
$357.67 at the old price.

### D-14 · Broadcast join as the production temporal join

**Context.** The price-interval table is about 40,000 rows. The brief asks for at least two
join optimisations, measured.
**Decision.** Three exact implementations, compared in `30_silver/03_temporal_join`:
- **broadcast**, the production path, used while the interval table is under a 64 MB guard
- the Databricks **`RANGE_JOIN`** hint, with the bin size set to the median interval length
- the **as-of** window, which carries the last price forward and has no join operator

The notebook refuses to write silver unless all three bind the same price to every click.
The naive join is not executed (D-02), and the hourly grid is dropped because it loses
sub-hour precision.
**Consequence (measured locally on the full 14 days).** The three agree on all 1,855,165
clicks. 2,385 clicks have no catalog price, exactly as on AWS. `price_match` is 99.999% exact
and 100% within the 1-cent noise band; the exact misses are the one-cent drifts D-06 treats
as noise. Runtimes and plans come from the workspace run.

### D-15 · Bronze is lossless: expectations warn, silver filters

**Context.** The brief's "known dirt": null `category_code`, `brand` and `user_session`, and
prices of 0. Bronze is the replayable record of what arrived.
**Decision.** In the bronze pipeline, every known-dirt rule is a **warn** expectation
(`ecomm/quality.py`): it is counted in the event log and the row is kept. Only a broken
contract with the harness **fails** an update: a file outside the `dt=/hh=/min5=` layout,
or a catalog row with no product. Silver decides what to filter, for example rows without
`event_time`, `user_id` or `product_id`.
**Consequence.** Nothing is lost silently. `verify_bronze` reads each rule's pass and fail
counts from the pipeline event log into the evidence pack.

### D-16 · `_batch_id` is the source micro-batch, read from the file path

**Context.** The brief's bronze has a `_batch_id` audit column. In a Lakeflow pipeline, the
update id is not available to row-level code.
**Decision.** `_batch_id` is the micro-batch the row arrived in, parsed from its
`dt=/hh=/min5=` folders (for example `2019-10-01T10:05`). A file outside that layout gets
NULL, and the fail expectation stops the update.
**Consequence.** Every row traces back to the 5-minute file that delivered it.

### D-17 · Performance evidence without a Spark UI

**Context.** Serverless has no Spark UI and no `_jdf`. The brief asks for pasted physical
plans and max-vs-median task durations.
**Decision.** `ecomm/perf.py` provides:
- plans from `df.explain(mode="formatted")`, captured as text
- operator names read from those plans, for example `PhotonBroadcastHashJoin`
- runtimes from an action that hashes every column, which `count()` would prune away
- skew as rows per shuffle partition when hashed on the window key

The query profile ("See performance") remains the place for per-operator detail.
**Consequence.** Every performance claim in the evidence pack has a number behind it, recorded
by the notebook that measured it.

### D-18 · Tables are declared once and overwritten in place

**Context.** `saveAsTable(mode="overwrite")` can replace a table's definition, dropping its
clustering keys, properties or evolved columns.
**Decision.** Every notebook-written table is a `TableSpec` in `ecomm/tables.py`: its columns,
comments, clustering and retention in one place. Writes go through `spec.overwrite()`, which
creates the table if it is missing, aligns columns by name (columns added later by step 5 are
written as NULL), and replaces the rows with `INSERT OVERWRITE`.
**Consequence.** Each run is one new version of the same table. The definition, the history
and any evolution survive re-runs.

### D-19 · Q1 windows at event precision, normalised by their real length

**Context.** The brief compares the 2 hours after a change with a 7-day baseline "at the
original price", and warns that a second change inside the baseline leaves the original price
undefined. The AWS build used hourly buckets. The change hour then mixed old-price and
new-price events, and every baseline was divided by 84 blocks, however long the original
price had actually been in force.
**Decision.** Both windows are cut at event precision from `silver.clickstream`:
- **Baseline:** the original price's own interval, clipped to the 7 days before the change.
  This is the rule for "changed twice".
- **Post:** the 2 hours after the change, cut short if the price changes again or the data ends.

Each window is normalised by its real length, in 2-hour blocks. A post window shorter than
1 hour is not measured. Every floor (1, 5, 10, 30) is reported, with 5 as the headline.
**Consequence (measured locally).** 1,059 changes above 15%, of which 1,058 are measurable.
Price drops that qualify: 128 at floor 1, 13 at floor 5, 3 at floor 10, 0 at floor 30.
The median at floor 5 is +4.938, and most post windows hold no purchase. That makes elasticity
exactly −1 ÷ price change, an artefact of the 2-hour window. The notebook measures this share
and says so in its honest paragraph, with six confounders.

### D-20 · Q3 is a deep clone that is never replaced, plus retention

**Context.** Q3 needs a named, immutable reference *and* a maintenance policy that won't delete
the data underneath it. Delta has no snapshot tags.
**Decision.**
- `audit.product_conversion_hourly_<audit_name>` is a `DEEP CLONE … VERSION AS OF n`. It is
  created once and never replaced, and carries its provenance (source, version, commit time)
  as table properties.
- Both it and the live table keep 365 days of history (D-09).
- The proof takes the brief's four readings, plus time travel to version *n*. It then puts the
  live table back with `RESTORE`, which is itself a new version.
- Retention is shown with `VACUUM … DRY RUN`, which lists files and deletes none.

**Why deep, not shallow.** A shallow clone points at the live table's files, so the frozen copy
would depend on the live table's clean-up. A deep clone owns its files.

### D-21 · Serving SQL runs on the warehouse through the SDK; the MV refreshes per run

**Context.** Materialized views are created by a SQL warehouse, and the three-way benchmark
has to time the warehouse. Bundle SQL tasks can't template the catalog name into a view
definition.
**Decision.** Serving notebooks build SQL with the same `Project` naming as everything else and
run it through the Databricks SDK's statement execution API (`ecomm/warehouse.py`). Timings and
bytes read come from the warehouse's own query history. The view is refreshed explicitly once per
orchestrated run: a `SCHEDULE` or `TRIGGER ON UPDATE` starts a background pipeline, and Free
Edition allows one active pipeline per type.
**Consequence.** The view's staleness is bounded by the orchestrator's cadence. Gold is rewritten
each run (D-18), which likely forces a full recompute; the event log records which technique ran.

### D-22 · Acceptance criteria are data, and the last task enforces them

**Context.** The brief's acceptance table is normally a checklist someone ticks by hand.
**Decision.** `ecomm/acceptance.py` maps each criterion to the notebook that proves it. That
notebook records `acceptance_<id>` as PASS or FAIL. The maintenance job's last task reads the
latest value of each and **fails the run** if a required criterion is missing or failed. The
criteria from the manual jobs (A6, A7, A11) are reported without failing the run.
**Consequence.** A green orchestrator run means every required criterion was met, and the proof
is in `ops.measurements`.

### D-23 · First deploy creates Unity Catalog objects before the pipeline

**Context.** Creating a Lakeflow pipeline fails with `CATALOG_DOES_NOT_EXIST` unless its target
catalog already exists. That is what the first `databricks bundle deploy` hit. Under D-04, the
catalog and schemas are created by the `setup` task at run time, not by the bundle.
**Decision.** Keep D-04 and bootstrap a new workspace in three steps:
1. Deploy. The jobs are created; the pipeline is not.
2. Run `ecomm_00_setup --only setup`.
3. Deploy again.

After that, every deploy is a single step.
**Consequence.** No deployment command can ever drop the catalog. The cost is a two-pass first
deploy, written down in the README.

### D-24 · External callers start the orchestrator as a service principal

**Context.** A Workato recipe starts the end-to-end run through the Jobs REST API. A caller
outside the workspace can't use a person's browser login. A personal access token would act
with all of that person's rights.
**Decision.** The caller signs in as the `workato-trigger` service principal with an OAuth
secret (client credentials). The bundle grants it `CAN_MANAGE_RUN` on `ecomm_99_orchestrator`
only. The bundle finds the principal by display name (variable `trigger_principal`), so the
public repository holds no application ID. The run itself executes as the job owner, so the
principal needs no grant on the catalog or the warehouse.
**Consequence.** A leaked secret can start or cancel this one job and do nothing else. The
grant is code, so every deploy re-applies it. The cost is one manual step per workspace:
create the service principal before the first deploy, or `bundle validate` fails.
