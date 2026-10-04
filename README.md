# ECOMM clickstream exam on Databricks

A data-engineering exam, *E-Commerce Clickstream & Dynamic Pricing Elasticity*, built end to end
on **Databricks Free Edition** with **Delta Lake**, packaged as a Declarative Automation Bundle.
The brief was written for EMR, Iceberg and Redshift. This is the Databricks-native version:
Lakeflow jobs and pipelines, Unity Catalog, serverless compute and Databricks SQL.

The brief's three questions:

| | Question | Answered by |
|---|---|---|
| Q1 | What happens to conversion in the 2 hours after a price drop of more than 15%? | `gold.price_elasticity` |
| Q2 | Which sessions abandoned a cart within 30 minutes of a price increase, and what revenue was lost? | `gold.cart_abandonment` |
| Q3 | Can we still query Black Friday's numbers months later, while the tables keep changing? | `audit.*` deep clone + retention policy |

**Start here:**
- [docs/architecture.md](docs/architecture.md): data flow, jobs, Unity Catalog layout, code layout
- [docs/decisions.md](docs/decisions.md): every design decision, with its context and cost

## Project layout

```
.
├── databricks.yml        bundle: `catalog` and `warehouse_id` variables, `dev` target
├── resources/jobs/       one YAML per Lakeflow job (10 jobs)
├── resources/pipelines/  the bronze Lakeflow pipeline
├── src/ecomm/            the library: settings, naming, data model, history policy, transforms, …
├── src/pipelines/        bronze pipeline source (Auto Loader + expectations)
├── src/notebooks/        one folder per layer; thin notebooks that call the library
├── src/sql/reports/      Q1, Q2, Q3 queries for the SQL editor
├── tests/unit/           pytest on local Spark: every rule the brief asks you to defend
├── tests/parity/         harness → silver → gold on the full data, vs an earlier AWS run
└── docs/                 architecture and the decision log (22 decisions)
```

## Workflow

| Step | Command |
|---|---|
| Test and lint the library locally | `uv run pytest` · `uv run ruff check .` |
| Validate the bundle | `databricks bundle validate --strict` |
| Deploy to the workspace | `databricks bundle deploy` |
| Run end to end | `databricks bundle run ecomm_99_orchestrator` |
| Inspect the evidence | query `ops.measurements` (see *See the results*) |

## One-time setup

1. Install the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) and log it
   in to your workspace. This opens a browser for OAuth:

   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```

   The bundle uses the CLI's default profile. To use another, add `--profile <name>` to the bundle
   commands. A deploy started from a Databricks Git folder uses the workspace it runs in.

2. For local development, install [uv](https://docs.astral.sh/uv/) and Java 17+ (for local Spark).

## Develop (local)

```bash
uv run pytest
```

```bash
uv run ruff check .
```

```bash
databricks bundle validate --strict
```

The full-data parity check runs harness → silver → gold on the five Kaggle CSVs in about 30
seconds. It should match the earlier AWS run, except for 26 price intervals
([D-06](docs/decisions.md#d-06--compare-prices-in-integer-cents)) and the effects that follow from them:

```bash
ECOMM_SOURCE_DIR=/path/to/ecommerce-events-history-in-cosmetics-shop uv run python tests/parity/local_parity.py
```

## Deploy and run

```bash
databricks bundle deploy
```

**First deploy into a new workspace.** A Lakeflow pipeline can only be created once its target
catalog exists, and the catalog is created by the `setup` task
([D-23](docs/decisions.md#d-23--first-deploy-creates-unity-catalog-objects-before-the-pipeline)).
The first deploy creates every job but stops at the pipeline. Run the `setup` task on its own,
then deploy again:

```bash
databricks bundle run ecomm_00_setup --only setup
```

```bash
databricks bundle deploy
```

**1 · The end-to-end build.** It runs every layer in order and fails if a required acceptance
criterion is not met:

```bash
databricks bundle run ecomm_99_orchestrator
```

In development mode the jobs appear as `[dev <you>] ecomm_…` under *Jobs & Pipelines*.
The orchestrator run shows one task per layer job; click a task to open that job's run and
its notebook output.

**2 · The step 5 evolution demo.** Manual, once silver exists:

```bash
databricks bundle run ecomm_35_evolution
```

**3 · The Q3 audit.** Manual, once gold exists. It runs DML against the live gold table on
purpose, then restores it:

```bash
databricks bundle run ecomm_60_audit
```

**Re-run one layer**, for example after fixing it:

```bash
databricks bundle run ecomm_40_gold
```

### Jobs

| Job | Tasks | Produces | Runs |
|---|---|---|---|
| `ecomm_00_setup` | `setup` → `land_source` | catalog, schemas, landing volume, `ops.measurements`; the 5 CSVs | orchestrator |
| `ecomm_10_harness` | `harness_clickstream` → `harness_price_catalog` | ~4,032 JSON micro-batches; the price-interval feed (A1, A2) | orchestrator |
| `ecomm_20_bronze` | `ingest` → `verify_bronze` | `bronze.clickstream`, `bronze.price_catalog` via the Lakeflow pipeline | orchestrator |
| `ecomm_30_silver` | `sessionize` ∥ `price_intervals` → `temporal_join` | sessions, price intervals, price-bound `silver.clickstream` (A3–A5) | orchestrator |
| `ecomm_40_gold` | `conversion_hourly` ∥ `q1_elasticity` ∥ `q2_abandonment` | the funnel, Q1 and Q2 tables (A9, A10) | orchestrator |
| `ecomm_70_serving` | `statistics` ∥ `materialized_view` → `three_way` | stats, `gold.mv_category_daily`, the benchmark (A12) | orchestrator |
| `ecomm_50_maintenance` | `optimize` ∥ `history_guard` → `acceptance_checks` | OPTIMIZE evidence (A8), the retention guard, the verdict | orchestrator (last) |
| `ecomm_35_evolution` | `evolve` | schema and clustering evolution proofs (A6, A7) | manual |
| `ecomm_60_audit` | `freeze` → `immutability_proof` → `retention_check` | `audit.product_conversion_hourly_black_friday_2026_final` (A11) | manual |
| `ecomm_99_orchestrator` | one `run_job_task` per orchestrated job | one end-to-end run | you |

### If a task fails

| Task | Likely cause | Fix |
|---|---|---|
| `setup` | the workspace refused `CREATE CATALOG` | `databricks bundle deploy --var catalog=workspace`, then run again |
| `land_source` | Kaggle is blocked by Free Edition's outbound-internet restriction | upload the 5 CSVs to `/Volumes/exam_ecommerce/raw/landing/source/cosmetics/` in Catalog Explorer, then run again |
| `ingest` | a pipeline error | open the `ecomm_bronze` pipeline in *Jobs & Pipelines*; its event log names the failing table or expectation |
| `temporal_join` | the three join strategies disagree, or `price_match` < 99% | the task stops before writing silver; the evidence shows which strategy and how many clicks |
| `materialized_view` | the warehouse couldn't create the view | check that "Serverless Starter Warehouse" exists (the bundle looks it up by that name) |
| `acceptance_checks` | a required criterion is not PASS | its output table lists each criterion, its status and the notebook that proves it |

To copy CSVs you already uploaded to another volume folder:

```bash
databricks bundle run ecomm_99_orchestrator --params source_copy_from=/Volumes/<catalog>/<schema>/<volume>/<folder>
```

The harness is replayed only once: re-runs skip it when its output exists
([D-05](docs/decisions.md#d-05--keep-the-micro-batch-harness-and-make-re-runs-safe)).

## See the results

**The evidence pack.** Every measurement from one end-to-end run:

```sql
SELECT step, metric, value, note
FROM exam_ecommerce.ops.measurements
WHERE orchestration_run_id = (SELECT max(orchestration_run_id) FROM exam_ecommerce.ops.measurements)
ORDER BY recorded_at
```

**The answers.** Open `src/sql/reports/q1_elasticity.sql`, `q2_abandonment.sql` and
`q3_black_friday_audit.sql` in the SQL editor, from the bundle's folder in the workspace. They
take the catalog, and for Q1 the volume floor, as named parameters.

**The data.** *Catalog → exam_ecommerce*: `raw.landing` holds the files, and each medallion
schema holds its tables. A table's *History* tab shows every version this project wrote.
