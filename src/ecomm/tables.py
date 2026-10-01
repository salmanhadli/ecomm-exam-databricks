"""The data model: every managed Delta table this project writes, defined in one place.

Pipeline-owned tables (bronze) are declared in src/pipelines; everything a notebook writes
is declared here as a `TableSpec`, and notebooks create and fill tables only through it:

    spec.create(spark, project)              CREATE TABLE IF NOT EXISTS, with clustering,
                                             comment and the history retention policy
    spec.overwrite(spark, project, df)       replace the table's rows in one new version

Why `overwrite` is an INSERT OVERWRITE rather than `saveAsTable(mode="overwrite")`:
INSERT OVERWRITE replaces the *data* and keeps the table itself: its clustering keys,
properties, column comments and history. Rewriting the whole table on every run is
fine at this size, and each run is still a new, time-travelable version.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecomm.history import tblproperties_sql
from ecomm.project import Project

# Column mapping lets a column be renamed or dropped as a metadata-only change
# (step 5), the Delta counterpart of Iceberg's column IDs.
COLUMN_MAPPING = {"delta.columnMapping.mode": "name"}

# Row tracking lets a Databricks SQL materialized view built on the table refresh
# incrementally instead of recomputing (step 9).
ROW_TRACKING = {"delta.enableRowTracking": "true"}


@dataclass(frozen=True)
class TableSpec:
    layer: str
    name: str
    columns: str                    # SQL column list, with COMMENTs
    cluster_by: tuple[str, ...]
    comment: str
    properties: dict[str, str] = field(default_factory=dict)

    def fqn(self, project: Project) -> str:
        return project.table(self.layer, self.name)

    def ddl(self, project: Project) -> str:
        cluster = f"CLUSTER BY ({', '.join(self.cluster_by)})" if self.cluster_by else ""
        comment = self.comment.replace("'", "\\'")
        return (f"CREATE TABLE IF NOT EXISTS {self.fqn(project)} ({self.columns})\n"
                f"{cluster}\nCOMMENT '{comment}'\n{tblproperties_sql(self.layer, self.properties)}")

    def create(self, spark, project: Project) -> None:
        spark.sql(self.ddl(project))

    def overwrite(self, spark, project: Project, df: DataFrame) -> None:
        """Replace all rows with `df`, keeping the table's definition and history.

        Columns are matched by name. A column that exists in the table but not in `df`
        (for example one added later by the step 5 evolution demo) is written as NULL.
        """
        self.create(spark, project)
        target = spark.read.table(self.fqn(project)).schema
        aligned = df.select(*[
            F.col(f.name) if f.name in df.columns else F.lit(None).cast(f.dataType).alias(f.name)
            for f in target.fields])
        view = f"_overwrite_{self.layer}_{self.name}"
        aligned.createOrReplaceTempView(view)
        spark.sql(f"INSERT OVERWRITE TABLE {self.fqn(project)} SELECT * FROM {view}")
        spark.catalog.dropTempView(view)


# --- silver ------------------------------------------------------------------------------

SILVER_SESSIONS = TableSpec(
    layer="silver", name="clickstream_sessions",
    comment="Bronze clickstream after deduplication and 30-minute sessionization. "
            "Intermediate for silver.clickstream; materialised because serverless cannot cache.",
    cluster_by=("user_id", "event_time"),
    columns="""
        event_time    TIMESTAMP,
        event_type    STRING,
        product_id    STRING,
        category_id   STRING,
        category_code STRING,
        brand         STRING,
        price         DOUBLE    COMMENT 'Price as recorded on the event (raw)',
        user_id       STRING,
        user_session  STRING    COMMENT 'Raw session id: unreliable, kept for comparison only',
        session_key   STRING    COMMENT 'Own session: user_id#seq, 30 minutes of inactivity ends a session',
        session_seq   BIGINT,
        gap_s         BIGINT    COMMENT 'Seconds since the same user''s previous event',
        _src_file     STRING,
        _batch_id     STRING,
        _ingested_at  TIMESTAMP,
        _updated_at   TIMESTAMP""",
)

SILVER_PRICE_INTERVALS = TableSpec(
    layer="silver", name="price_intervals",
    comment="Price validity intervals per product: [effective_start, effective_end). "
            "The newest interval per product is open-ended (effective_end NULL).",
    cluster_by=("product_id", "effective_start"),
    columns="""
        product_id      STRING,
        price           DECIMAL(12,2),
        effective_start TIMESTAMP,
        effective_end   TIMESTAMP     COMMENT 'Exclusive; NULL = still current',
        change_reason   STRING        COMMENT 'initial | increase | decrease | no_net_change',
        pct_change      DOUBLE        COMMENT 'Change vs the previous interval, as a ratio (-0.2 = -20%)',
        _updated_at     TIMESTAMP""",
)

SILVER_CLICKSTREAM = TableSpec(
    layer="silver", name="clickstream",
    comment="Deduplicated, sessionized events bound to the catalog price the user actually saw.",
    # The brief's hours(event_time) + bucket(32, user_id), as liquid clustering (decision D-01).
    cluster_by=("event_time", "user_id"),
    properties=COLUMN_MAPPING,
    columns="""
        event_time      TIMESTAMP,
        event_type      STRING,
        product_id      STRING,
        category_id     STRING,
        category_code   STRING         COMMENT 'NULL on 98% of rows; gold groups by category_id',
        brand           STRING,
        price_at_event  DECIMAL(12,2)  COMMENT 'Price recorded on the event',
        catalog_price   DECIMAL(12,2)  COMMENT 'Catalog price valid at event_time (temporal join)',
        price_match     BOOLEAN        COMMENT 'price_at_event = catalog_price',
        user_id         STRING,
        session_key     STRING,
        _updated_at     TIMESTAMP""",
)


# --- gold ----------------------------------------------------------------------------------

GOLD_CONVERSION_HOURLY = TableSpec(
    layer="gold", name="product_conversion_hourly",
    comment="Funnel per product, hour and catalog price: views, carts, purchases, rates, revenue. "
            "Source of the category materialized view and the Q3 frozen copy.",
    cluster_by=("event_hour", "product_id"),
    properties=ROW_TRACKING,
    columns="""
        product_id        STRING,
        category_id       STRING,
        category_code     STRING,
        event_hour        TIMESTAMP,
        catalog_price     DECIMAL(12,2)  COMMENT 'Part of the grain: one row per price seen in the hour',
        views             BIGINT,
        carts             BIGINT,
        purchases         BIGINT,
        view_to_cart      DOUBLE         COMMENT '0.0 when there are no views, never NULL',
        cart_to_purchase  DOUBLE         COMMENT '0.0 when there are no carts, never NULL',
        overall_conv      DOUBLE,
        revenue           DECIMAL(18,2)  COMMENT 'Sum of price_at_event over purchases',
        computed_at       TIMESTAMP""",
)

GOLD_PRICE_ELASTICITY = TableSpec(
    layer="gold", name="price_elasticity",
    comment="Q1: one row per price change above 15%, with purchases and conversion in the "
            "baseline (original price) and post (2 hours) windows, normalised to 2-hour blocks.",
    cluster_by=("changed_at",),
    columns="""
        product_id              STRING,
        category_id             STRING,
        category_code           STRING,
        changed_at              TIMESTAMP,
        original_price          DECIMAL(12,2),
        new_price               DECIMAL(12,2),
        pct_price_change        DOUBLE   COMMENT 'Ratio: -0.2 = a 20% drop',
        base_hours              DOUBLE   COMMENT 'Baseline length: up to 7 days, only at the original price',
        post_hours              DOUBLE   COMMENT 'Post length: up to 2 hours, only while the new price holds',
        base_views              BIGINT,
        base_purchases          BIGINT   COMMENT 'The volume floor is applied to this column',
        post_views              BIGINT,
        post_purchases          BIGINT,
        base_purchases_per_2h   DOUBLE,
        post_purchases_per_2h   DOUBLE,
        base_conv               DOUBLE,
        post_conv               DOUBLE,
        pct_change_purchases    DOUBLE,
        elasticity              DOUBLE   COMMENT 'pct_change_purchases / pct_price_change',
        computed_at             TIMESTAMP""",
)

GOLD_CART_ABANDONMENT = TableSpec(
    layer="gold", name="cart_abandonment",
    comment="Q2: carts added within 30 minutes of a price increase above 15% whose session never "
            "bought the product, with lost revenue at the cart price and at the old price.",
    cluster_by=("category_id",),
    columns="""
        product_id              STRING,
        category_id             STRING,
        category_code           STRING,
        session_key             STRING,
        carted_at               TIMESTAMP,
        changed_at              TIMESTAMP,
        price_at_cart           DECIMAL(12,2),
        price_before_increase   DECIMAL(12,2),
        pct_price_change        DOUBLE,
        lost_revenue_at_cart    DECIMAL(12,2)  COMMENT 'Headline: the price the shopper walked away from',
        lost_revenue_at_old     DECIMAL(12,2)  COMMENT 'Sensitivity: the same sale at the pre-increase price',
        computed_at             TIMESTAMP""",
)
