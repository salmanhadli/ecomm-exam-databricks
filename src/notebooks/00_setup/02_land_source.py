# Databricks notebook source
# MAGIC %md
# MAGIC # Setup · Land the source CSVs
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_00_setup` → `land_source` |
# MAGIC | **Writes** | `/Volumes/<catalog>/raw/landing/source/cosmetics/` — the five monthly CSVs (2.3 GB) |
# MAGIC | **Brief** | Dataset: *eCommerce Events History in Cosmetics Shop* (Kaggle) · evidence item 2 |
# MAGIC | **Databricks concepts** | Unity Catalog volumes: `/Volumes/...` paths work with plain Python file APIs (`os`, `shutil`, `open`) · Free Edition egress limits |
# MAGIC
# MAGIC Three ways to fill the folder, tried in this order:
# MAGIC
# MAGIC 1. **Already present** — nothing to do, so re-runs are free.
# MAGIC 2. **`source_copy_from`** job parameter — copy from a volume folder you already filled.
# MAGIC 3. **Download from Kaggle.** Free Edition limits outbound internet to trusted domains, so this
# MAGIC    may be refused. If so, the task fails with instructions: upload the five CSVs through
# MAGIC    *Catalog → `<catalog>` → raw → landing* into `source/cosmetics/`, then re-run.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import shutil
import sys
import tempfile
import zipfile

sys.path.insert(0, os.path.abspath("../.."))

from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.settings import KAGGLE_SLUG, SOURCE_FILES  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="land_source")

dbutils.widgets.text("source_copy_from", "")
copy_from = dbutils.widgets.get("source_copy_from").rstrip("/")

target = project.source_dir
os.makedirs(target, exist_ok=True)  # volume paths behave like a local file system


def missing_files() -> list[str]:
    return [name for name in SOURCE_FILES if not os.path.exists(f"{target}/{name}")]

# COMMAND ----------

# MAGIC %md ## 2 · Fill the source folder

# COMMAND ----------

CHUNK_BYTES = 4 * 1024 * 1024  # 4 MiB reads/writes: larger buffers cut per-call overhead on the volume mount

missing = missing_files()
method = "already present"

if missing and copy_from:
    method = f"copied from {copy_from}"
    for name in missing:
        shutil.copyfile(f"{copy_from}/{name}", f"{target}/{name}")
        print(f"copied {name}")

elif missing:
    import requests

    method = "downloaded from Kaggle"
    # Public datasets download without a token: Kaggle redirects to a signed storage URL.
    url = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_SLUG}"
    stage = tempfile.mkdtemp(prefix="kaggle-")
    archive = os.path.join(stage, "archive.zip")
    try:
        with requests.get(url, stream=True, timeout=(20, 300)) as response:
            response.raise_for_status()
            with open(archive, "wb") as out:
                for chunk in response.iter_content(CHUNK_BYTES):
                    out.write(chunk)
        # Stream each CSV straight from the zip into the volume: no extracted copy on local disk.
        with zipfile.ZipFile(archive) as zf:
            for member in zf.namelist():
                name = os.path.basename(member)
                if name in missing:
                    with zf.open(member) as src, open(f"{target}/{name}", "wb") as dst:
                        shutil.copyfileobj(src, dst, length=CHUNK_BYTES)
                    print(f"landed {name}")
    except requests.RequestException as err:
        raise RuntimeError(
            f"Kaggle download failed ({type(err).__name__}: {err}). Free Edition restricts "
            f"outbound internet. Upload {', '.join(missing)} to {target}/ through Catalog "
            f"Explorer, or set the job parameter source_copy_from to a volume folder that "
            f"already holds them.") from err
    finally:
        shutil.rmtree(stage, ignore_errors=True)

# COMMAND ----------

# MAGIC %md ## 3 · Verify and record evidence

# COMMAND ----------

still_missing = missing_files()
if still_missing:
    raise RuntimeError(f"Source files still missing in {target}: {still_missing}")

total_bytes = 0
for name in SOURCE_FILES:
    size = os.path.getsize(f"{target}/{name}")
    total_bytes += size
    ev.record(f"source_bytes.{name}", size)

ev.record("source_method", method)
ev.record("source_files", len(SOURCE_FILES))
ev.record("source_total_mb", round(total_bytes / 1e6, 1))
ev.flush()
