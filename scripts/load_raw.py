# Databricks notebook source
# MAGIC %md
# MAGIC # Load raw — the EL step
# MAGIC Downloads the course dataset from GitHub **directly into a Unity Catalog Volume**
# MAGIC (no local files, no drag-and-drop), then lands it as bronze tables.
# MAGIC
# MAGIC **SUPPORTER VERSION:** the 12 free months, plus every newer month of postings and skills
# MAGIC from the supporter link (a new month lands by the 5th). Don't re-run the curl command
# MAGIC from 4.01: it puts the free version back.
# MAGIC
# MAGIC Every scheduled run (4.01): Task 1 of the Databricks Job, pulled from `scripts/load_raw.py` in your repo.
# MAGIC Safe to rerun: files already in the Volume are skipped, the raw tables are re-landed.

# COMMAND ----------

REPO = "lukebarousse/dbt_Analytics_Engineering_Course"
TAG = "dataset-v1"                       # the course dataset release (pinned)
VOLUME = "/Volumes/raw/jobs/files"

MONTHS = [
    "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06",
]
FILES = [f"raw_job_postings_{m}.parquet" for m in MONTHS] + ["raw_job_skills.parquet"]

# COMMAND ----------

# the landing zone: the raw catalog + a schema named for the source system (jobs) + a Volume for the files
# (raw has ONE owner — this notebook; dev and prod both read it)
spark.sql("CREATE CATALOG IF NOT EXISTS raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS raw.jobs")
spark.sql("CREATE VOLUME IF NOT EXISTS raw.jobs.files")

# COMMAND ----------

# download from the GitHub release straight into the Volume (~85MB total)
import urllib.request, os

for name in FILES:
    dest = f"{VOLUME}/{name}"
    if os.path.exists(dest):
        print(f"  already have {name}")
        continue
    print(f"  downloading {name} ...")
    urllib.request.urlretrieve(f"https://github.com/{REPO}/releases/download/{TAG}/{name}", dest)

print("Done.")

# COMMAND ----------

# supporter months: manifest.json lists them, and each file sits next to it
import json, os, urllib.error, urllib.request
from urllib.parse import urljoin

SUPPORTER_URL = "https://lukeb.co/dbt-supporter-data"   # short link to the supporter manifest

new = []
try:
    with urllib.request.urlopen(SUPPORTER_URL) as resp:   # follows the short link
        manifest = json.load(resp)
        manifest_url = resp.geturl()   # the real address, after the redirect
    for month in manifest["supporter"]["months"]:
        for f in month["files"]:
            dest = f"{VOLUME}/{f['name']}"
            if f["kind"] not in ("postings", "skills") or os.path.exists(dest):
                continue
            print(f"  downloading {f['name']} ...")
            part = dest.replace(".parquet", ".part")   # the *.parquet globs can't see it yet
            urllib.request.urlretrieve(urljoin(manifest_url, f["name"]), part)
            if os.path.getsize(part) != f["bytes"]:
                raise RuntimeError(f"{f['name']} didn't download fully. Run it again.")
            dbutils.fs.mv(part, dest)   # Databricks' own move for Volume files
            if month["month"] not in new:
                new.append(month["month"])
except (urllib.error.URLError, ValueError) as e:
    raise RuntimeError(f"Supporter download failed ({e}). Try again later. If it keeps failing,\n"
                       "the link may have changed: check the supporter lesson.") from None
print(f"Supporter months downloaded: {', '.join(new) or 'none new'}")

# COMMAND ----------

# land the bronze tables — raw, untouched, exactly as the scraper delivered them
spark.sql(f"""
    CREATE OR REPLACE TABLE raw.jobs.raw_job_postings AS
    SELECT * FROM read_files('{VOLUME}/raw_job_postings_*.parquet', format => 'parquet')
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE raw.jobs.raw_job_skills AS
    SELECT * FROM read_files('{VOLUME}/raw_job_skills*.parquet', format => 'parquet')
""")

# COMMAND ----------

# sanity check: the free 12 months alone give 843,097 postings and 3,304,574 skill rows,
# and each supporter month adds more
display(spark.sql("""
    SELECT 'raw_job_postings' AS table, COUNT(*) AS rows FROM raw.jobs.raw_job_postings
    UNION ALL
    SELECT 'raw_job_skills', COUNT(*) FROM raw.jobs.raw_job_skills
"""))
