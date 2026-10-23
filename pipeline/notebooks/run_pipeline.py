# Databricks notebook source
# MAGIC %md
# MAGIC # Generate AIS positions and follow them through to callings
# MAGIC Configure port pairs in `config/fleet.yaml`. Each release is appended to Bronze,
# MAGIC then three checkpointed streams drain in order: tracks, registry association,
# MAGIC and calling history. `round_metrics` records timings for each complete release.
# MAGIC Run all resets this pipeline's owned tables and checkpoint folder.
# COMMAND ----------
import sys
from pathlib import Path

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ais_pipeline_tutorial")
dbutils.widgets.text("vessels", "")
dbutils.widgets.text("reports", "")
dbutils.widgets.text("source_root", "")
root = dbutils.widgets.get("source_root") or str(Path.cwd().parent)
sys.path.insert(0, root)
from ais_pipeline.runner import run
from ais_pipeline.validation import validate

prefix = dbutils.widgets.get("catalog") + "." + dbutils.widgets.get("schema")
summary = run(spark, dbutils, prefix, str(Path(root) / "config/fleet.yaml"),
              int(dbutils.widgets.get("vessels")) if dbutils.widgets.get("vessels") else None,
              int(dbutils.widgets.get("reports")) if dbutils.widgets.get("reports") else None)
summary["validation"] = validate(spark, prefix)
import json
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
