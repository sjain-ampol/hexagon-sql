# Databricks notebook source
# DBTITLE 1,Metric view overview
"""Create UC metric views over gold_sql fact_*_sql + dim_*_sql (Kimball).

Post-gold, idempotent CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML.
YAML bodies live in src/gold/semantic/*.yml so the semantic contract is reviewable
and the notebook only substitutes catalog/schema and issues DDL.
"""

# COMMAND ----------

# DBTITLE 1,Widgets and constants
dbutils.widgets.text("catalog", "hexagon")
dbutils.widgets.text("gold_schema", "gold_tni")
dbutils.widgets.text("source_path", "")

catalog = dbutils.widgets.get("catalog")
gold_schema_widget = dbutils.widgets.get("gold_schema")
source_path = dbutils.widgets.get("source_path").strip()

if not source_path:
    raise ValueError("source_path widget is required (workspace path to bundle src/)")

if not source_path.startswith("/Workspace"):
    source_path = "/Workspace" + source_path

VIEWS = (
    "mv_workpack_status_sql",
    "mv_task_step_execution_sql",
    "mv_wallchart_readiness_sql",
    "mv_workflow_cycle_time_sql",
)

ANCHOR_TABLE = "fact_work_package_status_sql"

# COMMAND ----------

# DBTITLE 1,Resolve schema
from pathlib import Path


def _table_exists(cat: str, sch: str, table: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(f"{cat}.{sch}.{table}"))
    except Exception:
        return False


def resolve_gold_schema(cat: str, preferred: str) -> str:
    if _table_exists(cat, preferred, ANCHOR_TABLE):
        return preferred

    rows = spark.sql(
        f"""
        SELECT table_schema
        FROM `{cat}`.information_schema.tables
        WHERE lower(table_name) = '{ANCHOR_TABLE}'
        ORDER BY
          CASE
            WHEN table_schema = '{preferred}' THEN 0
            WHEN table_schema LIKE '%{preferred}' THEN 1
            ELSE 2
          END,
          table_schema
        LIMIT 1
        """
    ).collect()
    if not rows:
        raise ValueError(
            f"{cat}.{preferred}.{ANCHOR_TABLE} not found. "
            "Run gold_sql_pipeline first, then rerun this notebook."
        )
    resolved = rows[0][0]
    print(f"gold_schema widget={preferred}; resolved={resolved}")
    return resolved


gold_schema = resolve_gold_schema(catalog, gold_schema_widget)
fq = f"{catalog}.{gold_schema}"

# COMMAND ----------

# DBTITLE 1,Create metric views
def load_yaml(name: str) -> str:
    path = Path(source_path) / "gold" / "semantic" / f"{name}.yml"
    if not path.is_file():
        raise FileNotFoundError(f"Metric view YAML not found: {path}")
    body = path.read_text(encoding="utf-8")
    return (
        body.replace("{catalog}", catalog)
        .replace("{gold_schema}", gold_schema)
        .strip()
    )


def create_metric_view(name: str) -> None:
    yaml_body = load_yaml(name)
    ddl = (
        f"CREATE OR REPLACE VIEW {fq}.{name}\n"
        "WITH METRICS\n"
        "LANGUAGE YAML\n"
        f"AS $$\n{yaml_body}\n$$"
    )
    spark.sql(ddl)
    print(f"created metric view {fq}.{name}")


print(f"Creating metric views in {fq} from {source_path}/gold/semantic ...")
for view_name in VIEWS:
    create_metric_view(view_name)
print("Done.")

# COMMAND ----------

