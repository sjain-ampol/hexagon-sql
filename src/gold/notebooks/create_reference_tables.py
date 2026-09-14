# Databricks notebook source
# DBTITLE 1,Reference table overview
"""Create / refresh business-owned reference tables in {catalog}.{ref_schema}.

Runs OUT of the gold SQL pipeline (as a job task ordered BEFORE it) so the
managed reference tables are never owned or overwritten by pipeline refreshes.
The gold dims/facts (dim_wallchart_step_sql, fact_wallchart_cell_sql) read these tables.

SQL bodies live in src/gold/reference_sql/*.sql for reviewable DDL/DML;
this notebook substitutes {catalog}/{ref_schema} and issues the statements.
ref_wallchart_tm_inclusion_sql.sql is DDL only — row data is loaded from the
Ampol Excel file on a Unity Catalog Volume (see tm_inclusion_volume_path widget).
"""

# COMMAND ----------

# DBTITLE 1,Install openpyxl for Excel reads
# MAGIC %pip install openpyxl --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Widgets and constants
dbutils.widgets.text("catalog", "hexagon")
dbutils.widgets.text("ref_schema", "tni_ref")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text(
    "tm_inclusion_volume_path",
    "/Volumes/hexagon/tni_ref/reference_raw_files/ref_wallchart_tm_inclusion/WallchartTMtable_20260904.xlsx",
)

catalog = dbutils.widgets.get("catalog")
ref_schema = dbutils.widgets.get("ref_schema")
source_path = dbutils.widgets.get("source_path").strip()
tm_inclusion_volume_path = dbutils.widgets.get("tm_inclusion_volume_path").strip()

if not source_path:
    raise ValueError("source_path widget is required (workspace path to bundle src/)")
if not source_path.startswith("/Workspace"):
    source_path = "/Workspace" + source_path
if not tm_inclusion_volume_path:
    raise ValueError("tm_inclusion_volume_path widget is required")

# COMMAND ----------

# DBTITLE 1,Helpers
import re
from datetime import date
from pathlib import Path
from typing import Optional

REF_SQL_DIR = Path(source_path) / "gold" / "reference_sql"
SOURCE_OF_TRUTH_RULE_SQL = Path(
    "/Workspace/Users/rbagchi@ampol.com.au/.bundle/ampol_hexagon_tni/dev/files/src/notebooks/reference_sql/ref_wallchart_rule.sql"
)

_TM_INCLUSION_ID_COLUMNS = frozenset(
    {"Project", "ProjectIdentifier", "ProjectName", "Summary", "Offsite TM", "EWR TM"}
)


def load_sql(path: Path) -> str:
    body = path.read_text(encoding="utf-8")
    return body.replace("{catalog}", catalog).replace("{ref_schema}", ref_schema)


def split_statements(sql: str) -> list[str]:
    parts = re.split(r";[ \t]*(?:--[^\n]*)?\r?\n", sql + "\n")
    return [p.strip() for p in parts if p.strip()]


def apply_file(path: Path) -> None:
    print(f"--- applying {path.name} ---")
    for stmt in split_statements(load_sql(path)):
        preview = " ".join(stmt.split())[:80]
        print(f"  > {preview} ...")
        spark.sql(stmt)


def apply_rule_seed_from_source_of_truth() -> None:
    if not SOURCE_OF_TRUTH_RULE_SQL.is_file():
        raise FileNotFoundError(f"Source-of-truth rule SQL not found: {SOURCE_OF_TRUTH_RULE_SQL}")
    body = SOURCE_OF_TRUTH_RULE_SQL.read_text(encoding="utf-8")
    body = body.replace("{catalog}", catalog).replace("{ref_schema}", ref_schema)
    body = body.replace("ref_wallchart_rule", "ref_wallchart_rule_sql")
    print(f"--- applying source-of-truth reseed for ref_wallchart_rule_sql from {SOURCE_OF_TRUTH_RULE_SQL} ---")
    for stmt in split_statements(body):
        preview = " ".join(stmt.split())[:80]
        print(f"  > {preview} ...")
        spark.sql(stmt)


def _asset_class_from_column(column_name: str) -> Optional[str]:
    if column_name.startswith("PSV"):
        return "PSV"
    if column_name.startswith("Column"):
        return "COL"
    if column_name.startswith("CV"):
        return "CV"
    if column_name.startswith("Exchangers"):
        return "EXCH"
    return None


def _is_blank_tm(value: str) -> bool:
    v = (value or "").strip()
    return not v or v.upper() in {"NA", "TM-00"}


def load_tm_inclusion_dataframe(xlsx_path: str):
    import pandas as pd

    if not Path(xlsx_path).exists():
        raise FileNotFoundError(
            f"TM inclusion Excel not found at {xlsx_path}. "
            "Upload WallchartTMtable.xlsx to the Volume path configured in tm_inclusion_volume_path."
        )

    pdf = pd.read_excel(xlsx_path, sheet_name=0, dtype=str)
    pdf.columns = [str(c).strip() for c in pdf.columns]

    if "Project" not in pdf.columns:
        raise ValueError(f"Excel at {xlsx_path} is missing required column 'Project'")

    def _row_has_data(row) -> bool:
        for col in pdf.columns:
            if col == "Project":
                continue
            val = row.get(col)
            if val is not None and str(val).strip() and str(val).lower() != "nan":
                return True
        return False

    pdf = pdf[pdf.apply(_row_has_data, axis=1)].copy()

    value_columns = [c for c in pdf.columns if c not in _TM_INCLUSION_ID_COLUMNS]
    if not value_columns:
        raise ValueError(f"No TM columns to unpivot in {xlsx_path}")

    melted = pdf.melt(
        id_vars=["Project"],
        value_vars=value_columns,
        var_name="source_column",
        value_name="task_model_name",
    )
    melted["task_model_name"] = melted["task_model_name"].fillna("").astype(str).str.strip()
    melted = melted[~melted["task_model_name"].apply(_is_blank_tm)]

    melted["asset_class"] = melted["source_column"].map(_asset_class_from_column)
    melted = melted[melted["asset_class"].notna()]

    melted["project"] = pd.to_numeric(melted["Project"], errors="coerce")
    melted = melted[melted["project"].notna()]
    melted["project"] = melted["project"].astype("int64")

    today = date.today()
    out = (
        melted[["project", "asset_class", "task_model_name", "source_column"]]
        .drop_duplicates(subset=["project", "asset_class", "task_model_name"])
        .assign(is_active=True, effective_from=today, effective_to=None)
    )

    if out.empty:
        raise ValueError(f"No active TM inclusion rows parsed from {xlsx_path}")

    return spark.createDataFrame(out)


def refresh_tm_inclusion_from_volume(xlsx_path: str) -> int:
    fq_table = f"{catalog}.{ref_schema}.ref_wallchart_tm_inclusion_sql"
    df = load_tm_inclusion_dataframe(xlsx_path)
    row_count = df.count()
    print(f"Parsed {row_count} TM inclusion rows from {xlsx_path}")

    spark.sql(f"DELETE FROM {fq_table} WHERE is_active = true")
    df.write.mode("append").saveAsTable(fq_table)
    return row_count

# COMMAND ----------

# DBTITLE 1,Create DDL and reseed rules
sql_files = sorted(REF_SQL_DIR.glob("*.sql"))
if not sql_files:
    raise FileNotFoundError(f"No reference SQL files found under {REF_SQL_DIR}")

print(f"Creating/refreshing reference tables in {catalog}.{ref_schema} from {REF_SQL_DIR} ...")
for f in sql_files:
    if f.name == "ref_wallchart_rule_sql.sql":
        apply_file(f)  # DDL from bundled file
        apply_rule_seed_from_source_of_truth()  # exact rule body from source-of-truth file
    else:
        apply_file(f)

# COMMAND ----------

# DBTITLE 1,Load TM inclusion
print(f"Loading ref_wallchart_tm_inclusion_sql from Volume: {tm_inclusion_volume_path}")
tm_rows = refresh_tm_inclusion_from_volume(tm_inclusion_volume_path)
print(f"Inserted {tm_rows} active rows into {catalog}.{ref_schema}.ref_wallchart_tm_inclusion_sql")

# COMMAND ----------

# DBTITLE 1,Summarize row counts
for table in ("ref_wallchart_rule_sql", "ref_wallchart_tm_inclusion_sql"):
    fq = f"{catalog}.{ref_schema}.{table}"
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {fq} WHERE is_active = true").collect()[0][0]
        print(f"{fq}: {n} active rows")
    except Exception as e:
        print(f"{fq}: could not count ({e})")

print("Done.")

# COMMAND ----------

