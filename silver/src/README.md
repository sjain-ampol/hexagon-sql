# Ampol Hexagon Silver — Native SDP SQL Pipeline

Standalone Spark Declarative Pipelines SQL equivalent of the LakeFlow Framework
metadata-driven silver pipeline.  Each `.sql` file handles one bronze-to-silver
table using `APPLY CHANGES INTO` (SCD Type 1).

## File layout

```
ampol_hexagon_silver_sql/
├── databricks.yml                              # DAB root: variables, dev target
├── resources/
│   └── hexagon_silver_sql_pipeline.yml         # Pipeline resource definition
└── src/
    ├── 01_projects.sql                         # One file per table
    ├── 02_assets.sql
    ├── 03_work_packages.sql
    ├── 04_work_packages_steps.sql
    ├── 05_work_package_details.sql
    ├── 06_work_package_workflow_lists.sql
    ├── 07_tasks_tests_planned_details.sql
    ├── 08_task_step_instance_details.sql
    └── README.md
```

## Pattern in each SQL file

Every file follows a five-statement pattern (main flow + recon flow):

1. **Main streaming view** — reads from the bronze source with `STREAM()`,
   applies column aliases (PascalCase → UPPER_SNAKE_CASE), casts, and
   computed columns (`ACTIVE_FLAG`, `_scd_sequence`, `LOADED_BY`, etc.).

2. **Target streaming table** — `CREATE OR REFRESH STREAMING TABLE <name>_sql`
   with CDF and row-tracking table properties.

3. **Main APPLY CHANGES INTO** — SCD Type 1 merge keyed on the entity's
   natural keys, sequenced by `_scd_sequence` (or `load_timestamp` for
   projects), with `IGNORE NULL UPDATES` and EXCEPT for bookkeeping columns.

4. **Recon streaming view** — reads from the `*_recon` bronze table. Produces
   only keys + `ACTIVE_FLAG` + `DT_REMOVED` + the sequence column.
   `ACTIVE_FLAG` uses `if(hard_delete, false, null)` so that non-delete
   recon records leave the flag unchanged (via IGNORE NULL UPDATES).

5. **Recon APPLY CHANGES INTO** — second SCD Type 1 merge into the SAME
   target table from the recon view. `IGNORE NULL UPDATES` ensures only
   `ACTIVE_FLAG` and `DT_REMOVED` are overwritten; all other business
   columns are preserved.

## How to add a new table / source

1. Copy any existing `.sql` file (e.g. `02_assets.sql`) to a new file.
2. Update the four references:
   - **Source**: `STREAM(catalog.schema.bronze_table)`
   - **View name**: `v_hexagon_<entity>`
   - **Target table**: `<entity>_sql`
   - **SELECT list**: map bronze columns to UPPER_SNAKE aliases.
3. Set the correct `KEYS`, `SEQUENCE BY`, and `EXCEPT` for the entity.
4. Register the new file in `resources/hexagon_silver_sql_pipeline.yml` under
   `libraries:` → `- file: path: src/<new_file>.sql`.
5. Deploy: `databricks bundle deploy -t dev`.

## Deployment

```bash
# From the bundle root (ampol_hexagon_silver_sql/)
databricks bundle validate -t dev
databricks bundle deploy -t dev

# Trigger a pipeline update
databricks bundle run hexagon_silver_sql_pipeline -t dev
```

## Conventions

- Source tables are fully qualified (`hexagon.hexagon_bronze.*`).
  To point at a different source, update the `STREAM()` references.
- Target tables live in the pipeline's default catalog/schema
  (`hexagon.hexagon_silver`), set in the resource YAML.
- Target names carry a `_sql` suffix to avoid colliding with the
  existing LakeFlow Framework tables.
- `meta_load_details` (a struct column injected by the LFF engine)
  is **not** produced here; it is framework-specific.
