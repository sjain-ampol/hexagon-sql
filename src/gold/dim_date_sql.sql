-- =============================================================================
-- dim_date_sql.sql  |  dim_date_sql
-- Source: generated calendar spine (2018-01-01 to 2030-12-31)
-- Target: ${catalog}.${gold_schema}.dim_date_sql
-- Type:   MATERIALIZED VIEW
-- Notes:  Static calendar dimension. Campaign day/phase remain null.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_date_sql
COMMENT 'Static calendar dimension (2018-2030). Role-playing date keys on facts.'
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY (date_key)
AS
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d AS calendar_date,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month,
  day(d) AS day,
  date_format(d, 'MMMM') AS month_name,
  date_format(d, 'EEEE') AS day_name,
  dayofweek(d) AS day_of_week,
  weekofyear(d) AS week_of_year,
  CAST(date_trunc('month', d) AS DATE) AS month_start_date,
  CAST(date_trunc('quarter', d) AS DATE) AS quarter_start_date,
  dayofweek(d) IN (1, 7) AS is_weekend,
  CAST(NULL AS INT) AS ti_campaign_day,
  CAST(NULL AS STRING) AS ti_campaign_phase
FROM (
  SELECT explode(sequence(to_date('2018-01-01'), to_date('2030-12-31'), INTERVAL 1 DAY)) AS d
);
