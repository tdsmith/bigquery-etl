WITH columns AS (
  SELECT
    column_name,
    REGEXP_REPLACE(column_name, r"_s3$", "") AS new_column_name,
    data_type,
    is_partitioning_column,
    description
  FROM INFORMATION_SCHEMA.COLUMNS
  JOIN INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
  USING (table_name, column_name, data_type)
  WHERE table_name = @source_table
), aggregates AS (
  SELECT
    description,
    CASE WHEN is_partitioning_column = "YES" OR description = "aggregate_grouping_field" THEN
      CONCAT(column_name, " AS ", new_column_name)
    -- custom aggregations by description
    WHEN description = "aggregate_max" THEN
      CONCAT("MAX(", column_name, ") OVER _w AS ", CONCAT(new_column_name, "_max"))
    WHEN description = "aggregate_mean" THEN
      CONCAT("AVG(", column_name, ") OVER _w AS ", CONCAT(new_column_name, "_mean"))
    WHEN description = "aggregate_sum" THEN
      CONCAT("SUM(", column_name, ") OVER _w AS ", CONCAT(new_column_name, "_sum"))
    WHEN description = "aggregate_null_string" THEN
      CONCAT("CAST(NULL AS STRING) AS ", new_column_name)
    WHEN description = "aggregate_null_sum" THEN
      CONCAT("NULL AS ", CONCAT(new_column_name, "_sum"))
    WHEN description = "aggregate_omit" THEN
      CAST(NULL AS STRING) -- no aggregation
    -- custom aggregations by column_name
    WHEN column_name = "active_ticks" THEN
      "SUM(active_ticks/(3600.0/5)) OVER _w AS active_hours_sum"
    WHEN column_name = "subsession_counter" THEN
      "COUNTIF(subsession_counter = 1) OVER w1 AS sessions_started_on_this_day"
    WHEN column_name = "subsession_length" THEN
      "SUM(subsession_length/3600.0) OVER w1 AS subsession_hours_sum"
    WHEN column_name in ("country", "city", "geo_subdivision1", "geo_subdivision2") THEN
      -- only aggregate a whole geoip lookup indicated by country being present
      CONCAT(
        "FIRST_VALUE(",
        "IF(country NOT IN (NULL, '??'),",
        IF(
          column_name = "country",
          column_name,
          CONCAT(
            "COALESCE(",
            column_name,
            ", '??')"
          )
        ),
        " IGNORE NULLS) OVER _w AS ",
        new_column_name
      )
    -- default aggregation
    ELSE
      CONCAT("FIRST_VALUE(", column_name, " IGNORE NULLS) OVER _w AS ", new_column_name)
    END AS agg
  FROM columns
  WHERE
    column_name != "generated_time"
  LIMIT 5
), where_clause AS (
  SELECT CONCAT(column_name, " = @", new_column_name) as param
  FROM columns
  WHERE is_partitioning_column = "YES"
), partition_by_clause AS (
  SELECT COALESCE(
    (
      SELECT CONCAT("PARTITION BY ", STRING_AGG(column_name, ", "))
      FROM columns
      WHERE
        is_partitioning_column = "YES"
        OR description = "aggregate_grouping_field"
    ),
    (
      SELECT CONCAT("PARTITION BY ", STRING_AGG(column_name, ", "))
      FROM columns
      WHERE column_name = "client_id"
    ),
    ""
  ) AS clause
), order_by_clause AS (
  SELECT COALESCE(
    (
      SELECT CONCAT("ORDER BY ", STRING_AGG(column_name, ", "))
      FROM columns
      WHERE description = "aggregate_ordering_field"
    ),
    (
      SELECT CONCAT("ORDER BY ", STRING_AGG(column_name, ", "))
      FROM columns
      WHERE column_name = "timestamp"
    ),
    ""
  )
), dedup_partition_by_clause AS (
  SELECT COALESCE(
    (
      SELECT CONCAT(ANY_VALUE(clause), ", ", STRING_AGG(column_name, ", "))
      FROM partition_by_clause, columns
      WHERE
        clause != ""
        AND column_name = "document_id"
    ),
    (
      SELECT CONCAT("PARTITION BY ", STRING_AGG(column_name, ", "))
      FROM columns
      WHERE column_name = "document_id"
    )
  ) AS clause
)
SELECT CONCAT(
  "WITH",
  COALESCE(
    -- if the document_id field is present this will dedup otherwise this will be NULL
    CONCAT(
      "\n  -- Identify duplicates of document_id within each aggregation window partition",
      "\n  numbered_duplicates AS (",
      "\n  SELECT",
      "\n    ROW_NUMBER() OVER (", (SELECT * FROM dedup_partition_by_clause), (SELECT * FROM order_by_clause), ") AS _n,",
      "\n    * REPLACE(LOWER(client_id) AS client_id)",
      "\n  FROM",
      "\n    ", @source_table,
      "\n  WHERE",
      "\n    ", (SELECT STRING_AGG(param, "\n     AND ") FROM where_clause),
      "\n    AND client_id IS NOT NULL ),",
      "\n  -- Remove duplicates of document_id to get valid SUM and AVG values",
      "\n  deduplicated AS (",
      "\n  SELECT",
      "\n    * EXCEPT (_n)",
      "\n  FROM",
      "\n    numbered_duplicates",
      "\n  WHERE",
      "\n    _n = 1 ),",
      "\n  -- Aggregate using windows"
    ),
    -- if deduplication is unavailable this is the fallback
    CONCAT(
      "\n  deduplicated AS (",
      "\n  SELECT",
      "\n    *",
      "\n  FROM",
      "\n    ", @source_table,
      "\n  WHERE",
      "\n    ", (SELECT STRING_AGG(param, "\n     AND ") FROM where_clause), " ),"
    )
  ),
  "\n  windowed AS (",
  "\n  SELECT",
  "\n    ROW_NUMBER() OVER _w_unframed AS _n,",
  (SELECT STRING_AGG(CONCAT("\n    ", agg, ","), "") FROM aggregates WHERE NOT REGEXP_CONTAINS(agg, " OVER ")),
  "\n    CURRENT_DATETIME() AS generated_timestamp",
  (SELECT STRING_AGG(CONCAT(",\n    ", agg), "") FROM aggregates WHERE REGEXP_CONTAINS(agg, " OVER ")),
  "\n  FROM",
  "\n    deduplicated",
  "\n  WINDOW",
-- This window is not included because it is only required for currently unused aggregate functions
--"\n    -- an unordered window is required for PERCENTILE_CONT and PERCENTILE_DISC",
--"\n    _w_unordered AS (", (SELECT * FROM partition_by_clause), "),",
  "\n    -- an unframed window is required by ROW_COUNT, LEAD, and LAG",
  "\n    _w_unframed AS (", (SELECT * FROM partition_by_clause), " ", (SELECT * FROM order_by_clause), "),",
  "\n    -- default aggregation window",
  "\n    _w AS (", (SELECT * FROM partition_by_clause), " ", (SELECT * FROM order_by_clause), " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))",
  "\n-- Choose one aggregated row from each window partition",
  "\nSELECT",
  "\n  * EXCEPT (_n)",
  "\nFROM",
  "\n  windowed",
  "\nWHERE",
  "\n  _n = 1"
) AS query
