CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_core_parquet_v3` AS
WITH
  unioned AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v2`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v3`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v4`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v5`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v6`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v7`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v8`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v9`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v10` )
  --
SELECT
  DATE(submission_timestamp) AS submission_date_s3,
  DATE(submission_timestamp) AS submission_date,
  metadata.uri.app_name,
  os,
  STRUCT(
    document_id,
    UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,
    metadata.header.date AS `date`,
    metadata.geo.country AS geo_country,
    metadata.geo.city AS geo_city,
    metadata.uri.app_build_id AS app_build_id,
    normalized_channel
  ) AS metadata,
  STRUCT(
    metadata.uri.app_name AS name
  ) AS application,
  v,
  client_id,
  seq,
  locale,
  osversion,
  device,
  arch,
  profile_date,
  default_search,
  distribution_id,
  created,
  tz,
  sessions,
  durations,
  searches,
  experiments,
  flash_usage,
  campaign,
  campaign AS campaign_id,
  default_browser,
  show_tracker_stats_share,
  accessibility_services,
  metadata.uri.app_version AS metadata_app_version,
  bug_1501329_affected
FROM
  unioned
