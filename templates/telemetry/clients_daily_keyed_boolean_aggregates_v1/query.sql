-- Query generated by:
    -- templates/clients_daily_scalar_aggregates.sql.py --agg-type keyed-boolean
WITH
    -- normalize client_id and rank by document_id
    numbered_duplicates AS (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY
                    client_id,
                    submission_date,
                    document_id
                ORDER BY `timestamp`
                ASC
            ) AS _n,
            * REPLACE(LOWER(client_id) AS client_id)
        FROM main_summary_v4
        WHERE submission_date = @submission_date
        AND channel in (
            "release", "esr", "beta", "aurora", "default", "nightly"
        )
        AND client_id IS NOT NULL
    ),


    -- Deduplicating on document_id is necessary to get valid SUM values.
    deduplicated AS (
        SELECT * EXCEPT (_n)
        FROM numbered_duplicates
        WHERE _n = 1
    ),


grouped_metrics AS
  (select
    timestamp,
    client_id,
    submission_date,
    os,
    app_version,
    app_build_id,
    channel,
    ARRAY<STRUCT<
        name STRING,
        value ARRAY<STRUCT<key STRING, value BOOLEAN>>
    >>[
        ('scalar_parent_devtools_tool_registered', scalar_parent_devtools_tool_registered),
        ('scalar_parent_widget_ime_name_on_windows', scalar_parent_widget_ime_name_on_windows),
        ('scalar_parent_widget_ime_name_on_mac', scalar_parent_widget_ime_name_on_mac),
        ('scalar_parent_a11y_theme', scalar_parent_a11y_theme),
        ('scalar_parent_sandbox_no_job', scalar_parent_sandbox_no_job),
        ('scalar_parent_widget_ime_name_on_linux', scalar_parent_widget_ime_name_on_linux),
        ('scalar_parent_services_sync_sync_login_state_transitions', scalar_parent_services_sync_sync_login_state_transitions),
        ('scalar_parent_security_pkcs11_modules_loaded', scalar_parent_security_pkcs11_modules_loaded)
    ] as metrics
  FROM deduplicated),

  flattened_metrics AS
    (SELECT
      timestamp,
      client_id,
      submission_date,
      os,
      app_version,
      app_build_id,
      channel,
      metrics.name AS metric,
      value.key AS key,
      value.value AS value
    FROM grouped_metrics
    CROSS JOIN unnest(metrics) AS metrics,
    unnest(metrics.value) AS value),


    -- Aggregate by client_id using windows
    windowed AS (
        SELECT
            ROW_NUMBER() OVER w1_unframed AS _n,
            submission_date,
            client_id,
            os,
            SPLIT(app_version, '.')[OFFSET(0)] AS app_version,
            app_build_id,
            channel,

metric,
key,
SUM(CASE WHEN value = True THEN 1 ELSE 0 END) OVER w1 AS true_col,
SUM(CASE WHEN value = False THEN 1 ELSE 0 END) OVER w1 AS false_col

        FROM flattened_metrics
        WINDOW
            -- Aggregations require a framed window
            w1 AS (
                PARTITION BY
                    client_id,
                    submission_date,
                    os,
                    app_version,
                    app_build_id,
                    channel
                    ,
                    metric,
                    key

                ORDER BY `timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
            ),

            -- ROW_NUMBER does not work on a framed window
            w1_unframed AS (
                PARTITION BY
                    client_id,
                    submission_date,
                    os,
                    app_version,
                    app_build_id,
                    channel
                    ,
                    metric,
                    key

                ORDER BY `timestamp` ASC
            )
    )

select
      client_id,
      submission_date,
      os,
      app_version,
      app_build_id,
      channel,
      ARRAY_CONCAT_AGG(ARRAY<STRUCT<
            metric STRING,
            metric_type STRING,
            key STRING,
            agg_type STRING,
            value FLOAT64
        >>
        [
            (metric, 'keyed-scalar-boolean', key, 'true', true_col),
            (metric, 'keyed-scalar-boolean', key, 'false', false_col)
        ]
    ) AS scalar_aggregates
from windowed
where _n = 1
group by 1,2,3,4,5,6
