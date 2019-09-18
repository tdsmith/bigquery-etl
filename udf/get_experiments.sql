CREATE TEMP FUNCTION udf_get_experiments(experiments ARRAY<STRUCT<key STRING, value STRUCT<branch STRING>>>)
RETURNS STRUCT<key_value ARRAY<STRUCT<key STRING, value STRING>>> AS ((
SELECT STRUCT(ARRAY_AGG((
    SELECT AS STRUCT
        key AS key,
        value.branch AS value)) as key_value)
    FROM UNNEST(experiments)));
