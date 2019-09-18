CREATE TEMP FUNCTION
  udf_get_theme(theme STRUCT<app_disabled BOOL, blocklisted BOOL, description STRING, has_binary_components BOOL, id STRING, install_day INT64, name STRING, scope INT64, update_day INT64, user_disabled BOOL, version STRING>) AS ((
  SELECT
    AS STRUCT
    IFNULL(theme.id, "MISSING") as id,
    theme.* EXCEPT (id)
));

-- Tests

SELECT
  assert_equals(udf_get_theme(theme), theme),
  assert_equals(udf_get_theme(missing_theme).id, "MISSING")
FROM
(SELECT AS VALUE STRUCT(STRUCT("foo" AS id, false AS blocklisted, "desc" AS description, "example theme" AS name, false AS user_disabled, false AS app_disabled, "1.0.1" AS version, 0 as scope, true AS foreign_install, true AS has_binary_components, 7490 as install_day, 7551 as update_day) as theme,
STRUCT(NULL AS id, false AS blocklisted, "desc" AS description, "example theme" AS name, false AS user_disabled, false AS app_disabled, "1.0.1" AS version, 0 as scope, true AS foreign_install, true AS has_binary_components, 7490 as install_day, 7551 as update_day) as missing_theme))
