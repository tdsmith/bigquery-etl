CREATE TEMP FUNCTION
udf_histogram_to_threshold_count(histogram STRING, threshold INT64) AS ((
    SELECT
    SUM(value)
    FROM
      UNNEST(udf_json_extract_histogram(histogram).values)
    WHERE
      key >= threshold));

-- Tests

SELECT
  assert_equals(13, udf_histogram_to_threshold_count('{"values":{"0":1,"1":2, "4": 10, "8": 7}}', 4)),
  assert_null(udf_histogram_to_threshold_count('{}', 1)),
  assert_equals(0, udf_histogram_to_threshold_count('{"values":{"0":0}}', 1)),
  assert_equals(1, udf_histogram_to_count(5, '{"values":{"5":1, "6":3}}'))
