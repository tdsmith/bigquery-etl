CREATE TEMP FUNCTION
udf_histogram_to_threshold_count(histogram STRING, threshold INT64) AS ((
    SELECT
    IFNULL(SUM(value), 0)
    FROM
      UNNEST(udf_json_extract_histogram(histogram).values)
    WHERE
      key >= threshold));

-- Tests

SELECT
  assert_equals(17, udf_histogram_to_threshold_count('{"values":{"0":1,"1":2, "4": 10, "8": 7}}', 4)),
  assert_equals(0, udf_histogram_to_threshold_count('{}', 1)),
  assert_equals(0, udf_histogram_to_threshold_count('{"values":{"0":0}}', 1)),
  assert_equals(3, udf_histogram_to_threshold_count('{"values":{"5":1, "6":3}}', 6))
