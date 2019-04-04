WITH _current AS (
  SELECT
    submission_date_s3 AS last_seen_date,
    * EXCEPT (submission_date_s3),
    0 AS days_since_seen,
    -- For measuring Active MAU, where this is the days since this
    -- client_id was an Active User as defined by
    -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
    IF(scalar_parent_browser_engagement_total_uri_count_sum >= 5,
      0,
      NULL) AS days_since_visited_5_uri
  FROM
    telemetry.clients_daily_v6
  WHERE
    submission_date_s3 = DATE '{{ds}}'
), _previous AS (
  SELECT
    * EXCEPT (submission_date,
      generated_time) REPLACE(
      -- omit values outside 28 day window
      IF(days_since_visited_5_uri < 27,
        days_since_visited_5_uri,
        NULL) AS days_since_visited_5_uri)
  FROM
    telemetry.clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(DATE '{{ds}}', INTERVAL 1 DAY)
    AND days_since_seen < 27
)
SELECT
  DATE '{{ds}}' AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  IF(_current.client_id IS NOT NULL,
    _current,
    _previous).* EXCEPT (days_since_seen,
    days_since_visited_5_uri),
  COALESCE(_current.days_since_seen,
    _previous.days_since_seen + 1) AS days_since_seen,
  COALESCE(_current.days_since_visited_5_uri,
    _previous.days_since_visited_5_uri + 1) AS days_since_visited_5_uri
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
