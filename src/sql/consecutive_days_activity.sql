with  users_with_date_aggregated AS
   (
    SELECT
    author_id,
    array_agg(DISTINCT etl_ts) as active_days_for_user
    FROM dim_all_users_stg
    WHERE extract(week FROM etl_ts) = 2
    AND author_id <> 'None'
    GROUP BY 1
    ),
    sequence_data AS
   (
    SELECT
    author_id
   , active_days_for_user
   , t.sequence_date::date as eow_date
    FROM users_with_date_aggregated
    CROSS JOIN generate_series(DATE '2024-01-08'
   , DATE '2024-01-14'
   , INTERVAL '1 day') AS t(sequence_date)
    ),
    summarised AS
(SELECT
    author_id,
    active_days_for_user,
    eow_date,
    POW(2, 31 - (DATE '2024-01-14' -   eow_date)) as pow_2
FROM sequence_data),
    weekly_activity_in_binary AS
        (
           SELECT
    author_id,
      CAST(
        SUM(
          CASE
            WHEN eow_date = ANY(active_days_for_user) THEN pow_2 ELSE 0 END
        ) AS BIGINT
      )::bit(32) AS weekly_activity_bin
FROM summarised
GROUP BY 1
        ),
    stats AS
        (
          SELECT
    author_id,
    weekly_activity_bin,
    substring(weekly_activity_bin  FROM 1 FOR 7) AS weekly_activity
  FROM
    weekly_activity_in_binary
        )
SELECT
    author_id,
    weekly_activity,
    CASE
        WHEN  weekly_activity & '1111111'::bit(7) = '1111111'::bit(7) THEN 1
        ELSE 0
    END as is_active_7_consecutive_days,
    CASE
        WHEN  (weekly_activity & '1111110'::bit(7) = '1111110'::bit(7))  THEN 1
        WHEN  (weekly_activity & '0111111'::bit(7) = '0111111'::bit(7))  THEN 1
        ELSE 0 END as is_active_6_consecutive_days,
        CASE
        WHEN  (weekly_activity & '1111101'::bit(7) = '1111101'::bit(7))  THEN 1
        WHEN  (weekly_activity & '1011111'::bit(7) = '1011111'::bit(7))  THEN 1
        ELSE 0 END as is_active_5_consecutive_days
FROM stats