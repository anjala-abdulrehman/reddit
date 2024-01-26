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
        )
   SELECT
    author_id,
    bit_count(weekly_activity_bin) as user_num_active_in_week
FROM weekly_activity_in_binary


