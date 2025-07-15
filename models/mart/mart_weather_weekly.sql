WITH weekly_aggregates AS (
    SELECT faa
         , DATE_TRUNC('week', date) AS week

         -- Temperature: use min and max of daily values
         , MIN(temp_min) AS min_temp_week
         , MAX(temp_max) AS max_temp_week
         , ROUND(AVG(temp_min), 2) AS avg_temp_min
         , ROUND(AVG(temp_max), 2) AS avg_temp_max

         -- Precipitation/snow: add it up
         , SUM(precipitation) AS total_precipitation
         , SUM(snowfall) AS total_snowfall

         -- Wind: use average and max
         , ROUND(AVG(wind_dir_avg), 2) AS avg_wind_dir
         , ROUND(AVG(wind_speed_avg), 2) AS avg_wind_speed
         , MAX(wind_peakgust) AS max_wind_gust

         -- Optional: mode of weekday or condition (requires custom aggregate)
         -- , MODE() WITHIN GROUP (ORDER BY weekday) AS most_common_weekday

    FROM {{ ref('prep_weather_daily') }}
    GROUP BY faa, DATE_TRUNC('week', date)
)

SELECT *
FROM weekly_aggregates
ORDER BY week, faa