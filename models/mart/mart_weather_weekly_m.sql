WITH weekly_aggregates AS (
    SELECT airport_code
         , DATE_TRUNC('week', date) AS week

         -- Temperatures
         , MIN(min_temp_c) AS min_temp_c_week
         , MAX(max_temp_c) AS max_temp_c_week
         , ROUND(AVG(avg_temp_c), 2) AS avg_temp_c_week

         -- Precipitation and snow
         , SUM(precipitation_mm) AS total_precipitation_mm
         , MAX(max_snow_mm) AS max_snow_mm_week

    FROM {{ ref('prep_weather_daily') }}
    GROUP BY airport_code, DATE_TRUNC('week', date)
)

SELECT *
FROM weekly_aggregates
ORDER BY week, airport_code