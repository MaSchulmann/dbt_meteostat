WITH weekly_weather AS (
    SELECT station_id AS faa
         , DATE_TRUNC('week', date) AS week
         , ROUND(AVG(temp_min), 2) AS avg_temp_min
         , ROUND(AVG(temp_max), 2) AS avg_temp_max
         , SUM(precipitation) AS total_precipitation
         , SUM(snowfall) AS total_snowfall
         , ROUND(AVG(wind_dir_avg), 2) AS avg_wind_dir
         , ROUND(AVG(wind_speed_avg), 2) AS avg_wind_speed
         , MAX(wind_peakgust) AS max_wind_gust
    FROM {{ ref('staging_weather_daily') }}
    GROUP BY station_id, DATE_TRUNC('week', date)
),

airport_info AS (
    SELECT faa, name, city, country
    FROM {{ ref('staging_airports') }}
)

SELECT w.*
     , a.name
     , a.city
     , a.country
FROM weekly_weather w
LEFT JOIN airport_info a
  ON w.faa = a.faa
ORDER BY week, w.faa