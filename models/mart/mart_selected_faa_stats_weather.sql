WITH weekly_weather AS (
    SELECT station_id AS faa
         , DATE_TRUNC('week', date) AS week
         , ROUND(AVG(min_temp_c), 2) AS avg_min_temp_c
         , ROUND(AVG(max_temp_c), 2) AS avg_max_temp_c
         , SUM(precipitation_mm) AS total_precipitation_mm
         , MAX(max_snow_mm) AS max_snow_mm
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