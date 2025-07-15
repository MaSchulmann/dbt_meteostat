WITH weekly_weather AS (
    SELECT
        airport_code,
        DATE_TRUNC('week', date) AS week,

        -- Temperatures
        ROUND(AVG(avg_temp_c), 2) AS avg_temp_c_week,
        MIN(min_temp_c) AS min_temp_c_week,
        MAX(max_temp_c) AS max_temp_c_week,

        -- Precipitation & Snow
        SUM(precipitation_mm) AS total_precipitation_mm,
        MAX(max_snow_mm) AS max_snow_mm_week,

        -- Wind & Pressure
        ROUND(AVG(avg_wind_direction), 1) AS avg_wind_direction_week,
        ROUND(AVG(avg_wind_speed_kmh), 1) AS avg_wind_speed_kmh_week,
        MAX(wind_peakgust_kmh) AS max_wind_peakgust_kmh_week,
        ROUND(AVG(avg_pressure_hpa), 1) AS avg_pressure_hpa_week,

        -- Sun
        SUM(sun_minutes) AS total_sun_minutes_week,

        -- Date Features (Mode)
        MIN(date_year) AS year,  -- year of the week
        MIN(cw) AS cw,           -- week number
        MODE() WITHIN GROUP (ORDER BY month_name) AS most_common_month_name,
        MODE() WITHIN GROUP (ORDER BY season) AS most_common_season
    FROM {{ ref('prep_weather_daily') }}
    GROUP BY airport_code, DATE_TRUNC('week', date)
)

SELECT *
FROM weekly_weather
ORDER BY week, airport_code



