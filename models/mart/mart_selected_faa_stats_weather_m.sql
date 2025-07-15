WITH weather_airports AS (
    -- Only airports for which we have weather data
    SELECT DISTINCT airport_code
    FROM {{ ref('prep_weather_daily') }}
),

-- Filter flights to just those airports
filtered_flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
    WHERE origin IN (SELECT airport_code FROM weather_airports)
       OR dest IN (SELECT airport_code FROM weather_airports)
),

-- Departure stats by airport and day
departure_stats AS (
    SELECT 
        origin AS airport_code,
        flight_date AS date,
        COUNT(DISTINCT dest) AS unique_departures,
        COUNT(*) AS total_departures,
        COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_departures,
        COUNT(*) FILTER (WHERE diverted = 1) AS diverted_departures,
        COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS completed_departures,
        COUNT(DISTINCT tail_number) AS unique_planes_dep,
        COUNT(DISTINCT airline) AS unique_airlines_dep
    FROM filtered_flights
    GROUP BY origin, flight_date
),

-- Arrival stats by airport and day
arrival_stats AS (
    SELECT 
        dest AS airport_code,
        flight_date AS date,
        COUNT(DISTINCT origin) AS unique_arrivals,
        COUNT(*) AS total_arrivals,
        COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_arrivals,
        COUNT(*) FILTER (WHERE diverted = 1) AS diverted_arrivals,
        COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS completed_arrivals,
        COUNT(DISTINCT tail_number) AS unique_planes_arr,
        COUNT(DISTINCT airline) AS unique_airlines_arr
    FROM filtered_flights
    GROUP BY dest, flight_date
),

-- Combine arrivals and departures, ensure all relevant airports included
combined_stats AS (
    SELECT 
        d.airport_code,
        d.date,
        d.unique_departures,
        a.unique_arrivals,
        d.total_departures + a.total_arrivals AS total_flights,
        d.cancelled_departures + a.cancelled_arrivals AS total_cancelled,
        d.diverted_departures + a.diverted_arrivals AS total_diverted,
        d.completed_departures + a.completed_arrivals AS total_completed,
        ROUND((d.unique_planes_dep + a.unique_planes_arr) / 2.0, 0) AS avg_unique_planes,
        ROUND((d.unique_airlines_dep + a.unique_airlines_arr) / 2.0, 0) AS avg_unique_airlines
    FROM departure_stats d
    LEFT JOIN arrival_stats a 
        ON d.airport_code = a.airport_code AND d.date = a.date

    UNION

    SELECT 
        a.airport_code,
        a.date,
        d.unique_departures,
        a.unique_arrivals,
        d.total_departures + a.total_arrivals AS total_flights,
        d.cancelled_departures + a.cancelled_arrivals AS total_cancelled,
        d.diverted_departures + a.diverted_arrivals AS total_diverted,
        d.completed_departures + a.completed_arrivals AS total_completed,
        ROUND((d.unique_planes_dep + a.unique_planes_arr) / 2.0, 0) AS avg_unique_planes,
        ROUND((d.unique_airlines_dep + a.unique_airlines_arr) / 2.0, 0) AS avg_unique_airlines
    FROM arrival_stats a
    LEFT JOIN departure_stats d 
        ON a.airport_code = d.airport_code AND a.date = d.date
    WHERE d.airport_code IS NULL
),

-- Add weather columns
weather AS (
    SELECT
        airport_code,
        date,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction,
        avg_wind_speed_kmh,
        wind_peakgust_kmh
    FROM {{ ref('prep_weather_daily') }}
),

-- Add airport info (optional)
airport_info AS (
    SELECT faa AS airport_code, name, city, country
    FROM {{ ref('staging_airports') }}
)

SELECT
    s.*,
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh,
    a.name,
    a.city,
    a.country
FROM combined_stats s
LEFT JOIN weather w
  ON s.airport_code = w.airport_code AND s.date = w.date
LEFT JOIN airport_info a
  ON s.airport_code = a.airport_code
ORDER BY s.date, s.airport_code



