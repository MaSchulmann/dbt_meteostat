WITH departures AS ( 
    SELECT 
        flight_date,
        origin AS airport_code,
        COUNT(DISTINCT dest) AS nunique_to,
        COUNT(sched_dep_time) AS dep_planned,
        SUM(cancelled) AS dep_cancelled,
        SUM(diverted) AS dep_diverted,
        COUNT(arr_time) AS dep_n_flights
        ,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- BONUS TASK
        ,COUNT(DISTINCT airline) AS dep_nunique_airlines -- BONUS TASK
    FROM {{ ref('prep_flights') }}
    WHERE origin IN (SELECT DISTINCT airport_code FROM {{ ref('prep_weather_daily') }})
    GROUP BY origin, flight_date
    ORDER BY origin, flight_date
),
arrivals AS (
    SELECT 
        flight_date,
        dest AS airport_code,
        COUNT(DISTINCT origin) AS nunique_from,
        COUNT(sched_dep_time) AS arr_planned,
        SUM(cancelled) AS arr_cancelled,
        SUM(diverted) AS arr_diverted,
        COUNT(arr_time) AS arr_n_flights
        ,COUNT(DISTINCT tail_number) AS arr_nunique_tails -- BONUS TASK
        ,COUNT(DISTINCT airline) AS arr_nunique_airlines -- BONUS TASK
    FROM {{ ref('prep_flights') }}
    WHERE dest IN (SELECT DISTINCT airport_code FROM {{ ref('prep_weather_daily') }})
    GROUP BY dest, flight_date
    ORDER BY dest, flight_date
),
total_stats AS (
    SELECT 
        d.flight_date,
        d.airport_code,
        d.nunique_to,
        a.nunique_from,
        d.dep_planned + a.arr_planned AS total_planned,
        d.dep_cancelled + a.arr_cancelled AS total_cancelled,
        d.dep_diverted + a.arr_diverted AS total_diverted,
        d.dep_n_flights + a.arr_n_flights AS total_flights
        ,((d.dep_nunique_tails + a.arr_nunique_tails)::NUMERIC/2) AS nunique_tails -- BONUS
        ,((d.dep_nunique_airlines + a.arr_nunique_airlines)::NUMERIC/2) AS nunique_airlines -- BONUS
    FROM departures d
    JOIN arrivals a
        ON d.flight_date = a.flight_date
       AND d.airport_code = a.airport_code
)
SELECT 
    t.*,
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh
FROM total_stats t
LEFT JOIN {{ ref('prep_weather_daily') }} w
    ON t.airport_code = w.airport_code AND t.flight_date = w.date
ORDER BY t.total_diverted DESC