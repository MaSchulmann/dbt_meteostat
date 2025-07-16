WITH flights_stats AS (
    SELECT 
        origin,
        dest,
        COUNT(flight_number) AS n_flights,
        COUNT(DISTINCT tail_number) AS nunique_tails,
        COUNT(DISTINCT airline) AS nunique_airlines,
        -- Average actual elapsed time (minutes -> interval)
        (AVG(actual_elapsed_time) * INTERVAL '1 minute') AS avg_actual_elapsed_time,
        -- Average arrival delay (minutes -> interval)
        (AVG(arr_delay) * INTERVAL '1 minute') AS avg_arr_delay,
        MIN(arr_delay_interval) AS min_arr_delay,
        MAX(arr_delay_interval) AS max_arr_delay,
        SUM(cancelled) AS total_cancelled,
        SUM(diverted) AS total_diverted
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, dest
),
add_names AS (
    SELECT 
        o.city AS origin_city,
        d.city AS dest_city,
        o.name AS origin_name,
        d.name AS dest_name,
        o.country AS origin_country,
        d.country AS dest_country,
        f.*
    FROM flights_stats f
    LEFT JOIN {{ ref('prep_airports') }} o ON f.origin = o.faa
    LEFT JOIN {{ ref('prep_airports') }} d ON f.dest = d.faa
)
SELECT *
FROM add_names
ORDER BY origin, dest