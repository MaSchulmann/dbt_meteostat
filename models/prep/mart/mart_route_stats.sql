WITH all_flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

route_stats AS (
    SELECT origin
         , dest
         , COUNT(*) AS total_flights
         , COUNT(DISTINCT tail_number) AS unique_airplanes
         , COUNT(DISTINCT airline) AS unique_airlines
         , ROUND(AVG(actual_elapsed_time), 2) AS avg_elapsed_time
         , ROUND(AVG(arr_delay), 2) AS avg_arrival_delay
         , MAX(arr_delay) AS max_arrival_delay
         , MIN(arr_delay) AS min_arrival_delay
         , COUNT(*) FILTER (WHERE cancelled = 1) AS total_cancelled
         , COUNT(*) FILTER (WHERE diverted = 1) AS total_diverted
    FROM all_flights
    GROUP BY origin, dest
),

origin_airport AS (
    SELECT faa AS origin
         , name AS origin_name
         , city AS origin_city
         , country AS origin_country
    FROM {{ ref('prep_airports') }}
),

dest_airport AS (
    SELECT faa AS dest
         , name AS dest_name
         , city AS dest_city
         , country AS dest_country
    FROM {{ ref('prep_airports') }}
)

SELECT r.*
     , o.origin_name
     , o.origin_city
     , o.origin_country
     , d.dest_name
     , d.dest_city
     , d.dest_country
FROM route_stats r
LEFT JOIN origin_airport o USING (origin)
LEFT JOIN dest_airport d USING (dest)