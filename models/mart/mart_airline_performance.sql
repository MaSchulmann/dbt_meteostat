WITH airline_stats AS (
    SELECT airline
         , COUNT(*) AS total_flights

         -- Delays: average when available
         , ROUND(AVG(dep_delay), 2) AS avg_dep_delay
         , ROUND(AVG(arr_delay), 2) AS avg_arr_delay

         -- Cancellation and diversion rate: % of total
         , ROUND(AVG(cancelled::INT) * 100, 2) AS cancellation_rate_pct
         , ROUND(AVG(diverted::INT) * 100, 2) AS diversion_rate_pct

    FROM {{ ref('prep_flights') }}
    GROUP BY airline
)

SELECT *
FROM airline_stats
ORDER BY cancellation_rate_pct DESC