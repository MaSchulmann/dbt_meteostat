WITH airports_reorder AS (
    SELECT faa
         , name
         , country
         , region        -- moved region right after country
         , city
         , lat
         , lon
         , alt
         , tz
         , dst
    FROM {{ ref('staging_airports') }}
)
SELECT *
FROM airports_reorder