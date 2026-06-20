CREATE OR REPLACE VIEW project_tp.gold.fact_trips_mysore
AS (
SELECT *
FROM project_tp.gold.fact_trips
WHERE city_id = 'KA01'
);