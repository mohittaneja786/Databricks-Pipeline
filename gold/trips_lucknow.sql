CREATE OR REPLACE VIEW project_tp.gold.fact_trips_lucknow
AS (
SELECT *
FROM project_tp.gold.fact_trips
WHERE city_id = 'UP01'
);