CREATE OR REPLACE VIEW project_tp.gold.fact_trips_indore
AS (
SELECT *
FROM project_tp.gold.fact_trips
WHERE city_id = 'MP01'
);