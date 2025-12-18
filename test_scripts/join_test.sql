\c energy_data_vault;

SELECT 
    dl.city,
    SUM(ft.annual_consume) AS total_consumption,
    ROUND((SUM(ft.annual_consume) * 100.0 / SUM(SUM(ft.annual_consume)) OVER ())::numeric, 2) AS consumption_percent,
    RANK() OVER (ORDER BY SUM(ft.annual_consume) DESC) AS consumption_rank,
    ROUND(AVG(ft.perc_of_active_connections)::numeric, 2) AS avg_active_connections_percent
FROM gold.fact_table ft
JOIN gold.dim_location dl ON ft.location_id = dl.location_id
GROUP BY dl.city
ORDER BY total_consumption DESC
LIMIT 20;