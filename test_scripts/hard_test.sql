\c energy_data_vault;

SELECT 
    purchase_area,
    type_of_connection,
    num_locations,
    total_consumption,
    avg_delivery,
    RANK() OVER (PARTITION BY purchase_area ORDER BY total_consumption DESC) AS connection_type_rank_in_area,
    ROUND((total_consumption * 100.0 / NULLIF(SUM(total_consumption) OVER (PARTITION BY purchase_area), 0))::numeric, 2) AS percent_in_area
FROM (
    SELECT 
        dl.purchase_area,
        dl.type_of_connection,
        COUNT(DISTINCT ft.location_id) AS num_locations,
        SUM(ft.annual_consume) AS total_consumption,
        ROUND(AVG(ft.delivery_perc)::numeric, 2) AS avg_delivery
    FROM gold.fact_table ft
    JOIN gold.dim_location dl ON ft.location_id = dl.location_id
    GROUP BY dl.purchase_area, dl.type_of_connection
) subquery
ORDER BY purchase_area, total_consumption DESC;