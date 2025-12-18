\c energy_data_vault;

SELECT 
    year,
    SUM(annual_consume) AS total_consumption,
    ROUND(AVG(delivery_perc)::numeric, 2) AS avg_delivery_percent,
    ROUND(AVG(perc_of_active_connections)::numeric, 2) AS avg_active_percent,
    SUM(SUM(annual_consume)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_consumption,
    LAG(SUM(annual_consume), 1) OVER (ORDER BY year) AS prev_year_consumption,
    ROUND(((SUM(annual_consume) - LAG(SUM(annual_consume), 1) OVER (ORDER BY year)) * 100.0 / 
          NULLIF(LAG(SUM(annual_consume), 1) OVER (ORDER BY year), 0))::numeric, 2) AS consumption_growth_percent
FROM gold.fact_table
GROUP BY year
ORDER BY year;