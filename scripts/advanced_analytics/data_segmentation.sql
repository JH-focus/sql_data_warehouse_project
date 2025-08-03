/*
Data segmentation is done with the CASE WHEN clause, that works like a switch function and defines
different checks in order to segregate the data into different groups.
segment products into cost ranges and count how many producs fall into each segment
*/

WITH product_segments AS(
SELECT
p.product_key,
p.product_name,
p.cost,
CASE WHEN p.cost <= 100 THEN '<100'
	WHEN p.cost BETWEEN 100 AND 500 THEN '100-500'
	WHEN p.cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE '>1000'
END AS cost_range
FROM gold.dim_products p
)

SELECT
cost_range,
COUNT(DISTINCT product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;