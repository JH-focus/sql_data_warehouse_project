/* Analyze the yearly performance of products by comparing their sales to both the
average sales performance of the product and the previous year's sales */
WITH yearly_product_sales AS (
SELECT
DATE_TRUNC('year', f.order_date) as order_year,
p.product_name,
SUM(f.sales_amount) current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE order_date IS NOT NULL
GROUP BY order_year, p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
ROUND(AVG(current_sales) OVER (PARTITION BY product_name), 2) as avg_sales,
current_sales - ROUND(AVG(current_sales) OVER (PARTITION BY product_name), 2)  as diff_avg,
CASE WHEN (current_sales - ROUND(AVG(current_sales) OVER (PARTITION BY product_name), 2)) > 0 THEN 'above avg'
	WHEN (current_sales - ROUND(AVG(current_sales) OVER (PARTITION BY product_name), 2)) < 0 THEN 'below avg'
	ELSE 'avg'
END AS avg_change,
--year over year analysis 
--LAG() OVER (PARTITION BY) function is perfect for year to previous year comparisons 
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) prev_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_prev_year,
CASE WHEN (current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year)) > 0 THEN 'increase'
	WHEN (current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year)) < 0 THEN 'decrease'
	ELSE 'no change'
END AS prev_year_change
FROM yearly_product_sales
ORDER BY product_name, order_year;


