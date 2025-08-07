--Which categories contribute the most to the overall sales?
--CTE category_sales in order to most clearly structure the code
--CTE stands for common table expressions
WITH category_sales AS(
SELECT 
category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category
)

SELECT 
category,
total_sales,
--CONCAT() can be used to add symbols -> this is also possible with the TO_CHAR() function
CONCAT(ROUND((total_sales / SUM(total_sales) OVER () * 100),2), '%') AS percentage_of_total,
SUM(total_sales) OVER () overall_sales
FROM category_sales;