/*
Common table expressions in order to understand the concepts
*/


--define the first standalone cte
WITH cte_total_sales AS
(SELECT
c.customer_key,
SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key
ORDER BY customer_key),

--define the second standalone cte 
cte_last_order AS
(SELECT
c.customer_key,
MAX(f.order_date) last_order_date
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key),

--create nested cte
cte_rank_sales AS
(SELECT
customer_key,
DENSE_RANK() OVER(ORDER BY total_sales DESC) AS customer_rank
FROM cte_total_sales),

--create a second cte
cte_customer_segments AS
(SELECT
customer_key,
total_sales,
CASE WHEN total_sales > 10000 THEN 'high'
	WHEN total_sales > 5000 THEN 'medium'
	ELSE 'low'
END AS customer_segment
FROM cte_total_sales)

--use the cte as you wish in your main query - FROM, JOIN, WHERE...f
SELECT
c.customer_key,
c.first_name,
c.last_name,
cts.total_sales,
clo.last_order_date,
ccs.customer_segment,
crs.customer_rank
FROM gold.dim_customers c
LEFT JOIN cte_total_sales cts
ON c.customer_key = cts.customer_key
LEFT JOIN cte_last_order clo
ON c.customer_key = clo.customer_key
LEFT JOIN cte_rank_sales crs
ON c.customer_key = crs.customer_key
LEFT JOIN cte_customer_segments ccs
ON c.customer_key = ccs.customer_key
ORDER BY customer_rank;

--loop and generate a sequence of numbers from 1 to 20
--the cte has to be defined as "WITH RECURSIVE" in postgreSQL
--postgresql doesn't limit the amount of times a recursive query can be executed
WITH RECURSIVE series AS(
	--Anchor query
	SELECT
	1 AS my_number
	UNION ALL
	--Recursive query
	SELECT
	my_number + 1
	FROM Series
	--define the number of iterations -> very important
	WHERE my_number < 1000
)

--Main query that gets all the data from the recursive cte
SELECT * FROM series;

--the recursive query is useful when there is a hierarchical structure you can
--iterate over (e.g. employees and their managers)



