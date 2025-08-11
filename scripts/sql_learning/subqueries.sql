--1.) subquery in the FROM--section of another query
/*
Task: find the products that have a price higher than the average price of all products
*/
--main query
SELECT
*
FROM(
	--subquery
	SELECT 
	p.product_name,
	f.sales_amount/f.quantity AS price,
	AVG(f.sales_amount/f.quantity) OVER () AS avg_price
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key)
WHERE price >= avg_price;


/*
Task: Rank customers based on ther total amount of sales
*/
--main query
SELECT
*,
DENSE_RANK() OVER (ORDER BY total_sales DESC) AS customer_rank
FROM
	--subquery
	(SELECT
	f.customer_key,
	SUM(f.sales_amount) AS total_sales
	FROM gold.fact_sales f
	GROUP BY customer_key);

--2.)subquery in the SELECT-section of a query -> only possible for scalar columns, not regular columns

--main query
SELECT
p.product_key,
p.product_name,
p.cost,
--subquery
(SELECT COUNT(*)FROM gold.fact_sales) AS total_orders
FROM gold.dim_products p;

--3.) subquery in the JOIN-section of a query -> JOIN a table with the results of a subquery

--main query
SELECT
c.*,
p.total_orders
FROM gold.dim_customers c
LEFT JOIN
	--subquery
	(SELECT
	f.customer_key,
	COUNT(*) AS total_orders
	FROM gold.fact_sales f
	GROUP BY f.customer_key) p
ON p.customer_key = c.customer_key;

--4.) subquery in the WHERE-section of a query -> with comparison operators only possible, if the subquery is scalar

--main query
SELECT
p.product_key,
p.cost,
ROUND((SELECT AVG(p.cost) FROM gold.dim_products p), 2) as avg_price
FROM gold.dim_products p
WHERE cost > 
--subquery
(SELECT AVG(p.cost) FROM gold.dim_products p);


--if used with logical operators the limitation to scalarity is not mandatory

--main query
SELECT
*
FROM gold.fact_sales f
WHERE f.customer_key IN 
	--subquery
	(SELECT
	c.customer_key
	FROM gold.dim_customers c
	WHERE country = 'Germany');

--super inefficient, but does the job -> do not use this
SELECT 
*
FROM gold.fact_sales
WHERE sales_amount > ANY (SELECT sales_amount FROM gold.fact_sales WHERE sales_amount > 900);


--correlated subquery -> more inefficient, since the subquery is executed for every row of the main query
--slow and hard on the hardware
SELECT
*,
(SELECT COUNT(*) FROM gold.dim_customers c WHERE f.customer_key = c.customer_key) total_sales
FROM gold.fact_sales f;


SELECT
*
FROM gold.fact_sales f
WHERE NOT EXISTS 
	(SELECT
	1 --static value is best practice when using a subquery in the EXISTS-section
	FROM gold.dim_customers c
	WHERE country = 'Germany'
	AND f.customer_key = c.customer_key);