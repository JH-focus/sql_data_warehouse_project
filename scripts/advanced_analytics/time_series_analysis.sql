SELECT
EXTRACT(YEAR FROM order_date) as order_year,
EXTRACT(MONTH FROM order_date) as order_month,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

--for correct aggregation, it is important, not to use the same column name as the original column
--correct formatting in postgreSQL is achieved by the TO_CHAR function, only disadvantage -> result is text not numeric
--Mon abbreviates the Months, while MM displays the numbers and Month the entire month name
--IMPORTANT: the sorting with TO_CHAR is messed up, because SQL orders strings by alphabet
SELECT
TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-Mon') as order_date_agg,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date_agg
ORDER BY order_date_agg;

--in order to correctly display the order dates, the DATE_TRUNC() Function ist necessary
--the return value is not as intuitive, but in a second column it can be displayed as
--wanted by the TO_CHAR() function
SELECT
DATE_TRUNC('month', order_date) as order_date_agg,
TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-Month') as order_date_clean,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date_agg
ORDER BY order_date_agg;