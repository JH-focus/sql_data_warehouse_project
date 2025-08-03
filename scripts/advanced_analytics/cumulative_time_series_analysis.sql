--Calculate the total sales per month
--Calculate the running total sales
--Window function SUM() in combination with OVER() works perfectly for the running total
SELECT
order_date_agg,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date_agg ASC) as running_total
FROM(
SELECT
DATE_TRUNC('month', order_date) as order_date_agg,
SUM(sales_amount) as total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date_agg
)
ORDER BY order_date_agg;


--in order to partition the running total sales by year
--ROUND() in order to display the correct number of digits after the decimal point
SELECT
order_date_agg,
total_sales,
SUM(total_sales) OVER (PARTITION BY DATE_TRUNC('year', order_date_agg) ORDER BY order_date_agg ASC) as running_total,
ROUND(AVG(avg_price) OVER (PARTITION BY DATE_TRUNC('year', order_date_agg) ORDER BY order_date_agg ASC), 2) as moving_avg_price
FROM(
SELECT
DATE_TRUNC('month', order_date) as order_date_agg,
SUM(sales_amount) as total_sales,
AVG(price) as avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date_agg
)
ORDER BY order_date_agg;