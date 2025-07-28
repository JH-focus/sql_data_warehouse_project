--check date column order_date
SELECT 
MIN(order_date),
MAX(order_date),
MAX(EXTRACT(YEAR FROM order_date)) - MIN(EXTRACT(YEAR FROM order_date)) AS order_range_years
FROM gold.fact_sales;

--check date column shipping_date
SELECT 
MIN(shipping_date),
MAX(shipping_date),
MAX(EXTRACT(YEAR FROM shipping_date)) - MIN(EXTRACT(YEAR FROM shipping_date)) AS shipping_range_years
FROM gold.fact_sales;

--check date column due_date
SELECT 
MIN(due_date),
MAX(due_date),
MAX(EXTRACT(YEAR FROM due_date)) - MIN(EXTRACT(YEAR FROM due_date)) AS due_range_years
FROM gold.fact_sales;

--check date column birthdate
SELECT 
MIN(birthdate),
MAX(birthdate),
-- the TO_CHAR documentation can be found here: https://www.postgresql.org/docs/10/functions-formatting.html
-- the 'FM999.99' stands for fill mode. Using nines means the number will be printed without 
-- leading or trailing digits, if they are irrelevant - meaning equalling zero
TO_CHAR(EXTRACT(EPOCH FROM (MAX(birthdate)::timestamp 
- MIN(birthdate)::timestamp))/ 31536000, 'FM999.00') AS age_range_years_formatted,

TO_CHAR(EXTRACT(EPOCH FROM (NOW()::timestamp 
- MAX(birthdate)::timestamp)/31536000), 'FM9999') AS age_youngest_customer,

-- the TO_CHAR documentation can be found here: https://www.postgresql.org/docs/10/functions-formatting.html
-- the 'FM0000' stands for fill mode. Using zeroes means the number will be printed with 
-- leading or trailing digits, even if irrelevant
TO_CHAR(EXTRACT(EPOCH FROM (NOW()::timestamp 
- MIN(birthdate)::timestamp)/31536000), 'FM0000.00') AS age_oldest_customer,

MAX(EXTRACT(YEAR FROM birthdate)) - MIN(EXTRACT(YEAR FROM birthdate)) AS age_range_years
FROM gold.dim_customers;