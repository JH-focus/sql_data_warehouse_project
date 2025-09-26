--Create a stored procedure/function for a customer summary
SELECT *
FROM gold.report_customers;

DROP FUNCTION gold.customer_summary;

--define the function, toge
CREATE OR REPLACE FUNCTION gold.customer_summary(p_segment text)
RETURNS TABLE (
  age_group text,
  customer_segment VARCHAR(20),
  total_customers bigint,
  total_sales numeric
)
LANGUAGE sql
AS $$
  SELECT
    rc.age_group,
	rc.customer_segment,
    COUNT(*)::bigint         AS total_customers,
    SUM(rc.total_sales)::numeric AS total_sales
  FROM gold.report_customers rc
  WHERE rc.customer_segment = p_segment
  GROUP BY rc.age_group, rc.customer_segment;
$$;

--this calls the function, providing the option to specify the parameter 
--
SELECT * FROM gold.customer_summary('regular');