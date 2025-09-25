--Create a stored procedure/function for a customer summary
SELECT *
FROM gold.report_customers;

DROP FUNCTION gold.customer_summary;

--define the function, together with the output table and the 
--parameters, that you want to make optional, as well as a default value 
CREATE OR REPLACE FUNCTION gold.customer_summary(p_segment text DEFAULT 'vip')
RETURNS TABLE (
  age_group VARCHAR(20),
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
--or use the default value, in this case 'vip'
SELECT * FROM gold.customer_summary();