--Create a stored procedure for a customer summary
SELECT *
FROM gold.report_customers;

DROP FUNCTION gold.vip_customer_summary


CREATE OR REPLACE PROCEDURE gold.vip_customer_summary()
LANGUAGE plpgsql
AS $$
BEGIN
	SELECT
		age_group,
		COUNT(*) AS total_customers,
		SUM(total_sales) AS total_sales
	FROM gold.report_customers
	WHERE customer_segment = 'vip'
	GROUP BY age_group;
END;
$$;


CALL gold.vip_customer_summary();