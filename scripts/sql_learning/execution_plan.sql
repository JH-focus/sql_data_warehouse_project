/*
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM silver.crm_cust_info;
*/

/*
=============================================
1.) get the intended exectution plan
=============================================
note: this does NOT execute the query
*/

EXPLAIN (FORMAT JSON)
SELECT *
FROM silver.crm_cust_info;


/*
=============================================
2.) get the actual exectution plan
=============================================
note: this DOES execute the query
*/

EXPLAIN (ANALYZE)
SELECT *
FROM silver.crm_cust_info;

--the ANALYZE shows actual runtime statistics (row counts, times, loops)
--but it actually executes the query

/*
=============================================
2.) get extra information
=============================================
note: this DOES execute the query
*/

EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, SUMMARY, SETTINGS, WAL, FORMAT JSON)
SELECT *
FROM silver.crm_cust_info;

--VERBOSE: fetches output column lists, subquery targets, function/projection details
--COSTS: shows estimated startup and total costs
--BUFFERS: requires ANALYZE and shows buffer activity (great for spotting I/O bottlenecks)
--TIMING: ON by default with ANALYZE - shows per-node execution time 
--SUMMARY: ON by default with ANALYZE - show overall planning and execution times
--SETTINGS: displays non-default planner settings (like changed enable_seqscan)
--WAL: show write-ahead log (WAL) usage per node - number of records, bytes, useful for monitoring write-heavy queries
--GENERIC_PLAN: forces showing a generic cached plan instead of a custom one (works only without ANALYZE)
--FORMAT { TEXT | JSON | YAML | XML }



--example with a little more complex execution plan

EXPLAIN (ANALYZE)
SELECT 
p.product_name,
SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
OPTION (HASH JOIN)


