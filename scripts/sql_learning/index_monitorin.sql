--monitoring index usage
/*
with this psql command the table info is displayed, including information
about the indexes of the table
\d silver.erp_loc_a101;
*/

SELECT 
idx.schemaname,
idx.tablename,
idx.indexname,
idx.indexdef,
tbl.hasindexes,
tbl.hasrules,
tbl.hastriggers,
tbl.rowsecurity
FROM pg_indexes idx
LEFT JOIN pg_tables tbl
ON idx.tablename = tbl.tablename
WHERE idx.tablename = 'erp_loc_a101' AND idx.schemaname = 'silver';

--stats for indexes and tables
SELECT * FROM pg_indexes WHERE tablename = 'erp_loc_a101' AND schemaname = 'silver';
SELECT * FROM pg_tables WHERE tablename = 'erp_loc_a101' AND schemaname = 'silver';

--stats regarding all the tables, including information about index usage
SELECT * FROM pg_stat_all_tables 
WHERE schemaname IN('bronze','silver')
ORDER BY schemaname, relname;

SELECT *
FROM silver.erp_loc_a101
WHERE erp_cust_country = 'United States';