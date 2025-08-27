/*
========================================================================
1.) Monitoring index usage
========================================================================
with this psql command the table info is displayed, including information
about the indexes of the table
\d silver.erp_loc_a101;
*/

--this query sums up the relevant information about the indexes in the
--bronze and silver schema
SELECT 
sui.indexrelname AS indexname,
sui.relname AS tablename,
sui.schemaname AS schemaname,
idx.indisunique AS unique_index,
idx.indisclustered AS clustured_index,
sui.idx_scan AS times_scanned,
sui.last_idx_scan AS last_scan,
sui.idx_tup_read AS rows_read,
sui.idx_tup_fetch AS rows_fetched
FROM pg_stat_user_indexes sui
LEFT JOIN pg_index idx
ON idx.indexrelid = sui.indexrelid
WHERE sui.schemaname IN ('bronze','silver');

--stats for indexes
SELECT * FROM pg_indexes;
SELECT * FROM pg_index;
SELECT * FROM pg_stat_user_indexes;
SELECT * FROM pg_stat_all_tables;
SELECT * FROM pg_class;
SELECT * FROM pg_am;
SELECT * FROM pg_attribute;
SELECT * FROM pg_constraint;
SELECT * FROM pg_depend;

--stats for tables
SELECT * FROM pg_tables;
SELECT * FROM pg_stat_all_tables;
SELECT * FROM pg_class;
SELECT * FROM pg_attribute;
SELECT * FROM pg_type;
SELECT * FROM pg_namespace;


/*
========================================================================
2.) Monitoring duplicate indexes
========================================================================
*/

SELECT 
cls.relname AS table_name,
tbl.schemaname AS schema_name,
tbl.tableowner AS table_owner,
idx.indrelid AS index_id,
att.attname AS column_name
FROM pg_class cls
LEFT JOIN pg_tables tbl
ON tbl.tablename = cls.relname
LEFT JOIN pg_stat_user_indexes sui
ON sui.relid = cls.oid
LEFT JOIN pg_index idx
ON idx.indexrelid = sui.indexrelid
LEFT JOIN pg_attribute att
ON att.attrelid = cls.oid
WHERE sui.schemaname IN ('bronze','silver')
ORDER BY cls.relname;

