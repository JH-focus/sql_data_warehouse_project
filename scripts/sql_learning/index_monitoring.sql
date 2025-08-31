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
SELECT * FROM pg_class WHERE relhasindex = TRUE;
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

--this query is based upon the pg_indexes table and displays all the relevant 
--information of an index. It is not joinable though, which makes it only half
--a solution. It excludes postgreSQL default tables.
SELECT 
idxs.schemaname AS schema_name,
idxs.tablename AS table_name,
idxs.indexname AS index_name,
idxs.indexdef AS index_definition,
split_part(split_part(idxs.indexdef, 'btree (', 2),')', 1) AS index_column,
COUNT(*) OVER (PARTITION BY idxs.tablename, split_part(split_part(idxs.indexdef, 'btree (', 2),')', 1)) AS column_count
FROM pg_indexes idxs
WHERE idxs.tablename NOT LIKE 'pg_%';


/*
========================================================================
3.) Monitoring table statistics
========================================================================
*/

--data about table statistics
SELECT * FROM pg_class; --reltuples, relpages, relallvisible
SELECT * FROM pg_statistic;
SELECT * FROM pg_statistic_ext;
SELECT * FROM pg_statistic_ext_data;
SELECT * FROM pg_stats; --view on top of pg_statistic
SELECT * FROM pg_stats_ext; --view on pg_statistic_ext and pg_statistic_ext_data
SELECT * FROM pg_stat_all_tables; --runtime statistics per table
SELECT * FROM pg_stat_user_tables; --runtime statistics only for user tables
SELECT * FROM pg_stat_sys_tables; --runtime statistics only for system tables
SELECT * FROM pg_stat_all_indexes; --index usage stats for all indexes
SELECT * FROM pg_stat_user_indexes; --index usage stats for user indexes


--table statistics with a focus on index usage
WITH idx_summ AS (
  SELECT relid,
         SUM(idx_scan)      AS idx_scan_total,
         SUM(idx_tup_fetch) AS idx_tup_fetch_total
  FROM pg_stat_user_indexes
  GROUP BY relid
),
top_idx AS (
  SELECT 
    ui.relid,
    ic.relname AS index_name,
    ui.idx_scan,
    ui.idx_tup_fetch,
    ROW_NUMBER() OVER (
      PARTITION BY ui.relid ORDER BY ui.idx_scan DESC NULLS LAST
    ) AS number_of_rows
  FROM pg_stat_user_indexes ui
  JOIN pg_class ic ON ic.oid = ui.indexrelid
),
-- pull table-level reloptions and pivot the autovacuum settings we care about
relopts AS (
  SELECT
    c.oid AS relid,
    MAX(CASE WHEN split_part(opt,'=',1) = 'autovacuum_analyze_threshold'
             THEN split_part(opt,'=',2)::numeric END) AS o_analyze_threshold,
    MAX(CASE WHEN split_part(opt,'=',1) = 'autovacuum_analyze_scale_factor'
             THEN split_part(opt,'=',2)::numeric END) AS o_analyze_scale,
    MAX(CASE WHEN split_part(opt,'=',1) = 'autovacuum_vacuum_threshold'
             THEN split_part(opt,'=',2)::numeric END) AS o_vacuum_threshold,
    MAX(CASE WHEN split_part(opt,'=',1) = 'autovacuum_vacuum_scale_factor'
             THEN split_part(opt,'=',2)::numeric END) AS o_vacuum_scale
  FROM pg_class c
  LEFT JOIN LATERAL unnest(c.reloptions) AS u(opt) ON TRUE
  GROUP BY c.oid
)
SELECT
  n.nspname AS schema_name,
  c.relname AS table_name,
  pg_size_pretty(pg_relation_size(c.oid)) AS table_size,

  -- Index usage summary (table-centric)
  COALESCE(isum.idx_scan_total, 0)      AS idx_scan_total,
  COALESCE(isum.idx_tup_fetch_total, 0) AS idx_tup_fetch_total,
  t.seq_scan,
  t.seq_tup_read,
  ROUND(
    100.0 * COALESCE(isum.idx_scan_total, 0)
    / NULLIF(COALESCE(isum.idx_scan_total, 0) + t.seq_scan, 0), 1
  ) AS pct_index_vs_seq_scans,

  -- I/O / cache
  io.heap_blks_read,
  io.heap_blks_hit,
  ROUND(
    100.0 * io.heap_blks_hit
    / NULLIF(io.heap_blks_hit + io.heap_blks_read, 0), 1
  ) AS heap_cache_hit_pct,

  -- Table health
  t.n_live_tup,
  t.n_dead_tup,
  TO_CHAR(t.last_vacuum,      'YYYY-MM-DD HH24:MI:SS') AS last_vacuum,
  TO_CHAR(t.last_autovacuum,  'YYYY-MM-DD HH24:MI:SS') AS last_autovacuum,
  TO_CHAR(t.last_analyze,     'YYYY-MM-DD HH24:MI:SS') AS last_analyze,
  TO_CHAR(t.last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') AS last_autoanalyze,

  -- Change activity counters
  t.n_tup_ins,
  t.n_tup_upd,
  t.n_tup_del,
  t.n_tup_hot_upd,
  t.n_mod_since_analyze AS changes_since_last_autoanalyze,

  -- Effective thresholds (per-table override -> global default)
  -- analyze_threshold = analyze_threshold_setting + analyze_scale_setting * n_live_tup
  (COALESCE(ro.o_analyze_threshold, current_setting('autovacuum_analyze_threshold')::numeric)
 + COALESCE(ro.o_analyze_scale,current_setting('autovacuum_analyze_scale_factor')::numeric) * t.n_live_tup) AS effective_analyze_threshold,

  -- vacuum_threshold = vacuum_threshold_setting + vacuum_scale_setting * n_live_tup
  (COALESCE(ro.o_vacuum_threshold, current_setting('autovacuum_vacuum_threshold')::numeric)
 + COALESCE(ro.o_vacuum_scale, current_setting('autovacuum_vacuum_scale_factor')::numeric) * t.n_live_tup) AS effective_vacuum_threshold,

  -- Top 3 most-used indexes (by scans)
  STRING_AGG(FORMAT('%s (scans=%s)', top.index_name, top.idx_scan),', ' ORDER BY top.number_of_rows) 
  AS top_3_indexes_by_scans

FROM pg_stat_user_tables t
JOIN pg_statio_user_tables io USING (relid)
JOIN pg_class c           ON c.oid = t.relid
JOIN pg_namespace n       ON n.oid = c.relnamespace
LEFT JOIN idx_summ isum   ON isum.relid = t.relid
LEFT JOIN top_idx  top    ON top.relid = t.relid AND top.number_of_rows <= 3
LEFT JOIN relopts ro      ON ro.relid = c.oid
GROUP BY
  n.nspname, c.relname, c.oid, t.relid,
  t.seq_scan, t.seq_tup_read,
  io.heap_blks_read, io.heap_blks_hit,
  t.n_live_tup, t.n_dead_tup,
  t.last_autovacuum, t.last_vacuum, t.last_autoanalyze, t.last_analyze,
  t.n_tup_ins, t.n_tup_upd, t.n_tup_del, t.n_tup_hot_upd, t.n_mod_since_analyze,
  isum.idx_scan_total, isum.idx_tup_fetch_total,
  ro.o_analyze_threshold, ro.o_analyze_scale, ro.o_vacuum_threshold, ro.o_vacuum_scale
ORDER BY
  pct_index_vs_seq_scans DESC NULLS LAST,
  idx_scan_total DESC NULLS LAST,
  schema_name, table_name;


  

--you can manually vacuum or analyze a table, the VERBOSE command adds logs in order to understand things more easily
VACUUM VERBOSE bronze.crm_prod_info; --only cleaning dead tuples, not updating the table statistics into pg_statistic
ANALYZE VERBOSE bronze.crm_prod_info; --only updating the table statistics into pg_statistic, not cleaning dead tuples
VACUUM ANALYZE VERBOSE bronze.crm_prod_info; --both cleaning and updating


/*
========================================================================
4.) Monitoring data fragmentation
========================================================================
*/

WITH t AS (
  SELECT relid, schemaname, relname,
         n_live_tup, n_dead_tup,
         ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup,0),1) AS dead_pct,
         pg_total_relation_size(relid) AS bytes
  FROM pg_stat_user_tables
),
i AS (
  SELECT ui.relid,
         SUM(ui.idx_scan) AS idx_scans
  FROM pg_stat_user_indexes ui
  GROUP BY ui.relid
)
SELECT
  t.schemaname, t.relname AS table_name,
  pg_size_pretty(t.bytes) AS total_size,
  t.dead_pct,
  COALESCE(i.idx_scans,0) AS idx_scans,
  CASE
  	WHEN t.bytes >= 100*1024*1024 AND t.dead_pct >= 30 THEN 'Reindex'
    WHEN t.bytes >= 100*1024*1024 AND t.dead_pct >= 20 THEN 'RECLAIM SPACE (VACUUM FULL/pg_repack)'
    WHEN t.dead_pct >= 10 THEN 'VACUUM / tune autovacuum'
    ELSE 'OK'
  END AS recommendation
FROM t
LEFT JOIN i USING (relid)
ORDER BY recommendation DESC, t.bytes DESC;


--reindex (drop index and rebuild it from scratch)
REINDEX INDEX silver.idx_erp_loc_a101_country; --single index
REINDEX TABLE silver.erp_loc_a101; --all indexes of a table
REINDEX SCHEMA silver; --all indexes of a schema
REINDEX DATABASE datawarehouse; --all indexes of a datawarehouse

--cluster
CLUSTER bronze.crm_prod_info USING idx_crm_prod_info_prod_id; --physically reorganize the data, so that it is stored in specific index order



