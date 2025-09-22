--creation of a table to test on
--in postgres you cannot partition an existing table, you have to create
--a new table and migrate the data afterwards - specify the column by which to partition
CREATE TABLE silver.partition_test (
order_num VARCHAR(50),
product_key VARCHAR(50),
cust_id INTEGER,
order_date DATE,
ship_date DATE,
due_date DATE,
sales INTEGER,
quantity INTEGER,
price INTEGER,
create_date TIMESTAMP WITHOUT TIME ZONE
) PARTITION BY RANGE (order_date);

--after the creation you have to create multiple "Tables" as a partition of
--the parent table, defining all the values

CREATE TABLE silver.partition_test_01 PARTITION OF silver.partition_test
FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

CREATE TABLE silver.partition_test_02 PARTITION OF silver.partition_test
FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

CREATE TABLE silver.partition_test_03 PARTITION OF silver.partition_test
FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');

CREATE TABLE silver.partition_test_04 PARTITION OF silver.partition_test
FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');

CREATE TABLE silver.partition_test_05 PARTITION OF silver.partition_test
FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');

CREATE TABLE silver.partition_test_00 PARTITION OF silver.partition_test DEFAULT;

--drop all the partitioning tables 

DROP TABLE silver.partition_test_01, silver.partition_test_02, silver.partition_test_03, 
silver.partition_test_04, silver.partition_test_05;

--after the creation of the different partitions you can insert
--the data 

INSERT INTO silver.partition_test (
order_num,
product_key,
cust_id,
order_date,
ship_date,
due_date,
sales,
quantity,
price,
create_date)
SELECT sls_ord_num,
sls_product_key,
sls_cust_id,
sls_order_date,
sls_ship_date,
sls_due_date,
sls_sales,
sls_quantity,
sls_price,
dwh_create_date 
FROM silver.crm_sales_details;

--SELECT Values from a single partition

SELECT * 
FROM silver.partition_test_00
UNION ALL 
SELECT * 
FROM silver.partition_test_01
UNION ALL 
SELECT * 
FROM silver.partition_test_02;

--maintenance script, that shows relevent metadata concerning the partitioned table

WITH partitions AS (
    SELECT
        parent.relname   AS parent_table,
        child.relname    AS partition_name,
        child.oid        AS partition_oid
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
    JOIN pg_namespace nmsp_child  ON nmsp_child.oid  = child.relnamespace
    WHERE nmsp_parent.nspname = 'silver'
      AND parent.relname = 'partition_test'
)
SELECT
    p.parent_table,
    p.partition_name,
    c.reltuples::bigint AS estimated_rows
FROM partitions p
JOIN pg_class c ON c.oid = p.partition_oid
ORDER BY p.partition_name;



--way more extensive monitoring script

CREATE SCHEMA IF NOT EXISTS admin;

CREATE OR REPLACE FUNCTION admin.monitor_partitions(
    p_parent        regclass,
    include_bloat   boolean DEFAULT false,
    exact_count     boolean DEFAULT false
)
RETURNS TABLE (
    parent_table         text,
    schema_name          text,
    partition_name       text,
    partition_regclass   regclass,
    bounds               text,
    is_default           boolean,
    estimated_rows       bigint,
    exact_rows           bigint,
    data_size_bytes      bigint,
    index_size_bytes     bigint,
    toast_size_bytes     bigint,
    total_size_bytes     bigint,
    data_size            text,
    index_size           text,
    toast_size           text,
    total_size           text,
    n_live_tup           bigint,
    n_dead_tup           bigint,
    last_vacuum          timestamp with time zone,
    last_autovacuum      timestamp with time zone,
    last_analyze         timestamp with time zone,
    last_autoanalyze     timestamp with time zone,
    seq_scan             bigint,
    idx_scan             bigint,
    n_tup_ins            bigint,
    n_tup_upd            bigint,
    n_tup_del            bigint,
    bloat_tuple_pct      numeric,
    dead_tuple_pct       numeric
)
LANGUAGE plpgsql AS
$$
DECLARE
    has_pgstattuple boolean;
    r RECORD;
    v_exact_count     bigint;
    v_bloat_tuple_pct numeric;
    v_dead_tuple_pct  numeric;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple')
    INTO has_pgstattuple;

    FOR r IN
        SELECT
            (p_parent::regclass)::text                           AS parent_table_text,
            ns_child.nspname                                      AS schema_name_name,
            c_child.relname                                       AS partition_name_name,
            c_child.oid                                           AS relid,
            c_child.oid::regclass                                 AS partition_regclass,
            pg_get_expr(c_child.relpartbound, c_child.oid)        AS bounds_expr,
            c_child.reltuples                                     AS estimated_rows_float,
            pg_table_size(c_child.oid)                            AS data_size_bytes,
            pg_indexes_size(c_child.oid)                          AS index_size_bytes,
            COALESCE(pg_total_relation_size(c_child.oid)
                     - pg_table_size(c_child.oid)
                     - pg_indexes_size(c_child.oid), 0)           AS toast_size_bytes,
            pg_total_relation_size(c_child.oid)                   AS total_size_bytes,
            st.n_live_tup, st.n_dead_tup,
            st.last_vacuum, st.last_autovacuum,
            st.last_analyze, st.last_autoanalyze,
            st.seq_scan, st.idx_scan,
            st.n_tup_ins, st.n_tup_upd, st.n_tup_del
        FROM pg_inherits i
        JOIN pg_class   c_parent   ON c_parent.oid   = i.inhparent
        JOIN pg_class   c_child    ON c_child.oid    = i.inhrelid
        JOIN pg_namespace ns_child ON ns_child.oid   = c_child.relnamespace
        LEFT JOIN pg_stat_all_tables st
               ON st.relid = c_child.oid
        WHERE i.inhparent = p_parent
        ORDER BY ns_child.nspname, c_child.relname
    LOOP
        -- exact count (optional; can be expensive)
        IF exact_count THEN
            EXECUTE format('SELECT count(*) FROM %s', r.partition_regclass::text)
              INTO v_exact_count;
        ELSE
            v_exact_count := NULL;
        END IF;

        -- bloat metrics (optional; requires pgstattuple)
        IF include_bloat AND has_pgstattuple THEN
            BEGIN
                -- Use the regclass overload directly (no dynamic SQL needed)
                SELECT approx_tuple_percent, dead_tuple_percent
                INTO v_bloat_tuple_pct, v_dead_tuple_pct
                FROM pgstattuple(r.partition_regclass);
            EXCEPTION WHEN others THEN
                v_bloat_tuple_pct := NULL;
                v_dead_tuple_pct  := NULL;
            END;
        ELSE
            v_bloat_tuple_pct := NULL;
            v_dead_tuple_pct  := NULL;
        END IF;

        RETURN QUERY
        SELECT
            r.parent_table_text::text,
            r.schema_name_name::text,            -- name -> text
            r.partition_name_name::text,         -- name -> text
            r.partition_regclass,
            r.bounds_expr,
            (r.bounds_expr = 'DEFAULT')::boolean AS is_default,
            r.estimated_rows_float::bigint,      -- float4 -> bigint
            v_exact_count,
            r.data_size_bytes,
            r.index_size_bytes,
            r.toast_size_bytes,
            r.total_size_bytes,
            pg_size_pretty(r.data_size_bytes),
            pg_size_pretty(r.index_size_bytes),
            pg_size_pretty(r.toast_size_bytes),
            pg_size_pretty(r.total_size_bytes),
            r.n_live_tup,
            r.n_dead_tup,
            r.last_vacuum,
            r.last_autovacuum,
            r.last_analyze,
            r.last_autoanalyze,
            r.seq_scan,
            r.idx_scan,
            r.n_tup_ins,
            r.n_tup_upd,
            r.n_tup_del,
            v_bloat_tuple_pct,
            v_dead_tuple_pct;
    END LOOP;
END;
$$;

SELECT *
FROM admin.monitor_partitions('silver.partition_test'::regclass, include_bloat => false, exact_count => true);



