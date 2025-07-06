/*
===================================================================
Create Databse and Schemas
===================================================================
Script Purpose:
This Script is supposed to create the Database 'datawarehouse'. Prior to
database creation it checks, if the database already exists. It drops the
existing Database and creates a new one an sets up three schemas within
the database: 'bronze', 'silver' and 'gold'.

Warning: 
  Running this Script will drop the entire 'datawarehouse database if it
  exists. All data in the database will be permanently deleted.
*/
DROP DATABASE IF EXISTS datawarehouse WITH (FORCE);

-- the following statement will only be executed in case the database 'datawarehouse' doesn't already exist
-- it shouldn't exist because of the initial DROP DATABASE statement
SELECT 'CREATE DATABASE datawarehouse'
WHERE NOT EXISTS (
   SELECT FROM pg_database WHERE datname = 'datawarehouse'
)\gexec


\c datawarehouse

-- Create Schemas for later use
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
