/* ---------------------------------------------------------------------------
   One-time SQL Server preparation for ch-sync.
   Run in the source database as a db_owner / sysadmin.
--------------------------------------------------------------------------- */

-- 1) Snapshot isolation: lets ch-sync read CT changes + current rows from one
--    consistent point in time without blocking writers.
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;

-- 2) Change Tracking at database level.
--    Retention must exceed: (longest initial load) + (longest outage you want
--    to survive without a full resync). 3-7 days is a common choice.
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
    ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);

-- 3) Enable CT on every user table that has a primary key.
DECLARE @sql nvarchar(max) = N'';
SELECT @sql += N'ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
             + N' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);' + CHAR(10)
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
  AND OBJECTPROPERTY(t.object_id, 'TableHasPrimaryKey') = 1
  AND NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables AS c WHERE c.object_id = t.object_id);
PRINT @sql;
EXEC sys.sp_executesql @sql;

-- 4) Tables WITHOUT a primary key cannot use Change Tracking. Review these:
SELECT s.name AS [schema], t.name AS [table_without_pk]
FROM sys.tables AS t JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0 AND OBJECTPROPERTY(t.object_id, 'TableHasPrimaryKey') = 0;

/* 5) Least-privilege login for the sync service (adjust names / password):

CREATE LOGIN ch_sync WITH PASSWORD = 'change-me';
CREATE USER ch_sync FOR LOGIN ch_sync;
ALTER ROLE db_datareader ADD MEMBER ch_sync;
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO ch_sync;   -- repeat per schema
GRANT VIEW DEFINITION ON SCHEMA::dbo TO ch_sync;        -- schema introspection
*/
