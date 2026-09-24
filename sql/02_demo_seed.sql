/* Demo database for docker-compose. Idempotent. */
IF DB_ID('ShopDemo') IS NULL CREATE DATABASE ShopDemo;
GO
USE ShopDemo;
GO
IF SCHEMA_ID('sales') IS NULL EXEC('CREATE SCHEMA sales');
GO
IF OBJECT_ID('dbo.Orders') IS NULL
BEGIN
    CREATE TABLE dbo.Orders (
        OrderId      bigint IDENTITY(1,1) PRIMARY KEY CLUSTERED,
        CustomerId   int NOT NULL,
        Status       nvarchar(20) NOT NULL,
        Amount       decimal(18,2) NULL,
        Currency     char(3) NOT NULL DEFAULT 'INR',
        IsPaid       bit NOT NULL DEFAULT 0,
        CreatedAt    datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        UpdatedAtTz  datetimeoffset(3) NULL,
        ExternalRef  uniqueidentifier NULL DEFAULT NEWID(),
        Notes        nvarchar(max) NULL
    );
    ;WITH n AS (SELECT TOP (200000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects a CROSS JOIN sys.all_objects b)
    INSERT dbo.Orders (CustomerId, Status, Amount, IsPaid, CreatedAt, UpdatedAtTz)
    SELECT i % 5000, CASE i % 4 WHEN 0 THEN N'new' WHEN 1 THEN N'paid' WHEN 2 THEN N'shipped' ELSE N'cancelled' END,
           CAST((i % 100000) / 7.0 AS decimal(18,2)), CASE WHEN i % 4 IN (1,2) THEN 1 ELSE 0 END,
           DATEADD(MINUTE, -i, SYSUTCDATETIME()), TODATETIMEOFFSET(DATEADD(MINUTE, -i, SYSUTCDATETIME()), '+05:30')
    FROM n;
END
GO
IF OBJECT_ID('sales.OrderLines') IS NULL
BEGIN
    CREATE TABLE sales.OrderLines (
        OrderId  bigint NOT NULL,
        LineNo   smallint NOT NULL,
        Sku      varchar(40) NOT NULL,
        Qty      int NOT NULL,
        Price    money NOT NULL,
        CONSTRAINT PK_OrderLines PRIMARY KEY CLUSTERED (OrderId, LineNo)
    );
    INSERT sales.OrderLines (OrderId, LineNo, Sku, Qty, Price)
    SELECT o.OrderId, l.n, CONCAT('SKU-', o.OrderId % 997, '-', l.n), 1 + o.OrderId % 5, 99.50
    FROM dbo.Orders AS o CROSS JOIN (VALUES (1),(2),(3)) AS l(n)
    WHERE o.OrderId <= 50000;
END
GO
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
    ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('dbo.Orders'))
    ALTER TABLE dbo.Orders ENABLE CHANGE_TRACKING;
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('sales.OrderLines'))
    ALTER TABLE sales.OrderLines ENABLE CHANGE_TRACKING;
GO
PRINT 'ShopDemo ready';
