/* Generate some inserts/updates/deletes to watch them flow into ClickHouse. */
USE ShopDemo;
UPDATE TOP (500) dbo.Orders SET Status = N'refunded', Amount = Amount * -1 WHERE Status = N'paid';
DELETE TOP (100) FROM sales.OrderLines WHERE LineNo = 3;
DELETE TOP (100) FROM dbo.Orders WHERE Status = N'cancelled' AND OrderId > 50000;
INSERT dbo.Orders (CustomerId, Status, Amount) VALUES (42, N'new', 10.00), (43, N'new', 20.00);
ALTER TABLE dbo.Orders ADD Channel varchar(20) NULL;   -- schema drift: picked up automatically
GO
UPDATE TOP (50) dbo.Orders SET Channel = 'mobile' WHERE Status = N'new';
