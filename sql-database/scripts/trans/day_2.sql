
set nocount on;
go 

/*
TRANSACTION NUMBER 1

Ship and due the order of customer 29781.
Update the ship date, due date, and status.
*/
begin transaction;

	declare @order_id int;

	select @order_id = SalesOrderID 
	from SalesLT.SalesOrderHeader 
	where CustomerID = 29781 and Status = 1;

	update SalesLT.SalesOrderHeader
	set 
	ShipDate = getdate(), 
	DueDate = getdate(), 
	Status = 5
	where SalesOrderID = @order_id;
	
	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit; 
go 

/*
TRANSACTION NUMBER 2

Cancel the order of the customer 29847.
*/
begin transaction;

	declare @order_id int;

	select @order_id = SalesOrderID 
	from SalesLT.SalesOrderHeader 
	where CustomerID = 29847 and Status = 1;

	update SalesLT.SalesOrderHeader
	set 
	Status = 6
	where SalesOrderID = @order_id;

	delete from SalesLT.SalesOrderDetail 
	where SalesOrderID = @order_id;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;
go 

/*
TRANSACTION NUMBER 0
set a product categoryID column to null
*/
begin transaction;

	update SalesLT.Product
	set ProductCategoryID = null
	where ProductID = 680;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;

print 'COMPLETED TRANSACTIONS FOR DAY 2';
go 
