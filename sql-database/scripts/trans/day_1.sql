
set nocount on;
go 

/*
TRANSACTION NUMBER 0

Modify the hash and salt value of customer 29531
Given that this columns are not tracked, this transaction
	should not generate a record in the CDC log
*/
begin transaction;

	update SalesLT.Customer 
	set PasswordHash = 'zh3goJUbYsPv92k4bVZuHtlLHwuvpQtu6uNcjkKSdF8=',
		PasswordSalt = 'rprd5Tw='
	where CustomerID = 29531;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;

/*
TRANSACTION NUMBER 1

Register a new customer called Eugene Porter.
*/
begin transaction;

	declare @customer_id as int;
	declare @address_id as int;

	insert into SalesLT.Customer
	(FirstName, LastName, CompanyName, SalesPerson, EmailAddress, Phone, PasswordHash, PasswordSalt)
	values
	('Eugene', 'Porter', 'Regressive Sports', 'adventure-works\jillian0', 'eugene0@adventure-works.com', '279-555-0130', 'abcde', 'abcde')

	select @customer_id = @@IDENTITY;

	insert into SalesLT.Address
	(AddressLine1, City, StateProvince, CountryRegion, PostalCode)
	values
	('1434 Marshall Rd', 'Alpine', 'California', 'United States', '91901');

	select @address_id = @@IDENTITY;

	insert into SalesLT.CustomerAddress
	(CustomerID, AddressID, AddressType)
	values
	(@customer_id, @address_id, 'Main Office')

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;

/*
TRANSACTION NUMBER 2

Modify the sales person of the customer 29568.
*/
begin transaction;

	update SalesLT.Customer 
	set SalesPerson = 'adventure-works\linda3'
	where CustomerID = 29568;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;
go 

/*
TRANSACTION NUMBER 3

Change the main office address of customer 29485.
A new address is inserted in the Address table and then CustomerAddress record
associated with the Main Office direction is udpated.
*/
begin transaction;

	declare @address_id as int;

	insert into SalesLT.Address
	(AddressLine1, City, StateProvince, CountryRegion, PostalCode)
	values
	('7343 Sepulveda Blvd', 'Van Nuys', 'California', 'United States', '91405');

	select @address_id = @@IDENTITY;

	update SalesLT.CustomerAddress 
	set AddressId = @address_id
	where CustomerID = 29485 and AddressType = 'Main Office'

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;

/*
TRANSACTION NUMBER 4

Create an order for the customer 29781. 
The order is initially in state 'in progress'.
*/
begin transaction;

	declare @createdOrder INT;
	declare @customer_id INT = 29781;
	declare @orderDate datetime = getdate();
	-- declare @inOneWeek datetime = DATEADD(day, 7, getdate());

	-- create a sales order header with a provisional due date
	insert into SalesLT.SalesOrderHeader 
	(OrderDate, DueDate, Status, AccountNumber, CustomerID, ShipToAddressID, BillToAddressId, ShipMethod, SubTotal, TaxAmt, Freight) 
	values 
	(@orderDate, @orderDate, 1, '10-4020-000277', @customer_id, 1035, 1035, 'CARGO TRANSPORT 5', 96.1088, 8.5233, 2.6635);

	-- select @createdOrder = @@IDENTITY;
	select @createdOrder = SalesOrderID from SalesLT.SalesOrderHeader  where CustomerID = @customer_id and Status = 1;

	insert into SalesLT.SalesOrderDetail
	(SalesOrderID, OrderQty, ProductID, UnitPrice, UnitPriceDiscount)
	values 
	(@createdOrder,	1, 870, 2.994, 0.00),
	(@createdOrder,	4, 874, 5.394, 0.00),
	(@createdOrder,	14, 875, 5.214, 0.02);

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;
go 

/*
TRANSACTION NUMBER 5

Create an order for the customer 29847. 
The order is initially in state 'in progress'
*/
begin transaction;

	declare @createdOrder INT;
	declare @customer_id INT = 29847;
	declare @orderDate datetime = getdate();
	-- declare @inOneWeek datetime = getdate();

	-- create a sales order header with a provisional due date
	insert into SalesLT.SalesOrderHeader 
	(OrderDate, DueDate, Status, AccountNumber, CustomerID, ShipToAddressID, BillToAddressId, ShipMethod, SubTotal, TaxAmt, Freight) 
	values 
	(@orderDate, @orderDate, 1, '10-4020-000609', @customer_id, 1092, 1092, 'CARGO TRANSPORT 5', 713.796, 70.4279, 22.0087);

	-- select @createdOrder = @@IDENTITY;
	select @createdOrder = SalesOrderID from SalesLT.SalesOrderHeader  where CustomerID = @customer_id and Status = 1;

	insert into SalesLT.SalesOrderDetail
	(SalesOrderID, OrderQty, ProductID, UnitPrice, UnitPriceDiscount)
	values 
	(@createdOrder,	1, 836, 356.898, 0.00),
	(@createdOrder,	1, 822, 356.898, 0.00);

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;
go 

/*
TRANSACTION NUMBER 6

Increase the list price of products for product model 36 (Touring-3000) by 5%
*/
begin transaction;

	update SalesLT.Product
	set ListPrice = ListPrice * 1.05
	where ProductModelID = 36;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;

/*
TRANSACTION NUMBER 7

Divide the 'Bottles and Cages' product category into two separte subcategories: 'Bottles' and 'Bottle Cages'
*/
begin transaction;

	insert into SalesLT.ProductCategory
	(Name, ParentProductCategoryID) 
	values 
	('Bottles', 4),
	('Bottle Cages', 4);

	declare @BottlesCategoryID INT;
	declare @BottleCagesCategoryID INT;

	select @BottlesCategoryID = ProductCategoryID from SalesLT.ProductCategory where Name = 'Bottles';
	select @BottleCagesCategoryID = ProductCategoryID from SalesLT.ProductCategory where Name = 'Bottle Cages';

	update SalesLT.Product
	set ProductCategoryID = @BottlesCategoryID
	where ProductID in (870);

	update SalesLT.Product
	set ProductCategoryID = @BottleCagesCategoryID
	where ProductID in (871, 872);

	delete from SalesLT.ProductCategory
	where ProductCategoryID = 32;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit;

/*
TRANSACTION NUMBER 8

Stop selling product 907.
*/
begin transaction;

	update SalesLT.Product
	set SellEndDate = GETDATE()
	where ProductId = 907;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit; 

/*
TRANSACTION NUMBER 9

Modify the name of product model 1 from 'Classic Vest' to 'Standard Vest'.
*/
begin transaction;

	update SalesLT.ProductModel
	set name = 'Standard Vest'
	where ProductModelId = 1;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit; 

/*
TRANSACTION NUMBER 10

Modify the of product 771.
*/	
begin transaction;

	delete SalesLT.Product
	where ProductID = 771;

	-- wait for 1 second
	WAITFOR DELAY '00:00:01'

commit; 


print 'COMPLETED TRANSACTIONS FOR DAY 1';
go 
