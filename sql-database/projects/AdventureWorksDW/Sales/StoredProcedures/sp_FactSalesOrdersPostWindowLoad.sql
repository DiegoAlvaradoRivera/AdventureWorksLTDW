CREATE PROCEDURE [Staging].[sp_FactSalesOrdersPostWindowLoad] (
	@window 	INT
)
AS
BEGIN

    UPDATE [Sales].[FactSalesOrders]
    SET CustomerSK = (SELECT * FROM Staging.f_GetCustomerSurrogateKey(CustomerID, OrderDate)),
        ProductSK  = (SELECT * FROM Staging.f_GetProductSurrogateKey(ProductID, OrderDate))
    WHERE __window = @window

END;

GO

