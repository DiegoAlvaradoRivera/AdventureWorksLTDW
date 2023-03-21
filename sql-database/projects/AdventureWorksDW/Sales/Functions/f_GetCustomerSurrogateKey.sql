CREATE FUNCTION [Staging].[f_GetCustomerSurrogateKey](
	@customerID			INT,
	@timestamp          DATETIME
)
RETURNS TABLE
AS
RETURN 
SELECT SurrogateKey 
FROM Sales.BaseDimCustomer 
WHERE CustomerID = @customerID AND valid_from <= @timestamp AND @timestamp < valid_to;


GO

