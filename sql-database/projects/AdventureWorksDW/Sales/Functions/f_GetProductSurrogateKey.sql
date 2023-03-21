CREATE FUNCTION [Staging].[f_GetProductSurrogateKey](
	@productID			INT,
	@timestamp          DATETIME
)
RETURNS TABLE
AS
RETURN 
SELECT SurrogateKey 
FROM Sales.BaseDimProduct 
WHERE ProductID = @productID AND valid_from <= @timestamp AND @timestamp < valid_to;


GO

