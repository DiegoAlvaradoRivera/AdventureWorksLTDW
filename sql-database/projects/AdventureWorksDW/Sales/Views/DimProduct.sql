CREATE VIEW [Sales].[DimProduct]
AS 
SELECT 

    [SurrogateKey], 
    [ProductID], 

	-- SCD 0 columns
    FIRST_VALUE(SellStartDate)   OVER (PARTITION BY ProductID ORDER BY valid_from) as SellStartDate,
	
	-- SCD 1 columns
	LAST_VALUE(Name)             OVER (PARTITION BY ProductID ORDER BY valid_from) as Name,
	LAST_VALUE(ProductNumber)    OVER (PARTITION BY ProductID ORDER BY valid_from) as ProductNumber,
	LAST_VALUE(Color)            OVER (PARTITION BY ProductID ORDER BY valid_from) as Color,
    LAST_VALUE(Size)             OVER (PARTITION BY ProductID ORDER BY valid_from) as Size,
    LAST_VALUE(Weight)           OVER (PARTITION BY ProductID ORDER BY valid_from) as Weight,
    LAST_VALUE(SellEndDate)      OVER (PARTITION BY ProductID ORDER BY valid_from) as SellEndDate,
    LAST_VALUE(DiscontinuedDate) OVER (PARTITION BY ProductID ORDER BY valid_from) as DiscontinuedDate,
    LAST_VALUE(ModelName)        OVER (PARTITION BY ProductID ORDER BY valid_from) as ModelName,
    LAST_VALUE(CategoryName)     OVER (PARTITION BY ProductID ORDER BY valid_from) as CategoryName,
    LAST_VALUE(SubcategoryName)  OVER (PARTITION BY ProductID ORDER BY valid_from) as SubcategoryName,
    LAST_VALUE(EnglishProductDescription) OVER (PARTITION BY ProductID ORDER BY valid_from) as EnglishProductDescription,

	-- SCD 2 columns
    [StandardCost], 
    [ListPrice]

    [valid_from], 
    [valid_to]

FROM [Sales].[BaseDimProduct];


GO

