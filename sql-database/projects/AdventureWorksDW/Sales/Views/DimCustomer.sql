CREATE VIEW [Sales].[DimCustomer]
AS 
SELECT 

    [SurrogateKey], 
    [CustomerID], 

	-- SCD 0 columns
	FIRST_VALUE(CompanyName) OVER (PARTITION BY CustomerID ORDER BY valid_from) as OriginalCompanyName,
	
	-- SCD 1 columns
	LAST_VALUE(NameStyle)    OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentNameStyle,
	LAST_VALUE(Title)        OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentTitle,
	LAST_VALUE(FirstName)    OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentFirstName,
	LAST_VALUE(MiddleName)   OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentMiddleName,
	LAST_VALUE(LastName)     OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentLastName,
	LAST_VALUE(Suffix)       OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentSuffix,
	LAST_VALUE(CompanyName)  OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentCompanyName,
	LAST_VALUE(EmailAddress) OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentEmailAddress,
	LAST_VALUE(Phone)        OVER (PARTITION BY CustomerID ORDER BY valid_from) as CurrentPhone,

	-- SCD 2 columns
	[SalesPerson], 
	[MainOfficeAddressLine1], 
    [MainOfficeAddressLine2], 
    [MainOfficeCity], 
    [MainOfficeStateProvince], 
    [MainOfficeCountryRegion], 
    [MainOfficePostalCode], 

	-- -- SCD 3 columns
	-- (
	-- 	SELECT I.MainOfficeStateProvince  
	-- 	FROM Sales.BaseDimCustomer AS I 
	-- 	WHERE I.CustomerID = O.CustomerID AND '2009-01-01' BETWEEN I.valid_from AND I.valid_to
	-- ) AS MainOfficeStateProvince2009,

    [valid_from], 
    [valid_to]

FROM [Sales].[BaseDimCustomer] AS O;


GO

