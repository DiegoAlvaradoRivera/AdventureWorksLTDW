CREATE TABLE [Sales].[BaseDimCustomer] (
    [__window]                INT            NOT NULL,
    [SurrogateKey]            INT            IDENTITY (1, 1) NOT NULL,
    [CustomerID]              INT            NOT NULL,
    [NameStyle]               BIT            NOT NULL,
    [Title]                   NVARCHAR(8)    NULL,
    [FirstName]               NVARCHAR(50)   NOT NULL,
    [MiddleName]              NVARCHAR(50)   NULL,
    [LastName]                NVARCHAR(50)   NOT NULL,
    [Suffix]                  NVARCHAR(10)   NULL,
    [CompanyName]             NVARCHAR(128)  NULL,
    [SalesPerson]             NVARCHAR(256)  NULL,
    [EmailAddress]            NVARCHAR(50)   NULL,
    [Phone]                   NVARCHAR(25)   NULL,
    [MainOfficeAddressLine1]  NVARCHAR(60)   NOT NULL,
    [MainOfficeAddressLine2]  NVARCHAR(60)   NULL,
    [MainOfficeCity]          NVARCHAR(30)   NOT NULL,
    [MainOfficeStateProvince] NVARCHAR(50)   NOT NULL,
    [MainOfficeCountryRegion] NVARCHAR(50)   NOT NULL,
    [MainOfficePostalCode]    NVARCHAR(15)   NOT NULL,
    [valid_from]              DATETIME       NOT NULL,
    [valid_to]                DATETIME       NOT NULL,
    CONSTRAINT [Sales_BaseDimCustomer_PK] PRIMARY KEY CLUSTERED ([SurrogateKey] ASC),
    CONSTRAINT [Sales_BaseDimCustomer_Unique] UNIQUE NONCLUSTERED ([CustomerID] ASC, [valid_from] ASC, [valid_to] ASC),
    CONSTRAINT [Sales_BaseDimCustomer_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

