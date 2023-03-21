CREATE TABLE [Sales].[BaseDimProduct] (
    [__window]                  INT            NOT NULL,
    [SurrogateKey]              INT            IDENTITY (1, 1) NOT NULL,
    [ProductID]                 INT            NOT NULL,
    [Name]                      NVARCHAR(50)   NOT NULL,
    [ProductNumber]             NVARCHAR(25)   NOT NULL,
    [Color]                     NVARCHAR(15)   NULL,
    [StandardCost]              MONEY          NOT NULL,
    [ListPrice]                 MONEY          NOT NULL,
    [Size]                      NVARCHAR(5)    NULL,
    [Weight]                    DECIMAL(8, 2)  NULL,
    [SellStartDate]             DATETIME       NOT NULL,
    [SellEndDate]               DATETIME       NULL,
    [DiscontinuedDate]          DATETIME       NULL,
    [ModelName]                 NVARCHAR(50)   NULL,
    [CategoryName]              NVARCHAR(50)   NOT NULL,
    [SubcategoryName]           NVARCHAR(50)   NOT NULL,
    [EnglishProductDescription] NVARCHAR(400)  NULL,
    [valid_from]                DATETIME       NOT NULL,
    [valid_to]                  DATETIME       NOT NULL,
    CONSTRAINT [Sales_BaseDimProduct_PK] PRIMARY KEY CLUSTERED ([SurrogateKey] ASC),
    CONSTRAINT [Sales_BaseDimProduct_Unique] UNIQUE NONCLUSTERED ([ProductID] ASC, [valid_from] ASC, [valid_to] ASC),
    CONSTRAINT [Sales_BaseDimProduct_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

