CREATE TABLE [Staging].[CDCProduct] (

    [__window]                  INT            NOT NULL,
    [__operation]               CHAR           NOT NULL,
    [__tran_end_time]           DATETIME       NOT NULL,

    [ProductID]                 INT            NOT NULL,
    
    [Name]                      NVARCHAR (50)  NULL,
    [ProductNumber]             NVARCHAR (25)  NULL,
    [Color]                     NVARCHAR (15)  NULL,
    [StandardCost]              MONEY          NULL,
    [ListPrice]                 MONEY          NULL,
    [Size]                      NVARCHAR (5)   NULL,
    [Weight]                    DECIMAL (8, 2) NULL,
    [ProductCategoryID]         INT            NULL,
    [ProductModelID]            INT            NULL,
    [SellStartDate]             DATETIME       NULL,
    [SellEndDate]               DATETIME       NULL,
    [DiscontinuedDate]          DATETIME       NULL,

    CONSTRAINT [Staging_CDCProduct_PK] PRIMARY KEY CLUSTERED ([ProductID] ASC, [__tran_end_time] ASC),
    CONSTRAINT [Staging_CDCProduct_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

