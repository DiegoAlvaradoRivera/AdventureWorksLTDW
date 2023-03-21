CREATE TABLE [Staging].[CDCCustomer] (

    [__window]                INT            NOT NULL,
    [__operation]             CHAR           NOT NULL,
    [__tran_end_time]         DATETIME       NOT NULL,

    [CustomerID]              INT            NOT NULL,
    
    [NameStyle]               BIT            NULL,
    [Title]                   NVARCHAR (8)   NULL,
    [FirstName]               NVARCHAR (50)  NULL,
    [MiddleName]              NVARCHAR (50)  NULL,
    [LastName]                NVARCHAR (50)  NULL,
    [Suffix]                  NVARCHAR (10)  NULL,
    [CompanyName]             NVARCHAR (128) NULL,
    [SalesPerson]             NVARCHAR (256) NULL,
    [EmailAddress]            NVARCHAR (50)  NULL,
    [Phone]                   NVARCHAR (25)  NULL,

    CONSTRAINT [Staging_CDCCustomer_PK] PRIMARY KEY CLUSTERED ([CustomerID] ASC, [__tran_end_time] ASC),
    CONSTRAINT [Staging_CDCCustomer_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

