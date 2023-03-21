CREATE TABLE [Staging].[CDCCustomerAddress] (

    [__window]                INT            NOT NULL,
    [__operation]             CHAR           NOT NULL,
    [__tran_end_time]         DATETIME       NOT NULL,

    [CustomerID]              INT            NOT NULL,
    
    [AddressID]               INT            NOT NULL,
    [AddressType]             NVARCHAR (50)  NULL,

    CONSTRAINT [Staging_CustomerAddress_PK] PRIMARY KEY CLUSTERED ([CustomerID] ASC, [AddressID] ASC, [__tran_end_time] ASC),
    CONSTRAINT [Staging_CDCCustomerAddress_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

