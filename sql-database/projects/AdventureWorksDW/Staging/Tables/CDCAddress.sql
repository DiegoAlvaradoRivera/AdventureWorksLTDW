CREATE TABLE [Staging].[CDCAddress] (

    [__window]                INT            NOT NULL,
    [__operation]             CHAR           NOT NULL,
    [__tran_end_time]         DATETIME       NOT NULL,

    [AddressID]               INT            NOT NULL,
    
    [AddressLine1]            NVARCHAR (60)  NULL,
    [AddressLine2]            NVARCHAR (60)  NULL,
    [City]                    NVARCHAR (30)  NULL,
    [StateProvince]           NVARCHAR (50)  NULL,
    [CountryRegion]           NVARCHAR (50)  NULL,
    [PostalCode]              NVARCHAR (15)  NULL,

    CONSTRAINT [Staging_CDCAddress_PK] PRIMARY KEY CLUSTERED ([AddressID] ASC, [__tran_end_time] ASC),
    CONSTRAINT [Staging_CDCAddress_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

