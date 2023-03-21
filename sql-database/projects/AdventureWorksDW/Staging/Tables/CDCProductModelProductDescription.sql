CREATE TABLE [Staging].[CDCProductModelProductDescription] (

    [__window]                  INT            NOT NULL,
    [__operation]               CHAR           NOT NULL,
    [__tran_end_time]           DATETIME       NOT NULL,

    [ProductModelID]            INT            NOT NULL,
    [ProductDescriptionID]      INT            NOT NULL,
    [Culture]                   NCHAR(50)      NOT NULL,

    CONSTRAINT [Staging_CDCProductModelProductDescription_PK] PRIMARY KEY CLUSTERED (
        [ProductModelID]        ASC, 
        [ProductDescriptionID]  ASC,
        [Culture]               ASC,
        [__tran_end_time]       ASC
    ),
    CONSTRAINT [Staging_CDCProductModelProductDescription_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

