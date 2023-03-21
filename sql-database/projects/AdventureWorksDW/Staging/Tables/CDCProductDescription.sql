CREATE TABLE [Staging].[CDCProductDescription] (

    [__window]                  INT            NOT NULL,
    [__operation]               CHAR           NOT NULL,
    [__tran_end_time]           DATETIME       NOT NULL,

    [ProductDescriptionID]      INT            NOT NULL,
    
    [Description]               NVARCHAR(400)  NULL,

    CONSTRAINT [Staging_CDCProductDescription_PK] PRIMARY KEY CLUSTERED (
        [ProductDescriptionID]  ASC,
        [__tran_end_time]       ASC
    ),
    CONSTRAINT [Staging_CDCProductDescription_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

