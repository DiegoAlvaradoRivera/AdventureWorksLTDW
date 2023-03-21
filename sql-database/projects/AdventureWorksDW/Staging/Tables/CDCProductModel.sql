CREATE TABLE [Staging].[CDCProductModel] (

    [__window]                  INT            NOT NULL,
    [__operation]               CHAR           NOT NULL,
    [__tran_end_time]           DATETIME       NOT NULL,

    [ProductModelID]            INT            NOT NULL,
    
    [Name]                      NVARCHAR (50)  NULL,


    CONSTRAINT [Staging_CDCProductModel_PK] PRIMARY KEY CLUSTERED ([ProductModelID] ASC, [__tran_end_time] ASC),
    CONSTRAINT [Staging_CDCProductModel_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

