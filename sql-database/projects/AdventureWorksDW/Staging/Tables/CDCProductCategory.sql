CREATE TABLE [Staging].[CDCProductCategory] (

    [__window]                  INT            NOT NULL,
    [__operation]               CHAR           NOT NULL,
    [__tran_end_time]           DATETIME       NOT NULL,

    [ProductCategoryID]         INT            NOT NULL,
    
    [ParentProductCategoryID]   INT            NULL,
    [Name]                      NVARCHAR (50)  NULL,

    CONSTRAINT [Staging_CDCProductCategory_PK] PRIMARY KEY CLUSTERED ([ProductCategoryID] ASC, [__tran_end_time] ASC),
    CONSTRAINT [Staging_CDCProductCategory_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

