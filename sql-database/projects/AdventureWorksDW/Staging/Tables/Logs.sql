CREATE TABLE [Staging].[Logs] (

    [Window]                  INT            NOT NULL,
    [Table]                   NVARCHAR(100)  NOT NULL,

    CONSTRAINT [Staging_Logs_PK] PRIMARY KEY CLUSTERED ([Window], [Table] ASC),
    CONSTRAINT [Sales_Logs_WindowFK] FOREIGN KEY ([Window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

