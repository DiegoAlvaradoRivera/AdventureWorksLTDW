CREATE TABLE [Staging].[Windows] (

    [Window]                  INT            NOT NULL,
    [FromTm]                  DATETIME       NOT NULL,
    [ToTm]                    DATETIME       NOT NULL,

    CONSTRAINT [Staging_Windows_PK] PRIMARY KEY CLUSTERED ([Window] ASC),
    CONSTRAINT [Staging_Windows_CheckFromLETTo] CHECK (FromTm <= ToTm)
);


GO

