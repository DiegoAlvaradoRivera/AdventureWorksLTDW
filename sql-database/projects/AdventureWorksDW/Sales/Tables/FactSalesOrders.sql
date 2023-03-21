CREATE TABLE [Sales].[FactSalesOrders] (
    [__window]               INT            NOT NULL,
    [SalesOrderDetailID]     INT            NOT NULL,
    [CustomerSK]             INT            NULL,
    [ProductSK]              INT            NULL,
    [SalesOrderID]           INT            NOT NULL,
    [CustomerID]             INT            NOT NULL,
    [ProductID]              INT            NOT NULL,
    [OrderDate]              DATETIME       NOT NULL,
    [DueDate]                DATETIME       NOT NULL,
    [ShipDate]               DATETIME       NULL,
    [Status]                 TINYINT        NOT NULL,
    [OnlineOrderFlag]        BIT            NOT NULL,
    [PurchaseOrderNumber]    NVARCHAR(25)   NULL,
    [AccountNumber]          NVARCHAR(15)   NULL,
    [ShipMethod]             NVARCHAR(50)   NOT NULL,
    [CreditCardApprovalCode] VARCHAR(15)    NULL,
    [Comment]                NVARCHAR(MAX)  NULL,
    [OrderQty]               SMALLINT       NOT NULL,
    [UnitPrice]              MONEY          NOT NULL,
    [UnitPriceDiscount]      MONEY          NOT NULL,
    [LineTotal]              AS             (isnull(([UnitPrice]*((1.0)-[UnitPriceDiscount]))*[OrderQty],(0.0))),
    [AllocatedTaxAmt]        MONEY          NOT NULL,
    [AllocatedFreight]       MONEY          NOT NULL,
    [ShippingAddressLine1]   NVARCHAR(60)   NOT NULL,
    [ShippingAddressLine2]   NVARCHAR(60)   NULL,
    [ShippingCity]           NVARCHAR(30)   NOT NULL,
    [ShippingStateProvince]  NVARCHAR(50)   NOT NULL,
    [ShippingCountryRegion]  NVARCHAR(50)   NOT NULL,
    [ShippingPostalCode]     NVARCHAR(15)   NOT NULL,
    CONSTRAINT [Sales_FactSalesOrders_CustomerSK_FKC] FOREIGN KEY ([CustomerSK]) REFERENCES [Sales].[BaseDimCustomer] ([SurrogateKey]),
    CONSTRAINT [Sales_FactSalesOrders_ProductSK_FKC] FOREIGN KEY ([ProductSK]) REFERENCES [Sales].[BaseDimProduct] ([SurrogateKey]),
    CONSTRAINT [Sales_FactSalesOrders_WindowFK] FOREIGN KEY ([__window]) REFERENCES [Staging].[Windows] ([Window])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [Sales_FactSalesOrders_UC]
    ON [Sales].[FactSalesOrders]([SalesOrderDetailID] ASC);


GO

CREATE CLUSTERED COLUMNSTORE INDEX [Sales_FactSalesOrders_CCI]
    ON [Sales].[FactSalesOrders];


GO

