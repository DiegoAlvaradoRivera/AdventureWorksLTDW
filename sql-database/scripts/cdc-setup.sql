

-- ACTIVATE CHANGE DATA CAPTURE AT THE DATABASE LEVEL
exec sys.sp_cdc_enable_db;
go 

-- ACTIVATE CHANGE DATA CAPTURE AT THE TABLE LEVEL
exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'product',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'productmodel',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'productcategory',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'productmodelproductdescription',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'productdescription',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'customer',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'customerAddress',
	@supports_net_changes = 1,
	@role_name = NULL;

exec sys.sp_cdc_enable_table
	@source_schema = 'saleslt',
	@source_name = 'Address',
	@supports_net_changes = 1,
	@role_name = NULL;

go 
