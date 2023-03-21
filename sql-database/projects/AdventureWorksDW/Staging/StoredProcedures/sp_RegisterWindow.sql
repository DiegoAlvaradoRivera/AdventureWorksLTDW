CREATE PROCEDURE Staging.sp_RegisterWindow (
	@window 	INT, 
	@from_tm 	DATETIME, 
	@to_tm 		DATETIME)
AS
BEGIN

	INSERT INTO Staging.Windows
	(Window, FromTm, ToTm) 
	VALUES 
	(@window, @from_tm, @to_tm);

END;


GO

