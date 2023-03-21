create function Staging.fn_IntervalsIntersect(
	@start1			datetime,
	@end1			datetime,
	@start2			datetime,
	@end2			datetime
)
returns bit
as
begin
	declare @result bit
	if((@end1 > @start2) AND (@end2 > @start1))
		set @result = 1
	else
		set @result = 0
	return @result
end;


go

