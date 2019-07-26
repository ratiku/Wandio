SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[WND_DISTANCE_RX]
(
    @SourceFirstName NVARCHAR(100),
    @SourceLastName NVARCHAR(100),
    @TargetFirstName NVARCHAR(100),
    @TargetLastName NVARCHAR(100)
)
RETURNS DECIMAL(18, 5)
AS
BEGIN
    DECLARE @to_return DECIMAL(18, 5) = 999;

    SET @SourceFirstName = RTRIM(LTRIM(@SourceFirstName));
    SET @SourceLastName = RTRIM(LTRIM(@SourceLastName));
    SET @TargetFirstName = RTRIM(LTRIM(@TargetFirstName));
    SET @TargetLastName = RTRIM(LTRIM(@TargetLastName));

    DECLARE @SourceFullName NVARCHAR(200) = ISNULL(@SourceFirstName, '') + N' ' + ISNULL(@SourceLastName, '');
    DECLARE @TargetFullName NVARCHAR(200) = ISNULL(@TargetFirstName, '') + N' ' + ISNULL(@TargetLastName, '');

    RETURN dbo.WND_DISTANCE(@SourceFullName, @TargetFullName)
           / CAST(dbo.WND_GREATEST(LEN(@SourceFullName), LEN(@TargetFullName)) AS DECIMAL);

END;
GO
