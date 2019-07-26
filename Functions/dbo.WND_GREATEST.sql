SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE FUNCTION [dbo].[WND_GREATEST]
(
    @a SQL_VARIANT,
    @b SQL_VARIANT
)
RETURNS SQL_VARIANT
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE
               WHEN @a <= @b THEN
                   @b
               WHEN @b < @a THEN
                   @a
               WHEN @a IS NULL THEN
                   @b
               WHEN @b IS NULL THEN
                   @a
               ELSE
                   NULL
           END;
END;
GO
