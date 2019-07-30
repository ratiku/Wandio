SET QUOTED_IDENTIFIER OFF
GO
SET ANSI_NULLS OFF
GO
CREATE FUNCTION [dbo].[WND_CLR_DISTANCE] (@source [nvarchar] (max), @target [nvarchar] (max))
RETURNS [int]
WITH EXECUTE AS CALLER
EXTERNAL NAME [WND].[UserDefinedFunctions].[GetDistance]
GO
