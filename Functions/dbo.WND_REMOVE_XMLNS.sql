SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[WND_REMOVE_XMLNS](@xml XML)
RETURNS XML
AS BEGIN
DECLARE @xml_text NVARCHAR(MAX) = CAST(@xml AS NVARCHAR(MAX))

SET @xml_text = REPLACE(@xml_text,' xmlns="http://schemas.fms.gov.ge/main/v1.0"','');
SET @xml_text = REPLACE(@xml_text,' xmlns:d3p1="http://schemas.fms.gov.ge/common/v1.0"','')
SET @xml_text = REPLACE(@xml_text,' xmlns:d4p1="http://schemas.fms.gov.ge/common/v1.0"','');
SET @xml_text = REPLACE(@xml_text,' xmlns:d5p1="http://schemas.fms.gov.ge/common/v1.0"','');
SET @xml_text = REPLACE(@xml_text,' xmlns:d2p1="http://schemas.fms.gov.ge/common/v1.0"','');
SET @xml_text = REPLACE(@xml_text,'d2p1:','');
SET @xml_text = REPLACE(@xml_text,'d4p1:','');
SET @xml_text = REPLACE(@xml_text,'d3p1:','');
SET @xml_text = REPLACE(@xml_text,'d5p1:','');
SET @xml_text = REPLACE(@xml_text,' i:nil=',' nil=');



RETURN CAST(@xml_text AS XML)

END
GO
