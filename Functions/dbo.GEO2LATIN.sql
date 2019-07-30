SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[GEO2LATIN]
(
    @text NVARCHAR(MAX)
)
RETURNS VARCHAR(MAX)
AS
BEGIN
    DECLARE @return VARCHAR(MAX) = '';
    DECLARE @i INT = 1;

    WHILE @i <= LEN(@text)
    BEGIN
        SET @return = @return + CASE SUBSTRING(@text, @i, 1)
                                    WHEN N'ა' THEN
                                        'a'
                                    WHEN N'ბ' THEN
                                        'b'
                                    WHEN N'გ' THEN
                                        'g'
                                    WHEN N'დ' THEN
                                        'd'
                                    WHEN N'ე' THEN
                                        'e'
                                    WHEN N'ვ' THEN
                                        'v'
                                    WHEN N'ზ' THEN
                                        'z'
                                    WHEN N'თ' THEN
                                        't'
                                    WHEN N'ი' THEN
                                        'i'
                                    WHEN N'კ' THEN
                                        'k'
                                    WHEN N'ლ' THEN
                                        'l'
                                    WHEN N'მ' THEN
                                        'm'
                                    WHEN N'ნ' THEN
                                        'n'
                                    WHEN N'ო' THEN
                                        'o'
                                    WHEN N'პ' THEN
                                        'p'
                                    WHEN N'ჟ' THEN
                                        'zh'
                                    WHEN N'რ' THEN
                                        'r'
                                    WHEN N'ს' THEN
                                        's'
                                    WHEN N'ტ' THEN
                                        't'
                                    WHEN N'უ' THEN
                                        'u'
                                    WHEN N'ფ' THEN
                                        'ph'
                                    WHEN N'ქ' THEN
                                        'q'
                                    WHEN N'ღ' THEN
                                        'gh'
                                    WHEN N'ყ' THEN
                                        'y'
                                    WHEN N'შ' THEN
                                        'sh'
                                    WHEN N'ჩ' THEN
                                        'ch'
                                    WHEN N'ც' THEN
                                        'c'
                                    WHEN N'ძ' THEN
                                        'dz'
                                    WHEN N'წ' THEN
                                        'ts'
                                    WHEN N'ჭ' THEN
                                        'tch'
                                    WHEN N'ხ' THEN
                                        'kh'
                                    WHEN N'ჯ' THEN
                                        'j'
                                    WHEN N'ჰ' THEN
                                        'h'
										ELSE
                                        SUBSTRING(@text, @i, 1)
                                END;

        SET @i = @i + 1;
    END;
	RETURN @return
END;
GO
