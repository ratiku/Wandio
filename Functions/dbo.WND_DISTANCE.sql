SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[WND_DISTANCE] ( @SourceString nvarchar(100), @TargetString nvarchar(100) ) 

RETURNS INT
AS
BEGIN
SET @SourceString = LTRIM(RTRIM(@SourceString));
SET @TargetString = LTRIM(RTRIM(@TargetString));

DECLARE @SourceTable TABLE
(
    ID BIGINT IDENTITY(1, 1),
    String NVARCHAR(100)
);
DECLARE @TargetTable TABLE
(
    ID BIGINT IDENTITY(1, 1),
    String NVARCHAR(100)
);



INSERT INTO @SourceTable
SELECT *
FROM dbo.WND_SPLIT_STRING(@SourceString, ' ');
INSERT INTO @TargetTable
SELECT *
FROM dbo.WND_SPLIT_STRING(@TargetString, ' ');


DECLARE @SourceCount BIGINT =
        (
            SELECT COUNT(1) FROM @SourceTable
        );
DECLARE @TargetCount BIGINT =
        (
            SELECT COUNT(1) FROM @TargetTable
        );

IF @SourceCount > 7
   OR @TargetCount > 7
    RETURN 2147483647;


DECLARE @SourceCombinations TABLE
(
    ID BIGINT IDENTITY(1, 1),
    CombinationString NVARCHAR(200)
);
DECLARE @TargetCombinations TABLE
(
    ID BIGINT IDENTITY(1, 1),
    CombinationString NVARCHAR(200)
);



IF @TargetCount = 1 
BEGIN
INSERT @TargetCombinations SELECT a.String FROM @TargetTable a; 
END;
IF @TargetCount = 2 
BEGIN
INSERT @TargetCombinations SELECT a.String+' '+b.String FROM @TargetTable a CROSS JOIN @TargetTable b WHERE a.ID <> b.ID; 
END;
IF @TargetCount = 3 
BEGIN
INSERT @TargetCombinations SELECT a.String+' '+b.String+' '+c.String FROM @TargetTable a CROSS JOIN @TargetTable b CROSS JOIN @TargetTable c WHERE a.ID <> b.ID AND a.ID <> c.ID AND b.ID <> c.ID;
END;
IF @TargetCount = 4 
BEGIN
INSERT @TargetCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String FROM @TargetTable a CROSS JOIN @TargetTable b CROSS JOIN @TargetTable c CROSS JOIN @TargetTable d WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND b.ID <> c.ID AND b.ID <> d.ID AND c.ID <> d.ID;
END;
IF @TargetCount = 5 
BEGIN
INSERT @TargetCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String + ' ' + e.String FROM @TargetTable a CROSS JOIN @TargetTable b CROSS JOIN @TargetTable c CROSS JOIN @TargetTable d CROSS JOIN @TargetTable e WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND a.ID <> e.ID AND b.ID <> c.ID AND b.ID <> d.ID AND b.ID <> e.ID AND c.ID <> d.ID AND c.ID <> e.ID AND d.ID <> e.ID;
END;
IF @TargetCount = 6 
BEGIN
INSERT @TargetCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String+' ' + e.String +' ' + f.String FROM @TargetTable a CROSS JOIN @TargetTable b CROSS JOIN @TargetTable c CROSS JOIN @TargetTable d CROSS JOIN @TargetTable e CROSS JOIN @TargetTable f WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND a.ID <> e.ID AND a.ID <> f.ID AND b.ID <> c.ID AND b.ID <> d.ID AND b.ID <> e.ID AND b.ID <> f.ID AND c.ID <> d.ID AND c.ID <> e.ID AND c.ID <> f.ID AND d.ID <> e.ID AND d.ID <> f.ID AND e.ID <> f.ID;
END;
IF @TargetCount = 7 
BEGIN
INSERT @TargetCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String+' ' + e.String +' ' + f.String +' ' + g.String FROM @TargetTable a CROSS JOIN @TargetTable b CROSS JOIN @TargetTable c CROSS JOIN @TargetTable d CROSS JOIN @TargetTable e CROSS JOIN @TargetTable f CROSS JOIN @TargetTable g WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND a.ID <> e.ID AND a.ID <> f.ID AND a.ID <> g.ID AND b.ID <> c.ID AND b.ID <> d.ID AND b.ID <> e.ID AND b.ID <> f.ID AND b.ID <> g.ID AND c.ID <> d.ID AND c.ID <> e.ID AND c.ID <> f.ID AND c.ID <> g.ID AND d.ID <> e.ID AND d.ID <> f.ID AND d.ID <> g.ID AND e.ID <> f.ID AND e.ID <> g.ID AND f.ID <> g.ID;
END;

IF @SourceCount = 1 
BEGIN
INSERT @SourceCombinations SELECT a.String FROM @SourceTable a; 
END;
IF @SourceCount = 2 
BEGIN
INSERT @SourceCombinations SELECT a.String+' '+b.String FROM @SourceTable a CROSS JOIN @SourceTable b WHERE a.ID <> b.ID; 
END;
IF @SourceCount = 3 
BEGIN
INSERT @SourceCombinations SELECT a.String+' '+b.String+' '+c.String FROM @SourceTable a CROSS JOIN @SourceTable b CROSS JOIN @SourceTable c WHERE a.ID <> b.ID AND a.ID <> c.ID AND b.ID <> c.ID;
END;
IF @SourceCount = 4 
BEGIN
INSERT @SourceCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String FROM @SourceTable a CROSS JOIN @SourceTable b CROSS JOIN @SourceTable c CROSS JOIN @SourceTable d WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND b.ID <> c.ID AND b.ID <> d.ID AND c.ID <> d.ID;
END;
IF @SourceCount = 5 
BEGIN
INSERT @SourceCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String + ' ' + e.String FROM @SourceTable a CROSS JOIN @SourceTable b CROSS JOIN @SourceTable c CROSS JOIN @SourceTable d CROSS JOIN @SourceTable e WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND a.ID <> e.ID AND b.ID <> c.ID AND b.ID <> d.ID AND b.ID <> e.ID AND c.ID <> d.ID AND c.ID <> e.ID AND d.ID <> e.ID;
END;
IF @SourceCount = 6 
BEGIN
INSERT @SourceCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String+' ' + e.String +' ' + f.String FROM @SourceTable a CROSS JOIN @SourceTable b CROSS JOIN @SourceTable c CROSS JOIN @SourceTable d CROSS JOIN @SourceTable e CROSS JOIN @SourceTable f WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND a.ID <> e.ID AND a.ID <> f.ID AND b.ID <> c.ID AND b.ID <> d.ID AND b.ID <> e.ID AND b.ID <> f.ID AND c.ID <> d.ID AND c.ID <> e.ID AND c.ID <> f.ID AND d.ID <> e.ID AND d.ID <> f.ID AND e.ID <> f.ID;
END;
IF @SourceCount = 7 
BEGIN
INSERT @SourceCombinations SELECT a.String + ' ' + b.String + ' ' + c.String+ ' ' + d.String+' ' + e.String +' ' + f.String +' ' + g.String FROM @SourceTable a CROSS JOIN @SourceTable b CROSS JOIN @SourceTable c CROSS JOIN @SourceTable d CROSS JOIN @SourceTable e CROSS JOIN @SourceTable f CROSS JOIN @SourceTable g WHERE a.ID <> b.ID AND a.ID <> c.ID AND a.ID <> d.ID AND a.ID <> e.ID AND a.ID <> f.ID AND a.ID <> g.ID AND b.ID <> c.ID AND b.ID <> d.ID AND b.ID <> e.ID AND b.ID <> f.ID AND b.ID <> g.ID AND c.ID <> d.ID AND c.ID <> e.ID AND c.ID <> f.ID AND c.ID <> g.ID AND d.ID <> e.ID AND d.ID <> f.ID AND d.ID <> g.ID AND e.ID <> f.ID AND e.ID <> g.ID AND f.ID <> g.ID;
END;


DECLARE @RKDistance TABLE
(
    SourceString NVARCHAR(4000),
    TargetString NVARCHAR(4000),
    Distance BIGINT
);

DECLARE @TargetCurrentString NVARCHAR(200),
        @SourceCurrentString NVARCHAR(200);

DECLARE targetcurs CURSOR LOCAL FORWARD_ONLY READ_ONLY FOR
SELECT CombinationString
FROM @TargetCombinations;
OPEN targetcurs;
FETCH NEXT FROM targetcurs
INTO @TargetCurrentString;

WHILE @@FETCH_STATUS = 0
BEGIN

    DECLARE sourcecurs CURSOR LOCAL FORWARD_ONLY READ_ONLY FOR
    SELECT CombinationString
    FROM @SourceCombinations;

    OPEN sourcecurs;

    FETCH NEXT FROM sourcecurs
    INTO @SourceCurrentString;

    WHILE @@FETCH_STATUS = 0
    BEGIN

        INSERT INTO @RKDistance
        SELECT @SourceCurrentString,
               @TargetCurrentString,
               dbo.WND_LEVENSHTEIN(@SourceCurrentString, @TargetCurrentString);

        FETCH NEXT FROM sourcecurs
        INTO @SourceCurrentString;

    END;
    CLOSE sourcecurs;
    DEALLOCATE sourcecurs;


    FETCH NEXT FROM targetcurs
    INTO @TargetCurrentString;
END;
CLOSE targetcurs;
DEALLOCATE targetcurs;

RETURN
(
    SELECT MIN(Distance) FROM @RKDistance
);
END;;
GO
