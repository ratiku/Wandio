SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[WND_LEVENSHTEIN]
(
    @SourceString NVARCHAR(100),
    @TargetString NVARCHAR(100)
)
--Returns the Levenshtein Distance between @SourceString string and @TargetString
--Translated to TSQL by Rati Kunchulia
RETURNS INT
AS
BEGIN
    DECLARE @Matrix NVARCHAR(4000),
            @LD INT,
            @TargetStringLength INT,
            @SourceStringLength INT,
            @ii INT,
            @jj INT,
            @CurrentSourceChar NCHAR(1),
            @CurrentTargetChar NCHAR(1),
            @Cost INT,
            @Above INT,
            @AboveAndToLeft INT,
            @ToTheLeft INT,
            @MinimumValueOfCells INT;
    -- Step 1: Set n to be the length of s. Set m to be the length of t. 
    --                    If n = 0, return m and exit.
    --    If m = 0, return n and exit.
    --    Construct a matrix containing 0..m rows and 0..n columns.
    IF @SourceString IS NULL
       OR @TargetString IS NULL
        RETURN NULL;
    SELECT @SourceStringLength = LEN(@SourceString),
           @TargetStringLength = LEN(@TargetString),
           @Matrix = REPLICATE(NCHAR(0), (@SourceStringLength + 1) * (@TargetStringLength + 1));
    IF @SourceStringLength = 0
        RETURN @TargetStringLength;
    IF @TargetStringLength = 0
        RETURN @SourceStringLength;
    IF (@TargetStringLength + 1) * (@SourceStringLength + 1) > 4000
        RETURN -1;
    --Step 2: Initialize the first row to 0..n.
    --     Initialize the first column to 0..m.
    SET @ii = 0;
    WHILE @ii <= @SourceStringLength
    BEGIN
        SET @Matrix = STUFF(@Matrix, @ii + 1, 1, NCHAR(@ii)); --d(i, 0) = i
        SET @ii = @ii + 1;
    END;
    SET @ii = 0;
    WHILE @ii <= @TargetStringLength
    BEGIN
        SET @Matrix = STUFF(@Matrix, @ii * (@SourceStringLength + 1) + 1, 1, NCHAR(@ii)); --d(0, j) = j
        SET @ii = @ii + 1;
    END;
    --Step 3 Examine each character of s (i from 1 to n).
    SET @ii = 1;
    WHILE @ii <= @SourceStringLength
    BEGIN

        --Step 4   Examine each character of t (j from 1 to m).
        SET @jj = 1;
        WHILE @jj <= @TargetStringLength
        BEGIN
            --Step 5 and 6
            SELECT
                --Set cell d[i,j] of the matrix equal to the minimum of:
                --a. The cell immediately above plus 1: d[i-1,j] + 1.
                --b. The cell immediately to the left plus 1: d[i,j-1] + 1.
                --c. The cell diagonally above and to the left plus the cost: d[i-1,j-1] + cost
                @Above = UNICODE(SUBSTRING(@Matrix, @jj * (@SourceStringLength + 1) + @ii - 1 + 1, 1)) + 1,
                @ToTheLeft = UNICODE(SUBSTRING(@Matrix, (@jj - 1) * (@SourceStringLength + 1) + @ii + 1, 1)) + 1,
                @AboveAndToLeft
                    = UNICODE(SUBSTRING(@Matrix, (@jj - 1) * (@SourceStringLength + 1) + @ii - 1 + 1, 1))
                      + CASE
                            WHEN (SUBSTRING(@SourceString, @ii, 1)) = (SUBSTRING(@TargetString, @jj, 1)) THEN
                                0
                            ELSE
                                1
                        END; --the cost
            -- If s[i] equals t[j], the cost is 0.
            -- If s[i] doesn't equal t[j], the cost is 1.
            -- now calculate the minimum value of the three
            IF (@Above < @ToTheLeft)
               AND (@Above < @AboveAndToLeft)
                SELECT @MinimumValueOfCells = @Above;
            ELSE IF (@ToTheLeft < @Above)
                    AND (@ToTheLeft < @AboveAndToLeft)
                SELECT @MinimumValueOfCells = @ToTheLeft;
            ELSE
                SELECT @MinimumValueOfCells = @AboveAndToLeft;
            SELECT @Matrix = STUFF(@Matrix, @jj * (@SourceStringLength + 1) + @ii + 1, 1, NCHAR(@MinimumValueOfCells)),
                   @jj = @jj + 1;
        END;
        SET @ii = @ii + 1;
    END;
    --Step 7 After iteration steps (3, 4, 5, 6) are complete, distance is found in cell d[n,m]
    RETURN UNICODE(SUBSTRING(@Matrix, @SourceStringLength * (@TargetStringLength + 1) + @TargetStringLength + 1, 1));
END;
GO
