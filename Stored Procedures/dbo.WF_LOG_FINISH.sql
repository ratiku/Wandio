SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WF_LOG_FINISH]
    @etl_log_key BIGINT,
    @row_count BIGINT
AS
BEGIN


    UPDATE jobs.ETL_LOG
    SET ETL_END_DATE = GETDATE(),
        ETL_STAT = 'FINISH',
        ROW_COUNT = @row_count
    WHERE ETL_LOG_KEY = @etl_log_key;



END;
GO
