SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WF_LOG_ERROR]
    @etl_log_key BIGINT,
    @error_message NVARCHAR(2000)
    
AS
BEGIN


    UPDATE jobs.ETL_LOG
    SET ETL_END_DATE = GETDATE(),
        ETL_STAT = 'ERROR',
        ERROR_LOG = @error_message
    WHERE ETL_LOG_KEY = @etl_log_key;



END;
GO
