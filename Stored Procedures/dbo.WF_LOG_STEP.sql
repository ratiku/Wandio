SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[WF_LOG_STEP]  @etl_log_key bigint,
@log_name nvarchar(500), @log_data nvarchar(4000)
AS 
BEGIN
INSERT INTO jobs.ETL_LOG_STEP
(
    ETL_LOG_KEY,
    LOG_DATE,
    LOG_NAME,
    LOG_DATA
)
VALUES
(   @etl_log_key,         -- ETL_LOG_KEY - bigint
    GETDATE(), -- LOG_DATE - datetime
    @log_name,       -- LOG_NAME - nvarchar(200)
    @log_data       -- LOG_DATA - nvarchar(4000)
    )


END
GO
