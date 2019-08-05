SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WF_LOG_START] @p_donor_id BIGINT,
@p_submission_id NVARCHAR(100), @p_extension NVARCHAR(500),
@p_job_id BIGINT,
@etl_name NVARCHAR(200)
AS 
BEGIN
DECLARE @etl_log_key BIGINT

INSERT INTO jobs.ETL_LOG
(
    ETL_START_DATE,
    ETL_END_DATE,
    ETL_NAME,
    P_DONOR_ID,
    P_SUMBISSION_ID,
	P_EXTENSION,
    ETL_STAT,
    ROW_COUNT,
    ERROR_LOG,
	P_JOB_ID
)
VALUES
(   GETDATE(), NULL, @etl_name, -- ETL_NAME - nvarchar(200)
    @p_donor_id,                -- P_DONOR_ID - bigint
    @p_submission_id,			-- P_SUMBISSION_ID - nvarchar(100)
	@p_extension,				-- P_EXTENSION - nvarchar(100)
    'RUNNING',                  -- ETL_STAT - varchar(7)
    NULL,                       -- ROW_COUNT - bigint
    NULL,                       -- ERROR_LOG - nvarchar(2000)
	@p_job_id
    );

SET @etl_log_key = SCOPE_IDENTITY();

RETURN @etl_log_key


END
GO
