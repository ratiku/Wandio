SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_SUBM_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_SUBM_ALT0_IM',
            @package_body NVARCHAR(4000),
            @row_count BIGINT;
    BEGIN TRY

        SELECT @package_body = ROUTINE_DEFINITION
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_NAME = @package_name;

        EXEC @etl_log_key = dbo.WF_LOG_START @p_donor_id = @p_donor_id,
                                             @p_submission_id = @p_submission_id,
                                             @p_extension = @p_extension, -- nvarchar(500)
                                             @p_job_id = @p_job_id,       -- bigint
                                             @etl_name = @package_name;   -- nvarchar(200)


        EXEC dbo.WF_LOG_STEP @etl_log_key = @etl_log_key, -- bigint
                             @log_name = N'PACKAGE_BODY', -- nvarchar(500)
                             @log_data = @package_body;   -- nvarchar(4000)


        EXEC dbo.WF_LOG_STEP @etl_log_key = @etl_log_key,    -- bigint
                             @log_name = N'PACKAGE_START',   -- nvarchar(500)
                             @log_data = N'loading started'; -- nvarchar(4000)

        MERGE INTO dbo.SUBM TRG
        USING
        (
            SELECT Submissions.SubmissionType AS SUBM_TP_ID,
                   Submissions.[Date] AS SUBM_DATE,
                   Submissions.[Timestamp] AS SUBM_GEN_TIMESTAMP,
                   Submissions.Version AS VRSN,
                   ISNULL(Submissions.OverdueIndicator, -1) AS OVDU_FLAG,
                   ISNULL(Submissions.ActivityKind, 'XNA') ACT_KIND_TP_ID,
                   CAST(Submissions.ETag AS DATETIME) AS ETAG,
                   CAST(Submissions.DonorId AS NVARCHAR(50)) + '.' + Submissions.SubmissionId AS SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM FMS.dbo.Submissions WITH (NOLOCK)
            WHERE (
                      (Submissions.DonorId = @p_donor_id
                      AND Submissions.SubmissionId = @p_submission_id)
                      OR @p_extension = N'FULL'
                  )
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.SUBM_TP_ID = SRC.SUBM_TP_ID,
                       TRG.SUBM_DATE = SRC.SUBM_DATE,
                       TRG.SUBM_GEN_TIMESTAMP = SRC.SUBM_GEN_TIMESTAMP,
                       TRG.VRSN = SRC.VRSN,
                       TRG.OVDU_FLAG = SRC.OVDU_FLAG,
                       TRG.ACT_KIND_TP_ID = SRC.ACT_KIND_TP_ID,
                       TRG.ETAG = SRC.ETAG,
                       TRG.DEL_FLAG = 0,
                       TRG.UPD_PROCESS_ID = SUBSTRING(
                                                         CAST(@p_job_id AS VARCHAR(50)) + ':'
                                                         + CAST(@etl_log_key AS VARCHAR(50)) + '|' + TRG.UPD_PROCESS_ID,
                                                         1,
                                                         255
                                                     ),
                       TRG.UPD_DT = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT
            (
                SUBM_TP_ID,
                SUBM_DATE,
                SUBM_GEN_TIMESTAMP,
                VRSN,
                OVDU_FLAG,
                ACT_KIND_TP_ID,
                ETAG,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.SUBM_TP_ID, SRC.SUBM_DATE, SRC.SUBM_GEN_TIMESTAMP, SRC.VRSN, SRC.OVDU_FLAG, SRC.ACT_KIND_TP_ID,
             SRC.ETAG, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
             CAST(@p_job_id AS NVARCHAR(50)) + ':' + CAST(@etl_log_key AS VARCHAR(50)), GETDATE(),
             CAST(@p_job_id AS NVARCHAR(50)) + ':' + CAST(@etl_log_key AS VARCHAR(50)), GETDATE());
        SET @row_count = @@ROWCOUNT;
        EXEC dbo.WF_LOG_STEP @etl_log_key = @etl_log_key,                   -- bigint
                             @log_name = N'PACKAGE_END',                    -- nvarchar(500)
                             @log_data = N'loanding finished successfully'; -- nvarchar(4000)
        EXEC dbo.WF_LOG_FINISH @etl_log_key = @etl_log_key, -- bigint
                               @row_count = @row_count;     -- bigint




    END TRY
    BEGIN CATCH
        DECLARE @error_message NVARCHAR(2000),
                @log_data NVARCHAR(4000);

        SET @error_message = ERROR_MESSAGE();
        SET @log_data = N'loanding finished error ' + @error_message;
        EXEC dbo.WF_LOG_STEP @etl_log_key = @etl_log_key,  -- bigint
                             @log_name = N'PACKAGE_ERROR', -- nvarchar(500)
                             @log_data = @log_data;        -- nvarchar(4000)

        EXEC dbo.WF_LOG_ERROR @etl_log_key = @etl_log_key,     -- bigint
                              @error_message = @error_message; -- nvarchar(2000)


    END CATCH;
END;
GO
