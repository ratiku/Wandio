SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_SUBMPTREL_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_SUBMPTREL_ALT0_IM',
            @package_body NVARCHAR(4000),
            @row_count BIGINT;
    BEGIN TRY
        DECLARE @c_log NVARCHAR(2000),
                @c_subm_pt_rel_sa_id BIGINT;

        SELECT @c_subm_pt_rel_sa_id = SUBM_PT_REL_TP_KEY
        FROM dict.SUBM_PT_REL_TP
        WHERE SRC_ID = 'SA';

        SELECT @c_log = N'@c_subm_pt_rel_sa_id = ' + CAST(@c_subm_pt_rel_sa_id AS NVARCHAR(50));


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


        EXEC dbo.WF_LOG_STEP @etl_log_key = @etl_log_key, -- bigint
                             @log_name = N'CONSTANTS',    -- nvarchar(500)
                             @log_data = @c_log;          -- nvarchar(4000)



        EXEC dbo.WF_LOG_STEP @etl_log_key = @etl_log_key,    -- bigint
                             @log_name = N'PACKAGE_START',   -- nvarchar(500)
                             @log_data = N'loading started'; -- nvarchar(4000)

        MERGE INTO dbo.SUBM_PT_REL TRG
        USING
        (
            SELECT CASE
                       WHEN SubmissionParties.SubmissionId IS NULL THEN
                           -1
                       ELSE
                           ISNULL(SUBM.SUBM_KEY, -2)
                   END SUBM_KEY,
                   CASE
                       WHEN SubmissionParties.PartyId IS NULL THEN
                           -1
                       ELSE
                           ISNULL(PT.PT_KEY, -2)
                   END PT_KEY,
                   @c_subm_pt_rel_sa_id SUBM_PT_REL_TP_KEY,
                   CAST(SubmissionParties.DonorId AS NVARCHAR(50)) + '.' + SubmissionParties.SubmissionId + '.'
                   + SubmissionParties.PartyId SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM FMS.dbo.SubmissionParties WITH (NOLOCK)
                LEFT JOIN dbo.SUBM WITH (NOLOCK)
                    ON SUBM.SRC_ID = CAST(SubmissionParties.DonorId AS NVARCHAR(50)) + '.'
                                     + SubmissionParties.SubmissionId
                       AND SUBM.SRC_SYS_ID = 'ALT0'
                LEFT JOIN PT WITH (NOLOCK)
                    ON PT.SRC_ID = CAST(SubmissionParties.DonorId AS NVARCHAR(50)) + '.'
                                   + SubmissionParties.SubmissionId + '.' + SubmissionParties.PartyId
                       AND PT.SRC_SYS_ID = 'ALT0'
            WHERE (
                      SubmissionParties.DonorId = @p_donor_id
                      AND SubmissionParties.SubmissionId = @p_submission_id
                  )
                  OR @p_extension = N'FULL'
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.SUBM_KEY = SRC.SUBM_KEY,
                       TRG.PT_KEY = SRC.PT_KEY,
                       TRG.SUBM_PT_REL_TP_KEY = SRC.SUBM_PT_REL_TP_KEY,
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
                SUBM_KEY,
                PT_KEY,
                SUBM_PT_REL_TP_KEY,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.SUBM_KEY, SRC.PT_KEY, SRC.SUBM_PT_REL_TP_KEY, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
