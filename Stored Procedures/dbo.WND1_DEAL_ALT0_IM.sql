SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_DEAL_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_DEAL_ALT0_IM',
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

        MERGE INTO dbo.DEAL TRG
        USING
        (
            SELECT CASE
                       WHEN Deals.TypeCode IS NULL THEN
                           -1
                       ELSE
                           ISNULL(DEAL_TP.DEAL_TP_KEY, -2)
                   END DEAL_TP_KEY,
                   CASE
                       WHEN Deals.SubmissionId IS NULL THEN
                           -1
                       ELSE
                           ISNULL(SUBM.SUBM_KEY, -2)
                   END AS SUBM_KEY,
                   CASE
                       WHEN Deals.Country IS NULL THEN
                           -1
                       ELSE
                           ISNULL(CNTRY.CNTRY_KEY, -2)
                   END CNTRY_KEY,
                   CASE
                       WHEN Deals.Status IS NULL THEN
                           -1
                       ELSE
                           ISNULL(DEAL_STAT.DEAL_STAT_KEY, -2)
                   END DEAL_STAT_KEY,
                   CASE
                       WHEN Deals.Form IS NULL THEN
                           -1
                       ELSE
                           ISNULL(DEAL_FRM.DEAL_FRM_KEY, -2)
                   END DEAL_FRM_KEY,
                   CASE
                       WHEN Deals.Ccy IS NULL THEN
                           -1
                       ELSE
                           ISNULL(CCY.CCY_KEY, -2)
                   END CCY_KEY,
				   CASE WHEN SubmissionMainEntryDeals.DealId IS NOT NULL THEN 1 ELSE 0 END AS MAIN_DEAL_FLAG,
                   Deals.Reference AS REFERENCE,
                   Deals.Date AS DEAL_DATE,
                   Deals.Amount AS DEAL_AMT,
                   ISNULL(Deals.Purpose, 'XNA') AS DEAL_PURP,
                   ISNULL(Deals.Location, 'XNA') AS DEAL_LOC,
                   ISNULL(Deals.Subject, 'XNA') AS DEAL_SUBJ,
                   Deals.StartDate AS DEAL_START_DATE,
                   Deals.EndDate AS DEAL_END_DATE,
                   ISNULL(Deals.RegistrationIssuer, 'XNA') AS REG_ISSUER,
                   Deals.RegistrationDate AS REG_DATE,
                   ISNULL(Deals.RegistrationNumber, 'XNA') AS REG_NO,
                   dbo.WND_REMOVE_XMLNS(Deals.Extension) AS EXTENSION,
				   vDealExtensions.StockIndicator STOCK_FLAG ,
                   CAST(Deals.DonorId AS NVARCHAR(50)) + '.' + Deals.SubmissionId + '.'
                   + CAST(Deals.DealId AS NVARCHAR(50)) AS SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM FMS.dbo.Deals WITH (NOLOCK)
			    LEFT JOIN FMS.dbo.SubmissionMainEntryDeals WITH (NOLOCK)  ON SubmissionMainEntryDeals.DealId = Deals.DealId
				AND SubmissionMainEntryDeals.DonorId = Deals.DonorId AND SubmissionMainEntryDeals.SubmissionId = Deals.SubmissionId
				LEFT JOIN FMS.dbo.vDealExtensions WITH (NOLOCK)  ON vDealExtensions.DonorId = Deals.DonorId AND vDealExtensions.SubmissionId = Deals.SubmissionId AND vDealExtensions.DealId = Deals.DealId 
                LEFT JOIN dbo.SUBM WITH (NOLOCK)
                    ON SUBM.SRC_ID = CAST(Deals.DonorId AS NVARCHAR(50)) + '.' + Deals.SubmissionId
                       AND SUBM.SRC_SYS_ID = 'ALT0'
                LEFT JOIN dict.DEAL_TP WITH (NOLOCK)
                    ON DEAL_TP.SRC_ID = Deals.TypeCode
                       AND DEAL_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.CNTRY WITH (NOLOCK)
                    ON CNTRY.ISO_CODE = Deals.Country
                       AND CNTRY.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.DEAL_STAT WITH (NOLOCK)
                    ON DEAL_STAT.SRC_ID = Deals.Status
                       AND DEAL_STAT.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.DEAL_FRM WITH (NOLOCK)
                    ON DEAL_FRM.SRC_ID = Deals.Form
                       AND DEAL_FRM.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.CCY WITH (NOLOCK)
                    ON CCY.SRC_ID = Deals.Ccy
                       AND CCY.SRC_SYS_ID = 'WND0'
            WHERE (
                      Deals.DonorId = @p_donor_id
                      AND Deals.SubmissionId = @p_submission_id
                  )
                  OR @p_extension = N'FULL'
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.SUBM_KEY = SRC.SUBM_KEY,
                       TRG.DEAL_TP_KEY = SRC.DEAL_TP_KEY,
                       TRG.CNTRY_KEY = SRC.CNTRY_KEY,
                       TRG.DEAL_STAT_KEY = SRC.DEAL_STAT_KEY,
                       TRG.DEAL_FRM_KEY = SRC.DEAL_FRM_KEY,
					   TRG.CCY_KEY = SRC.CCY_KEY,
					   TRG.MAIN_DEAL_FLAG = SRC.MAIN_DEAL_FLAG,
                       TRG.REFERENCE = SRC.REFERENCE,
                       TRG.DEAL_DATE = SRC.DEAL_DATE,
                       TRG.DEAL_AMT = SRC.DEAL_AMT,
                       TRG.DEAL_PURP = SRC.DEAL_PURP,
                       TRG.DEAL_LOC = SRC.DEAL_LOC,
                       TRG.DEAL_SUBJ = SRC.DEAL_SUBJ,
                       TRG.DEAL_START_DATE = SRC.DEAL_START_DATE,
                       TRG.DEAL_END_DATE = SRC.DEAL_END_DATE,
                       TRG.REG_ISSUER = SRC.REG_ISSUER,
                       TRG.REG_DATE = SRC.REG_DATE,
                       TRG.REG_NO = SRC.REG_NO,
                       TRG.EXTENSION = SRC.EXTENSION,
					   TRG.STOCK_FLAG = SRC.STOCK_FLAG,
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
                DEAL_TP_KEY,
                CNTRY_KEY,
                DEAL_STAT_KEY,
                DEAL_FRM_KEY,
                CCY_KEY,
				MAIN_DEAL_FLAG,
                REFERENCE,
                DEAL_DATE,
                DEAL_AMT,
                DEAL_PURP,
                DEAL_LOC,
                DEAL_SUBJ,
                DEAL_START_DATE,
                DEAL_END_DATE,
                REG_ISSUER,
                REG_DATE,
                REG_NO,
                EXTENSION,
				STOCK_FLAG,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.SUBM_KEY, SRC.DEAL_TP_KEY, SRC.CNTRY_KEY, SRC.DEAL_STAT_KEY, SRC.DEAL_FRM_KEY, SRC.CCY_KEY,
			SRC.MAIN_DEAL_FLAG,
             SRC.REFERENCE, SRC.DEAL_DATE, SRC.DEAL_AMT, SRC.DEAL_PURP, SRC.DEAL_LOC, SRC.DEAL_SUBJ,
             SRC.DEAL_START_DATE, SRC.DEAL_END_DATE, SRC.REG_ISSUER, SRC.REG_DATE, SRC.REG_NO, SRC.EXTENSION, SRC.STOCK_FLAG,
             SRC.SRC_ID, SRC.SRC_SYS_ID, 0, CAST(@p_job_id AS NVARCHAR(50)) + ':' + CAST(@etl_log_key AS VARCHAR(50)),
             GETDATE(), CAST(@p_job_id AS NVARCHAR(50)) + ':' + CAST(@etl_log_key AS VARCHAR(50)), GETDATE());
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
