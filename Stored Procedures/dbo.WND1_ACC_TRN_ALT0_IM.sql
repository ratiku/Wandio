SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_ACC_TRN_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_ACC_TRN_ALT0_IM',
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

        MERGE INTO dbo.ACC_TRN TRG
        USING
        (
            SELECT CASE
                       WHEN Transactions.SubmissionId IS NULL THEN
                           -1
                       ELSE
                           ISNULL(SUBM.SUBM_KEY, -2)
                   END AS SUBM_KEY,
                   CASE
                       WHEN Transactions.TypeCode IS NULL THEN
                           -1
                       ELSE
                           ISNULL(ACC_TRN_TP.ACC_TRN_TP_KEY, -2)
                   END AS ACC_TRN_TP_KEY,
                   CASE
                       WHEN Transactions.Ccy IS NULL THEN
                           -1
                       ELSE
                           ISNULL(CCY.CCY_KEY, -2)
                   END AS CCY_KEY,
                   CASE
                       WHEN vTransactionExtensions.BorderCrossingTransactionFrom IS NULL THEN
                           -1
                       ELSE
                           ISNULL(SRC_CNTRY.CNTRY_KEY, -2)
                   END AS SRC_CNTRY_KEY,
                   CASE
                       WHEN vTransactionExtensions.BorderCrossingTransactionTo IS NULL THEN
                           -1
                       ELSE
                           ISNULL(TRG_CNTRY.CNTRY_KEY, -2)
                   END AS TRG_CNTRY_KEY,
                   CASE
                       WHEN vTransactionExtensions.InsuranceType IS NULL THEN
                           -1
                       ELSE
                           ISNULL(INS_TP.INS_TP_KEY, -2)
                   END AS INS_TP_KEY,
                   CASE
                       WHEN vTransactionExtensions.LotteryTransactionGamblingType IS NULL THEN
                           -1
                       ELSE
                           ISNULL(GMBLNG_TP.GMBLNG_TP_KEY, -2)
                   END AS GMBLNG_TP_KEY,
                   CASE
                       WHEN SubmissionMainEntryTransactions.TransactionId IS NOT NULL THEN
                           1
                       ELSE
                           0
                   END AS MAIN_ACC_TRN_FLAG,
                   ISNULL(Transactions.Reference,'XNA') AS REFERENCE,
                   Transactions.Date AS ACC_TRN_DATE,
                   Transactions.Amount AS ACC_TRN_AMT,
                   Transactions.CashPaymentIndicator AS CASH_FLAG,
                   Transactions.Purpose AS ACC_TRN_PURP,
                   Transactions.Location AS ACC_TRN_LOC,
                   Transactions.Place AS ACC_TRN_PLC,
                   dbo.WND_REMOVE_XMLNS(Transactions.Extension) AS EXTENSION,
                   CAST(Transactions.DonorId AS NVARCHAR(50)) + '.' + Transactions.SubmissionId + '.'
                   + CAST(Transactions.TransactionId AS NVARCHAR(50)) AS SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM FMS.dbo.Transactions WITH (NOLOCK)
                LEFT JOIN FMS.dbo.SubmissionMainEntryTransactions WITH (NOLOCK)
                    ON SubmissionMainEntryTransactions.DonorId = Transactions.DonorId
                       AND SubmissionMainEntryTransactions.SubmissionId = Transactions.SubmissionId
                       AND SubmissionMainEntryTransactions.TransactionId = Transactions.TransactionId
                LEFT JOIN FMS.dbo.vTransactionExtensions WITH (NOLOCK)
                    ON vTransactionExtensions.DonorId = Transactions.DonorId
                       AND vTransactionExtensions.SubmissionId = Transactions.SubmissionId
                       AND vTransactionExtensions.TransactionId = Transactions.TransactionId
                LEFT JOIN dict.CNTRY SRC_CNTRY
                    ON SRC_CNTRY.ISO_CODE = vTransactionExtensions.BorderCrossingTransactionFrom
                       AND SRC_CNTRY.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.CNTRY TRG_CNTRY
                    ON TRG_CNTRY.ISO_CODE = vTransactionExtensions.BorderCrossingTransactionTo
                       AND TRG_CNTRY.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.INS_TP
                    ON INS_TP.SRC_ID = vTransactionExtensions.InsuranceType
                       AND INS_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.GMBLNG_TP
                    ON GMBLNG_TP.SRC_ID = vTransactionExtensions.LotteryTransactionGamblingType
                       AND GMBLNG_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dbo.SUBM WITH (NOLOCK)
                    ON SUBM.SRC_ID = CAST(SubmissionMainEntryTransactions.DonorId AS NVARCHAR(50)) + '.'
                                     + SubmissionMainEntryTransactions.SubmissionId
                       AND SUBM.SRC_SYS_ID = 'ALT0'
                LEFT JOIN dict.ACC_TRN_TP WITH (NOLOCK)
                    ON ACC_TRN_TP.SRC_ID = Transactions.TypeCode
                       AND ACC_TRN_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.CCY WITH (NOLOCK)
                    ON CCY.SRC_ID = Transactions.Ccy
                       AND CCY.SRC_SYS_ID = 'WND0'
            WHERE (
                      Transactions.DonorId = @p_donor_id
                      AND Transactions.SubmissionId = @p_submission_id
                  )
                  OR @p_extension = N'FULL'
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.SUBM_KEY = SRC.SUBM_KEY,
                       TRG.ACC_TRN_TP_KEY = SRC.ACC_TRN_TP_KEY,
                       TRG.CCY_KEY = SRC.CCY_KEY,
                       TRG.SRC_CNTRY_KEY = SRC.SRC_CNTRY_KEY,
                       TRG.TRG_CNTRY_KEY = SRC.TRG_CNTRY_KEY,
                       TRG.INS_TP_KEY = SRC.INS_TP_KEY,
                       TRG.GMBLNG_TP_KEY = SRC.GMBLNG_TP_KEY,
                       TRG.MAIN_ACC_TRN_FLAG = SRC.MAIN_ACC_TRN_FLAG,
                       TRG.REFERENCE = SRC.REFERENCE,
                       TRG.ACC_TRN_DATE = SRC.ACC_TRN_DATE,
                       TRG.ACC_TRN_AMT = SRC.ACC_TRN_AMT,
                       TRG.CASH_FLAG = SRC.CASH_FLAG,
                       TRG.ACC_TRN_PURP = SRC.ACC_TRN_PURP,
                       TRG.ACC_TRN_LOC = SRC.ACC_TRN_LOC,
                       TRG.ACC_TRN_PLC = SRC.ACC_TRN_PLC,
                       TRG.EXTENSION = SRC.EXTENSION,
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
                ACC_TRN_TP_KEY,
                CCY_KEY,
                SRC_CNTRY_KEY,
                TRG_CNTRY_KEY,
                INS_TP_KEY,
                GMBLNG_TP_KEY,
                MAIN_ACC_TRN_FLAG,
                REFERENCE,
                ACC_TRN_DATE,
                ACC_TRN_AMT,
                CASH_FLAG,
                ACC_TRN_PURP,
                ACC_TRN_LOC,
                ACC_TRN_PLC,
                EXTENSION,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.SUBM_KEY, SRC.ACC_TRN_TP_KEY, SRC.CCY_KEY, SRC.SRC_CNTRY_KEY, SRC.TRG_CNTRY_KEY, SRC.INS_TP_KEY,
             SRC.GMBLNG_TP_KEY, SRC.MAIN_ACC_TRN_FLAG, SRC.REFERENCE, SRC.ACC_TRN_DATE, SRC.ACC_TRN_AMT, SRC.CASH_FLAG,
             SRC.ACC_TRN_PURP, SRC.ACC_TRN_LOC, SRC.ACC_TRN_PLC, SRC.EXTENSION, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
