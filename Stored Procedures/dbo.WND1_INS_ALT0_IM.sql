SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_INS_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_INS_ALT0_IM',
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




        MERGE INTO dbo.INS TRG
        USING
        (
            SELECT ACC_TRN.ACC_TRN_KEY,
                   CASE
                       WHEN EXTENSION.value('(//TransactionExtensionType/InsuranceType)[1]', 'NVARCHAR(MAX)') IS NULL THEN
                           -1
                       ELSE
                           ISNULL(INS_TP.INS_TP_KEY, -2)
                   END AS INS_TP_KEY,
                   CASE
                       WHEN EXTENSION.value('(//TransactionExtensionType/Form)[1]', 'NVARCHAR(MAX)') IS NULL
                            OR EXTENSION.value('(//TransactionExtensionType/Form)[1]', 'NVARCHAR(MAX)') = '' THEN
                           -1
                       ELSE
                           ISNULL(INS_FRM_TP.INS_FRM_TP_KEY, -2)
                   END AS INS_FRM_TP_KEY,
                   CASE
                       WHEN EXTENSION.value('(//TransactionExtensionType/Amount/Currency)[1]', 'NVARCHAR(MAX)') IS NULL THEN
                           -1
                       ELSE
                           ISNULL(CCY.CCY_KEY, -2)
                   END CCY_KEY,
                   EXTENSION.value('(//TransactionExtensionType/Period/After)[1]', 'DATE') AS INS_VALID_FROM,
                   EXTENSION.value('(//TransactionExtensionType/Period/Before)[1]', 'DATE') AS INS_VALID_TO,
                   EXTENSION.value('(//TransactionExtensionType/Area/World)[1]', 'BIT') AS AREA_WRLD_FLAG,
                   EXTENSION.value('(//TransactionExtensionType/Amount/Value)[1]', 'DECIMAL(19,2)') AS INS_AMT,
                   CAST(CASE
                            WHEN EXTENSION.value('(//TransactionExtensionType/FranchiseRate)[1]', 'NVARCHAR(MAX)') = '' THEN
                                NULL
                            ELSE
                                EXTENSION.value('(//TransactionExtensionType/FranchiseRate)[1]', 'NVARCHAR(MAX)')
                        END AS DECIMAL(19, 2)) FRANCHS_RX,
				   
                   CASE
                       WHEN EXTENSION.value('(//TransactionExtensionType/RepaymentPeriodType)[1]', 'nvarchar(max)') = '' THEN
                           NULL
                       ELSE
                           EXTENSION.value('(//TransactionExtensionType/RepaymentPeriodType)[1]', 'nvarchar(max)')
                   END REPMT_TP_DESCR,
                   CASE
                       WHEN EXTENSION.value('(//TransactionExtensionType/Basis)[1]', 'nvarchar(max)') = '' THEN
                           NULL
                       ELSE
                           EXTENSION.value('(//TransactionExtensionType/Basis)[1]', 'nvarchar(max)')
                   END BASIS,
                   EXTENSION.value('(//TransactionExtensionType/ReinsuranceDetails/Method/Code)[1]', 'nvarchar(max)') REINS_METH_CODE,
                   EXTENSION.value('(//TransactionExtensionType/ReinsuranceDetails/Method/Note)[1]', 'nvarchar(max)') REINS_METH_NOTE,
                   EXTENSION.value('(//TransactionExtensionType/ReinsuranceDetails/Type/Code)[1]', 'nvarchar(max)') REINS_TP_CODE,
                   EXTENSION.value('(//TransactionExtensionType/ReinsuranceDetails/Type/Note)[1]', 'nvarchar(max)') REINS_TP_NOTE,
                   ACC_TRN.SRC_ID AS SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM dbo.ACC_TRN
                INNER JOIN dict.ACC_TRN_TP
                    ON ACC_TRN.ACC_TRN_TP_KEY = ACC_TRN_TP.ACC_TRN_TP_KEY
                LEFT JOIN dict.INS_TP
                    ON INS_TP.SRC_ID = EXTENSION.value('(//TransactionExtensionType/InsuranceType)[1]', 'NVARCHAR(MAX)')
                       AND INS_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.INS_FRM_TP
                    ON INS_FRM_TP.SRC_ID = EXTENSION.value('(//TransactionExtensionType/Form)[1]', 'NVARCHAR(MAX)')
                       AND INS_FRM_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.CCY
                    ON CCY.SRC_ID = EXTENSION.value('(//TransactionExtensionType/Amount/Currency)[1]', 'NVARCHAR(MAX)')
                       AND CCY.SRC_SYS_ID = 'WND0'
            WHERE EXTENSION IS NOT NULL
                  AND ACC_TRN_TP.INS_FLAG = 1
                  AND
                  (
                      (ACC_TRN.SRC_ID LIKE CAST(@p_donor_id AS NVARCHAR(50)) + '.' + @p_submission_id + '.%')
                      OR @p_extension = N'FULL'
                  )
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.ACC_TRN_KEY = SRC.ACC_TRN_KEY,
                       TRG.INS_TP_KEY = SRC.INS_TP_KEY,
                       TRG.INS_FRM_TP_KEY = SRC.INS_FRM_TP_KEY,
                       TRG.CCY_KEY = SRC.CCY_KEY,
                       TRG.INS_VALID_FROM = SRC.INS_VALID_FROM,
                       TRG.INS_VALID_TO = SRC.INS_VALID_TO,
                       TRG.AREA_WRLD_FLAG = SRC.AREA_WRLD_FLAG,
                       TRG.INS_AMT = SRC.INS_AMT,
                       TRG.FRANCHS_RX = SRC.FRANCHS_RX,
                       TRG.BASIS = SRC.BASIS,
                       TRG.REINS_METH_CODE = SRC.REINS_METH_CODE,
                       TRG.REINS_METH_NOTE = SRC.REINS_METH_NOTE,
                       TRG.REINS_TP_CODE = SRC.REINS_TP_CODE,
                       TRG.REINS_TP_NOTE = SRC.REINS_TP_NOTE,
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
                ACC_TRN_KEY,
                INS_TP_KEY,
                INS_FRM_TP_KEY,
                CCY_KEY,
                INS_VALID_FROM,
                INS_VALID_TO,
                AREA_WRLD_FLAG,
                INS_AMT,
                FRANCHS_RX,
                REPMT_TP_DESCR,
                BASIS,
                REINS_METH_CODE,
                REINS_METH_NOTE,
                REINS_TP_CODE,
                REINS_TP_NOTE,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.ACC_TRN_KEY, SRC.INS_TP_KEY, SRC.INS_FRM_TP_KEY, SRC.CCY_KEY, SRC.INS_VALID_FROM, SRC.INS_VALID_TO,
             SRC.AREA_WRLD_FLAG, SRC.INS_AMT, SRC.FRANCHS_RX, SRC.REPMT_TP_DESCR, SRC.BASIS, SRC.REINS_METH_CODE,
             SRC.REINS_METH_NOTE, SRC.REINS_TP_CODE, SRC.REINS_TP_NOTE, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
