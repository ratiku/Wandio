SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_PT_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_PT_ALT0_IM',
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

        MERGE INTO dbo.PT_ID_CARD TRG
        USING
        (
            SELECT ISNULL(PT.PT_KEY, -2) AS PT_KEY,
                   ISNULL(ID_CARD_TP.ID_CARD_TP_KEY, -2) AS ID_CARD_TP_KEY,
                   ISNULL(CNTRY.CNTRY_KEY, -2) AS CNTRY_KEY,
                   COALESCE(
                               SubmissionPartyIdentityDocuments.DocumentIdentifier,
                               SubmissionPartyIndividuals.PersonIdentifier,
                               SubmissionPartyIndividuals.GeorgianIdNumber
                           ) AS ID,
                   SubmissionPartyIndividuals.GeorgianIdCardSerie AS CARD_SERIE_ID,
                   COALESCE(
                               SubmissionPartyIndividuals.GeorgianIdCard2Number,
                               SubmissionPartyIndividuals.GeorgianIdCardNumber
                           ) AS CARD_SERIE_NO,
                   COALESCE(
                               CAST(SubmissionPartyIdentityDocuments.ValidAfter AS DATE),
                               CAST(SubmissionPartyIndividuals.GeorgianIdCard2ValidFrom AS DATE),
                               CAST(SubmissionPartyIndividuals.GeorgianIdCardValidFrom AS DATE),
                               CAST('19010101' AS DATE)
                           ) AS ISSUE_DATE,
                   COALESCE(
                               CAST(SubmissionPartyIdentityDocuments.ValidBefore AS DATE),
                               CAST(SubmissionPartyIndividuals.GeorgianIdCard2ValidTo AS DATE),
                               CAST(SubmissionPartyIndividuals.GeorgianIdCardValidTo AS DATE),
                               CAST('29991231' AS DATE)
                           ) AS EXPR_DATE,
                   COALESCE(
                               SubmissionPartyIdentityDocuments.IssuerName,
                               SubmissionPartyIndividuals.GeorgianIdCard2IssuerName,
                               SubmissionPartyIndividuals.GeorgianIdCardIssuerName
                           ) AS ISSUER,
                   SubmissionPartyIdentityDocuments.Note AS NOTE,
                   CAST(SubmissionPartyIndividuals.DonorId AS NVARCHAR(50)) + '.'
                   + SubmissionPartyIndividuals.SubmissionId + '.' + SubmissionPartyIndividuals.PartyId AS SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM FMS.dbo.SubmissionPartyIndividuals WITH (NOLOCK)
                LEFT JOIN PT WITH (NOLOCK)
                    ON PT.SRC_ID = CAST(SubmissionPartyIndividuals.DonorId AS NVARCHAR(50)) + '.'
                                   + SubmissionPartyIndividuals.SubmissionId + '.' + SubmissionPartyIndividuals.PartyId
                       AND PT.SRC_SYS_ID = 'ALT0'
                LEFT JOIN FMS.dbo.SubmissionPartyIdentityDocuments WITH (NOLOCK)
                    ON SubmissionPartyIdentityDocuments.DonorId = SubmissionPartyIndividuals.DonorId
                       AND SubmissionPartyIdentityDocuments.SubmissionId = SubmissionPartyIndividuals.SubmissionId
                       AND SubmissionPartyIdentityDocuments.PartyId = SubmissionPartyIndividuals.PartyId
                LEFT JOIN dict.ID_CARD_TP
                    ON ID_CARD_TP.SRC_ID = CASE
                                               WHEN SubmissionPartyIndividuals.GeorgianIdCardNumber IS NOT NULL THEN
                                                   'GID'
                                               WHEN SubmissionPartyIndividuals.GeorgianIdCard2Number IS NOT NULL THEN
                                                   'GIDN'
                                               ELSE
                                                   SubmissionPartyIdentityDocuments.DocumentType
                                           END
                       AND ID_CARD_TP.SRC_SYS_ID = 'WND0'
                LEFT JOIN dict.CNTRY
                    ON CNTRY.ISO_CODE = ISNULL(SubmissionPartyIdentityDocuments.IssuingCountry, 'GEO')
                       AND CNTRY.SRC_SYS_ID = 'WND0'
            WHERE (
                      SubmissionPartyIndividuals.DonorId = @p_donor_id
                      AND SubmissionPartyIndividuals.SubmissionId = @p_submission_id
                      OR @p_extension = N'FULL'
                  )
                  AND NOT (
                              SubmissionPartyIndividuals.GeorgianIdCard2Number IS NULL
                              AND SubmissionPartyIndividuals.GeorgianIdCardNumber IS NULL
                              AND SubmissionPartyIdentityDocuments.PartyId IS NULL
                          )
        ) SRC
        ON (TRG.SRC_ID = SRC.SRC_ID)
        WHEN MATCHED THEN
            UPDATE SET TRG.CNTRY_KEY = SRC.CNTRY_KEY,
                       TRG.ID = SRC.ID,
                       TRG.CARD_SERIE_ID = SRC.CARD_SERIE_ID,
                       TRG.CARD_SERIE_NO = SRC.CARD_SERIE_NO,
                       TRG.ISSUE_DATE = SRC.ISSUE_DATE,
                       TRG.EXPR_DATE = SRC.EXPR_DATE,
                       TRG.ISSUER = SRC.ISSUER,
                       TRG.NOTE = SRC.NOTE,
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
                PT_KEY,
                ID_CARD_TP_KEY,
                CNTRY_KEY,
                ID,
                CARD_SERIE_ID,
                CARD_SERIE_NO,
                ISSUE_DATE,
                EXPR_DATE,
                ISSUER,
                NOTE,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.PT_KEY, SRC.ID_CARD_TP_KEY, SRC.CNTRY_KEY, SRC.ID, SRC.CARD_SERIE_ID, SRC.CARD_SERIE_NO,
             SRC.ISSUE_DATE, SRC.EXPR_DATE, SRC.ISSUER, SRC.NOTE, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
