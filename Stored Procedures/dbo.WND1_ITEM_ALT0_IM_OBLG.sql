SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_ITEM_ALT0_IM_OBLG]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_ITEM_ALT0_IM_OBLG',
            @package_body NVARCHAR(4000),
            @row_count BIGINT;
    BEGIN TRY
        /* START CONSTANTS */
        DECLARE @c_log NVARCHAR(2000) = N'',
                @c_item_tp_key BIGINT,
                @c_item_sub_tp_key BIGINT;


        SELECT @c_item_tp_key = ITEM_TP_KEY
        FROM dict.ITEM_TP
        WHERE SRC_ID = 'OBL';

        SELECT @c_item_sub_tp_key = ITEM_SUB_TP_KEY
        FROM dict.ITEM_SUB_TP
        WHERE SRC_ID = 'OBL';

        SELECT @c_log = N'@c_item_tp_key = ' + CAST(@c_item_tp_key AS NVARCHAR(50)) + N'; ';
        SELECT @c_log = @c_log + N'@c_item_sub_tp_key = ' + CAST(@c_item_sub_tp_key AS NVARCHAR(50)) + N'; ';
        /* END CONSTANTS */



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



        /* START PRE-MAPPING */

        DECLARE @curs_xml XML,
                @curs_deal_key BIGINT,
                @curs_src_id NVARCHAR(255);

        DECLARE @curs_tbl TABLE
        (
            DEAL_KEY BIGINT NOT NULL,
            SRC_ID NVARCHAR(255),
            ISSUER_NAME NVARCHAR(1000),
            ISSUER_LGL_FORM NVARCHAR(100),
            CLASS NVARCHAR(100),
            CNT BIGINT,
            COST_AMT DECIMAL(19, 4),
            CCY_ID NVARCHAR(10),
            ORDR BIGINT
        );



        DECLARE itemcurs CURSOR FORWARD_ONLY LOCAL READ_ONLY FOR
        SELECT DEAL.DEAL_KEY,
               DEAL.SRC_ID,
               DEAL.EXTENSION
        FROM dbo.SUBM WITH (NOLOCK)
            INNER JOIN dbo.DEAL WITH (NOLOCK)
                ON DEAL.SUBM_KEY = SUBM.SUBM_KEY
                   AND DEAL.SRC_SYS_ID = 'ALT0'
            INNER JOIN dict.DEAL_TP WITH (NOLOCK)
                ON DEAL_TP.DEAL_TP_KEY = DEAL.DEAL_TP_KEY
        WHERE (
                  SUBM.SRC_ID = CAST(@p_donor_id AS NVARCHAR(50)) + '.' + @p_submission_id
                  OR @p_extension = N'FULL'
              )
              AND SUBM.SRC_SYS_ID = 'ALT0'
              AND DEAL_TP.OBLG_EXTNSN_FLAG = 1
              AND DEAL.EXTENSION IS NOT NULL;

        OPEN itemcurs;

        FETCH NEXT FROM itemcurs
        INTO @curs_deal_key,
             @curs_src_id,
             @curs_xml;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            CREATE TABLE #curs_tbl_xml
            (
                ISSUER_NAME NVARCHAR(1000),
                ISSUER_LGL_FORM NVARCHAR(100),
                CLASS NVARCHAR(100),
                CNT BIGINT,
                COST_AMT DECIMAL(19, 4),
                CCY_ID NVARCHAR(10)
            );


            CREATE TABLE #curs_tbl_xml_final
            (
                ID BIGINT IDENTITY(1, 1) PRIMARY KEY,
                ISSUER_NAME NVARCHAR(1000),
                ISSUER_LGL_FORM NVARCHAR(100),
                CLASS NVARCHAR(100),
                CNT BIGINT,
                COST_AMT DECIMAL(19, 4),
                CCY_ID NVARCHAR(10)
            );

            INSERT INTO #curs_tbl_xml
            (
                ISSUER_NAME,
                ISSUER_LGL_FORM,
                CLASS,
                CNT,
                COST_AMT,
                CCY_ID
            )
            SELECT ISSUER_NAME = item.value('(//Issuer/Name)[1]', 'NVARCHAR(MAX)'),
                   ISSUER_LGL_FORM = item.value('(//Issuer/LegalForm)[1]', 'NVARCHAR(MAX)'),
                   CLASS = item.value('(Class)[1]', 'NVARCHAR(MAX)'),
                   CNT = item.value('(Count)[1]', 'BIGINT'),
                   COST_AMT = item.value('(//Cost/Value)[1]', 'DECIMAL(19,4)'),
                   CCY_ID = item.value('(//Cost/Currency)[1]', 'NCHAR(10)')
            FROM @curs_xml.nodes('/DealExtensionType/Securities/Item') a(item);

            DELETE FROM #curs_tbl_xml
            WHERE ISSUER_NAME IS NULL
                  AND ISSUER_LGL_FORM IS NULL
                  AND CLASS IS NULL
                  AND CNT IS NULL
                  AND COST_AMT IS NULL
                  AND CCY_ID IS NULL;

            INSERT INTO #curs_tbl_xml_final
            (
                ISSUER_NAME,
                ISSUER_LGL_FORM,
                CLASS,
                CNT,
                COST_AMT,
                CCY_ID
            )
            SELECT *
            FROM #curs_tbl_xml;

            INSERT INTO @curs_tbl
            (
                DEAL_KEY,
                SRC_ID,
                ISSUER_NAME,
                ISSUER_LGL_FORM,
                CLASS,
                CNT,
                COST_AMT,
                CCY_ID,
                ORDR
            )
            SELECT @curs_deal_key,
                   @curs_src_id + '.' + CAST(ID AS NVARCHAR(50)),
                   ISSUER_NAME,
                   ISSUER_LGL_FORM,
                   CLASS,
                   CNT,
                   COST_AMT,
                   CCY_ID,
                   ID
            FROM #curs_tbl_xml_final;

            DROP TABLE #curs_tbl_xml_final;
            DROP TABLE #curs_tbl_xml;

            FETCH NEXT FROM itemcurs
            INTO @curs_deal_key,
                 @curs_src_id,
                 @curs_xml;
        END;
        CLOSE itemcurs;
        DEALLOCATE itemcurs;
        /* END PRE-MAPPING */


        MERGE INTO dbo.ITEM TRG
        USING
        (
            SELECT DEAL_KEY,
                   @c_item_tp_key AS ITEM_TP_KEY,
                   @c_item_sub_tp_key AS ITEM_SUB_TP_KEY,
                   SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM @curs_tbl
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.DEAL_KEY = SRC.DEAL_KEY,
                       TRG.ITEM_TP_KEY = SRC.ITEM_TP_KEY,
                       TRG.ITEM_SUB_TP_KEY = SRC.ITEM_SUB_TP_KEY,
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
                DEAL_KEY,
                ITEM_TP_KEY,
                ITEM_SUB_TP_KEY,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.DEAL_KEY, SRC.ITEM_TP_KEY, SRC.ITEM_SUB_TP_KEY, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
