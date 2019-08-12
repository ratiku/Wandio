SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_INSCNTRYREL_ALT0_IM]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_INSCNTRYREL_ALT0_IM',
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



        /* START PRE-MAPPING */

        DECLARE @curs_xml XML,
                @curs_ins_key BIGINT,
                @curs_src_id NVARCHAR(255);

        DECLARE @curs_tbl TABLE
        (
            INS_KEY BIGINT NOT NULL,
            SRC_ID NVARCHAR(255),
            CNTRY_ID NVARCHAR(200),
            ORDR BIGINT
        );



        DECLARE itemcurs CURSOR FORWARD_ONLY LOCAL READ_ONLY FOR
        SELECT INS.INS_KEY,
               INS.SRC_ID,
               ACC_TRN.EXTENSION
        FROM dbo.INS WITH (NOLOCK)
            INNER JOIN dbo.ACC_TRN WITH (NOLOCK)
                ON ACC_TRN.ACC_TRN_KEY = INS.ACC_TRN_KEY
                   AND INS.SRC_SYS_ID = ACC_TRN.SRC_SYS_ID
            INNER JOIN dict.ACC_TRN_TP
                ON ACC_TRN.ACC_TRN_TP_KEY = ACC_TRN_TP.ACC_TRN_TP_KEY
                   AND ACC_TRN_TP.SRC_SYS_ID = 'WND0'
        WHERE (
                  INS.SRC_ID LIKE CAST(@p_donor_id AS NVARCHAR(50)) + '.' + @p_submission_id + '.%'
                  OR @p_extension = N'FULL'
              )
              AND INS.SRC_SYS_ID = 'ALT0'
              AND ACC_TRN.EXTENSION IS NOT NULL
              AND ACC_TRN_TP.INS_FLAG = 1;;

        OPEN itemcurs;

        FETCH NEXT FROM itemcurs
        INTO @curs_ins_key,
             @curs_src_id,
             @curs_xml;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            CREATE TABLE #curs_tbl_xml
            (
                CNTRY_ID NVARCHAR(200)
            );


            CREATE TABLE #curs_tbl_xml_final
            (
                ID BIGINT IDENTITY(1, 1) PRIMARY KEY,
                CNTRY_ID NVARCHAR(200)
            );

            INSERT INTO #curs_tbl_xml
            (
                CNTRY_ID
            )
            SELECT CNTRY_ID = item.value('(.)[1]', 'NVARCHAR(MAX)')
            FROM @curs_xml.nodes('/TransactionExtensionType/Area/Region/Country') a(item);

            DELETE FROM #curs_tbl_xml
            WHERE CNTRY_ID IS NULL;

            INSERT INTO #curs_tbl_xml_final
            (
                CNTRY_ID
            )
            SELECT *
            FROM #curs_tbl_xml;

            INSERT INTO @curs_tbl
            (
                INS_KEY,
                SRC_ID,
                CNTRY_ID
            )
            SELECT @curs_ins_key,
                   @curs_src_id + '.' + CAST(ID AS NVARCHAR(50)),
                   CNTRY_ID
            FROM #curs_tbl_xml_final;

            DROP TABLE #curs_tbl_xml_final;
            DROP TABLE #curs_tbl_xml;

            FETCH NEXT FROM itemcurs
            INTO @curs_ins_key,
                 @curs_src_id,
                 @curs_xml;
        END;
        CLOSE itemcurs;
        DEALLOCATE itemcurs;
        /* END PRE-MAPPING */

        MERGE INTO dbo.INS_CNTRY_REL TRG
        USING
        (
            SELECT ISNULL(INS.INS_KEY, -2) INS_KEY,
                   ISNULL(CNTRY.CNTRY_KEY, -2) CNTRY_KEY,
                   curs_tbl.SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM @curs_tbl curs_tbl
                 LEFT JOIN dbo.INS WITH (NOLOCK)
                    ON INS.INS_KEY = curs_tbl.INS_KEY
                       AND INS.SRC_SYS_ID = 'ALT0'
                LEFT JOIN dict.CNTRY WITH (NOLOCK)
                    ON CNTRY.ISO_CODE = curs_tbl.CNTRY_ID
                       AND CNTRY.SRC_SYS_ID = 'WND0'

        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.INS_KEY = SRC.INS_KEY,
                       TRG.CNTRY_KEY = SRC.CNTRY_KEY,
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
                INS_KEY,
                CNTRY_KEY,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.INS_KEY, SRC.CNTRY_KEY, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
