SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[WND1_ITEM_ALT0_IM_FLT]
    @p_donor_id BIGINT,
    @p_submission_id NVARCHAR(100),
    @p_job_id BIGINT,
    @p_extension NVARCHAR(500)
AS
BEGIN
    DECLARE @etl_log_key BIGINT,
            @package_name NVARCHAR(40) = N'WND1_ITEM_ALT0_IM_FLT',
            @package_body NVARCHAR(4000),
            @row_count BIGINT;
    BEGIN TRY
        /* START CONSTANTS */
        DECLARE @c_log NVARCHAR(2000) = N'',
                @c_item_tp_key BIGINT,
                @c_item_sub_tp_key BIGINT;

        SELECT @c_item_tp_key = ITEM_TP_KEY
        FROM dict.ITEM_TP
        WHERE SRC_ID = 'RST';

        SELECT @c_item_sub_tp_key = ITEM_SUB_TP_KEY
        FROM dict.ITEM_SUB_TP
        WHERE SRC_ID = 'FLT';

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
                @curs_src_id NVARCHAR(255),
				@curs_string NVARCHAR(MAX);

        DECLARE @curs_tbl TABLE
        (
            DEAL_KEY BIGINT NOT NULL,
            SRC_ID NVARCHAR(255),
            BAR_CODE NVARCHAR(200),
            ACT_TP_DESCR NVARCHAR(MAX),
            ACT_DESCR NVARCHAR(MAX),
            AMT DECIMAL(19, 4),
            CCY_ID NVARCHAR(10),
            CADASTRAL_NUMBER NVARCHAR(MAX),
            DESCR NVARCHAR(MAX),
            LOC_DESCR NVARCHAR(MAX),
            ADDR NVARCHAR(MAX),
            ORDR BIGINT
        );



        DECLARE itemcurs CURSOR FORWARD_ONLY LOCAL READ_ONLY FOR
        SELECT DEAL.DEAL_KEY,
               DEAL.SRC_ID,
               DEAL.EXTENSION,
			   CAST(DEAL.EXTENSION AS NVARCHAR(MAX)) EXTENSION_STRING
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
              AND DEAL_TP.FLT_EXTNSN_FLAG = 1
              AND DEAL.EXTENSION IS NOT NULL;

        OPEN itemcurs;

        FETCH NEXT FROM itemcurs
        INTO @curs_deal_key,
             @curs_src_id,
             @curs_xml,
			 @curs_string;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            CREATE TABLE #curs_tbl_xml
            (
                BAR_CODE NVARCHAR(200),
                ACT_TP_DESCR NVARCHAR(MAX),
                ACT_DESCR NVARCHAR(MAX),
                AMT DECIMAL(19, 4),
                CCY_ID NVARCHAR(10),
                CADASTRAL_NUMBER NVARCHAR(MAX),
                DESCR NVARCHAR(MAX),
                LOC_DESCR NVARCHAR(MAX),
                ADDR NVARCHAR(MAX)
            );


            CREATE TABLE #curs_tbl_xml_final
            (
                ID BIGINT IDENTITY(1, 1) PRIMARY KEY,
                BAR_CODE NVARCHAR(200),
                ACT_TP_DESCR NVARCHAR(MAX),
                ACT_DESCR NVARCHAR(MAX),
                AMT DECIMAL(19, 4),
                CCY_ID NVARCHAR(10),
                CADASTRAL_NUMBER NVARCHAR(MAX),
                DESCR NVARCHAR(MAX),
                LOC_DESCR NVARCHAR(MAX),
                ADDR NVARCHAR(MAX)
            );

			IF @curs_string LIKE '%<Immovables>%<Item>%'
			BEGIN
            INSERT INTO #curs_tbl_xml
            (
                BAR_CODE,
                ACT_TP_DESCR,
                ACT_DESCR,
                AMT,
                CCY_ID,
                CADASTRAL_NUMBER,
                DESCR,
                LOC_DESCR,
                ADDR
            )
            SELECT BAR_CODE = item.value('(//Barcode)[1]', 'NVARCHAR(MAX)'),
                   ACT_TP_DESCR = item.value('(//ActType)[1]', 'NVARCHAR(MAX)'),
                   ACT_DESCR = item.value('(//ActContent)[1]', 'NVARCHAR(MAX)'),
                   AMT = item.value('(//CurrencyAmount/Value)[1]', 'DECIMAL(19,4)'),
                   CCY_ID = item.value('(//CurrencyAmount/Currency)[1]', 'NVARCHAR(MAX)'),
                   CADASTRAL_NUMBER = item.value('(CadCode)[1]', 'NVARCHAR(MAX)'),
                   DESCR = item.value('(Description)[1]', 'NVARCHAR(MAX)'),
                   LOC_DESCR = item.value('(Zone)[1]', 'NVARCHAR(MAX)'),
                   ADDR = item.value('(Address/Line1)[1]', 'NVARCHAR(MAX)')
            FROM @curs_xml.nodes('/DealExtensionType/PropertyDescription/Immovables/Item') a(item);
			END
			ELSE IF @curs_string LIKE '%<EntireProperties>%<Item>%'
			BEGIN
				INSERT INTO #curs_tbl_xml
            (
                BAR_CODE,
                ACT_TP_DESCR,
                ACT_DESCR,
                AMT,
                CCY_ID,
                CADASTRAL_NUMBER,
                DESCR,
                LOC_DESCR,
                ADDR
            )
            SELECT BAR_CODE = item.value('(//Barcode)[1]', 'NVARCHAR(MAX)'),
                   ACT_TP_DESCR = item.value('(//ActType)[1]', 'NVARCHAR(MAX)'),
                   ACT_DESCR = item.value('(//ActContent)[1]', 'NVARCHAR(MAX)'),
                   AMT = item.value('(//CurrencyAmount/Value)[1]', 'DECIMAL(19,4)'),
                   CCY_ID = item.value('(//CurrencyAmount/Currency)[1]', 'NVARCHAR(MAX)'),
                   CADASTRAL_NUMBER = item.value('(CadCode)[1]', 'NVARCHAR(MAX)'),
                   DESCR = item.value('(Description)[1]', 'NVARCHAR(MAX)'),
                   LOC_DESCR = item.value('(Zone)[1]', 'NVARCHAR(MAX)'),
                   ADDR = item.value('(Address/Line1)[1]', 'NVARCHAR(MAX)')
            FROM @curs_xml.nodes('/DealExtensionType/PropertyDescription/EntireProperties/Item') a(item);
			END
            ELSE
            BEGIN
			INSERT INTO #curs_tbl_xml
            (
                BAR_CODE,
                ACT_TP_DESCR,
                ACT_DESCR,
                AMT,
                CCY_ID,
                CADASTRAL_NUMBER,
                DESCR,
                LOC_DESCR,
                ADDR
            )
            SELECT BAR_CODE = item.value('(//Barcode)[1]', 'NVARCHAR(MAX)'),
                   ACT_TP_DESCR = item.value('(//ActType)[1]', 'NVARCHAR(MAX)'),
                   ACT_DESCR = item.value('(//ActContent)[1]', 'NVARCHAR(MAX)'),
                   AMT = item.value('(//CurrencyAmount/Value)[1]', 'DECIMAL(19,4)'),
                   CCY_ID = item.value('(//CurrencyAmount/Currency)[1]', 'NVARCHAR(MAX)'),
                   CADASTRAL_NUMBER = item.value('(PropertyDescription/Immovables/Item/CadCode)[1]', 'NVARCHAR(MAX)'),
                   DESCR = item.value('(PropertyDescription/Immovables/Item/Description)[1]', 'NVARCHAR(MAX)'),
                   LOC_DESCR = item.value('(PropertyDescription/Immovables/Item/Zone)[1]', 'NVARCHAR(MAX)'),
                   ADDR = item.value('(PropertyDescription/Immovables/Item/Address/Line1)[1]', 'NVARCHAR(MAX)')
            FROM @curs_xml.nodes('/DealExtensionType') a(item);

			END
            
            DELETE FROM #curs_tbl_xml
            WHERE BAR_CODE IS NULL
                  AND ACT_TP_DESCR IS NULL
                  AND ACT_DESCR IS NULL
                  AND AMT IS NULL
                  AND CCY_ID IS NULL;

            INSERT INTO #curs_tbl_xml_final
            (
                BAR_CODE,
                ACT_TP_DESCR,
                ACT_DESCR,
                AMT,
                CCY_ID,
                CADASTRAL_NUMBER,
                DESCR,
                LOC_DESCR,
                ADDR
            )
            SELECT *
            FROM #curs_tbl_xml;

            INSERT INTO @curs_tbl
            (
                DEAL_KEY,
                SRC_ID,
                BAR_CODE,
                ACT_TP_DESCR,
                ACT_DESCR,
                AMT,
                CCY_ID,
                CADASTRAL_NUMBER,
                DESCR,
                LOC_DESCR,
                ADDR,
                ORDR
            )
            SELECT @curs_deal_key,
                   @curs_src_id + '.' + CAST(ID AS NVARCHAR(50)),
                   BAR_CODE,
                   ACT_TP_DESCR,
                   ACT_DESCR,
                   AMT,
                   CCY_ID,
                   CADASTRAL_NUMBER,
                   DESCR,
                   LOC_DESCR,
                   ADDR,
                   ID
            FROM #curs_tbl_xml_final;

            DROP TABLE #curs_tbl_xml_final;
            DROP TABLE #curs_tbl_xml;

            FETCH NEXT FROM itemcurs
            INTO @curs_deal_key,
                 @curs_src_id,
                 @curs_xml,
				 @curs_string;
        END;
        CLOSE itemcurs;
        DEALLOCATE itemcurs;
        /* END PRE-MAPPING */


        MERGE INTO dbo.ITEM TRG
        USING
        (
            SELECT a.DEAL_KEY,
                   @c_item_tp_key AS ITEM_TP_KEY,
                   @c_item_sub_tp_key AS ITEM_SUB_TP_KEY,
                   CASE
                       WHEN a.CCY_ID IS NULL THEN
                           -1
                       ELSE
                           ISNULL(CCY.CCY_KEY, -2)
                   END AS CCY_KEY,
                   a.BAR_CODE,
                   a.ACT_TP_DESCR,
                   a.ACT_DESCR,
                   a.AMT,
                   a.SRC_ID,
                   'ALT0' AS SRC_SYS_ID
            FROM @curs_tbl a
                LEFT JOIN dict.CCY
                    ON CCY.SRC_ID = CCY_ID
        ) SRC
        ON (
               TRG.SRC_ID = SRC.SRC_ID
               AND TRG.SRC_SYS_ID = 'ALT0'
           )
        WHEN MATCHED THEN
            UPDATE SET TRG.DEAL_KEY = SRC.DEAL_KEY,
                       TRG.ITEM_TP_KEY = SRC.ITEM_TP_KEY,
                       TRG.ITEM_SUB_TP_KEY = SRC.ITEM_SUB_TP_KEY,
                       TRG.CCY_KEY = SRC.CCY_KEY,
                       TRG.BAR_CODE = SRC.BAR_CODE,
                       TRG.ACT_TP_DESCR = SRC.ACT_TP_DESCR,
                       TRG.ACT_DESCR = SRC.ACT_DESCR,
                       TRG.AMT = SRC.AMT,
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
                BAR_CODE,
                ACT_TP_DESCR,
                ACT_DESCR,
                AMT,
                CCY_KEY,
                SRC_ID,
                SRC_SYS_ID,
                DEL_FLAG,
                INS_PROCESS_ID,
                INS_DT,
                UPD_PROCESS_ID,
                UPD_DT
            )
            VALUES
            (SRC.DEAL_KEY, SRC.ITEM_TP_KEY, SRC.ITEM_SUB_TP_KEY, SRC.BAR_CODE, SRC.ACT_TP_DESCR, SRC.ACT_DESCR,
             SRC.AMT, SRC.CCY_KEY, SRC.SRC_ID, SRC.SRC_SYS_ID, 0,
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
