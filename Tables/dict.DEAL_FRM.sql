CREATE TABLE [dict].[DEAL_FRM]
(
[DEAL_FRM_KEY] [bigint] NOT NULL IDENTITY(1, 1),
[DESCR] [nvarchar] (2000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[LOC_DESCR] [nvarchar] (2000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_SYS_ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[INS_DT] [datetime] NOT NULL CONSTRAINT [DF__DEAL_FRM__INS_DT__7BB05806] DEFAULT (getdate()),
[INS_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[UPD_DT] [datetime] NOT NULL CONSTRAINT [DF__DEAL_FRM__UPD_DT__7CA47C3F] DEFAULT (getdate()),
[UPD_PROCESS_ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dict].[DEAL_FRM] ADD CONSTRAINT [PK_DEALFRM] PRIMARY KEY CLUSTERED  ([DEAL_FRM_KEY]) ON [PRIMARY]
GO
ALTER TABLE [dict].[DEAL_FRM] ADD CONSTRAINT [AK_DEALFRM] UNIQUE NONCLUSTERED  ([SRC_ID], [SRC_SYS_ID]) ON [PRIMARY]
GO
