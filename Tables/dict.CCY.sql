CREATE TABLE [dict].[CCY]
(
[CCY_KEY] [bigint] NOT NULL IDENTITY(1, 1),
[DESCR] [nvarchar] (2000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[LOC_DESCR] [nvarchar] (2000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_SYS_ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[INS_DT] [datetime] NOT NULL CONSTRAINT [DF__CCY__INS_DT__2D27B809] DEFAULT (getdate()),
[INS_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[UPD_DT] [datetime] NOT NULL CONSTRAINT [DF__CCY__UPD_DT__2E1BDC42] DEFAULT (getdate()),
[UPD_PROCESS_ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dict].[CCY] ADD CONSTRAINT [PK_CCY] PRIMARY KEY CLUSTERED  ([CCY_KEY]) ON [PRIMARY]
GO
ALTER TABLE [dict].[CCY] ADD CONSTRAINT [AK_CCY] UNIQUE NONCLUSTERED  ([SRC_ID], [SRC_SYS_ID]) ON [PRIMARY]
GO
