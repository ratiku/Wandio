CREATE TABLE [dbo].[SUBM_ADD_INF]
(
[SUBM_ADD_INF_KEY] [bigint] NOT NULL IDENTITY(1, 1),
[SUBM_KEY] [bigint] NOT NULL CONSTRAINT [DF__SUBM_ADD___SUBM___0682EC34] DEFAULT ((-1)),
[COMMENT] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__SUBM_ADD___COMME__086B34A6] DEFAULT ('XNA'),
[GEN_TIMESTAMP] [smalldatetime] NULL,
[VRSN] [smallint] NULL,
[SRC_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_SYS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[DEL_FLAG] [smallint] NOT NULL CONSTRAINT [DF__SUBM_ADD___DEL_F__095F58DF] DEFAULT ((0)),
[INS_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[INS_DT] [datetime] NOT NULL CONSTRAINT [DF__SUBM_ADD___INS_D__0B47A151] DEFAULT (getdate()),
[UPD_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[UPD_DT] [datetime] NOT NULL CONSTRAINT [DF__SUBM_ADD___UPD_D__0C3BC58A] DEFAULT (getdate())
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[SUBM_ADD_INF] ADD CONSTRAINT [CK__SUBM_ADD___DEL_F__0A537D18] CHECK (([DEL_FLAG]=(0) OR [DEL_FLAG]=(1)))
GO
ALTER TABLE [dbo].[SUBM_ADD_INF] ADD CONSTRAINT [PK_SUBMADDINF] PRIMARY KEY CLUSTERED  ([SUBM_ADD_INF_KEY]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[SUBM_ADD_INF] ADD CONSTRAINT [AK_SUBMADDINF] UNIQUE NONCLUSTERED  ([SRC_ID], [SRC_SYS_ID]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[SUBM_ADD_INF] ADD CONSTRAINT [FK_SUBMADDINF_SUBM] FOREIGN KEY ([SUBM_KEY]) REFERENCES [dbo].[SUBM] ([SUBM_KEY])
GO
