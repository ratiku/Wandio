CREATE TABLE [dbo].[INS_CNTRY_REL]
(
[INS_CNTRY_REL_KEY] [bigint] NOT NULL IDENTITY(1, 1),
[INS_KEY] [bigint] NOT NULL CONSTRAINT [DF__INS_CNTRY__INS_K__24D2692A] DEFAULT ((-1)),
[CNTRY_KEY] [bigint] NOT NULL CONSTRAINT [DF__INS_CNTRY__CNTRY__26BAB19C] DEFAULT ((-1)),
[SRC_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_SYS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[DEL_FLAG] [smallint] NOT NULL CONSTRAINT [DF__INS_CNTRY__DEL_F__28A2FA0E] DEFAULT ((0)),
[INS_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[INS_DT] [datetime] NOT NULL CONSTRAINT [DF__INS_CNTRY__INS_D__2A8B4280] DEFAULT (getdate()),
[UPD_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[UPD_DT] [datetime] NOT NULL CONSTRAINT [DF__INS_CNTRY__UPD_D__2B7F66B9] DEFAULT (getdate())
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[INS_CNTRY_REL] ADD CONSTRAINT [CK__INS_CNTRY__DEL_F__29971E47] CHECK (([DEL_FLAG]=(0) OR [DEL_FLAG]=(1)))
GO
ALTER TABLE [dbo].[INS_CNTRY_REL] ADD CONSTRAINT [PK_INSCNTRYREL] PRIMARY KEY CLUSTERED  ([INS_CNTRY_REL_KEY]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[INS_CNTRY_REL] ADD CONSTRAINT [AK_INSCNTRYREL] UNIQUE NONCLUSTERED  ([SRC_ID], [SRC_SYS_ID]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[INS_CNTRY_REL] ADD CONSTRAINT [FK_INSCNTRYREL_CNTRY] FOREIGN KEY ([CNTRY_KEY]) REFERENCES [dict].[CNTRY] ([CNTRY_KEY])
GO
ALTER TABLE [dbo].[INS_CNTRY_REL] ADD CONSTRAINT [FK_INSCNTRYREL_INS] FOREIGN KEY ([INS_KEY]) REFERENCES [dbo].[INS] ([INS_KEY])
GO
