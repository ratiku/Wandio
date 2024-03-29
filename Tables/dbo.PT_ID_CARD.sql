CREATE TABLE [dbo].[PT_ID_CARD]
(
[PT_ID_CARD_KEY] [bigint] NOT NULL IDENTITY(1, 1),
[PT_KEY] [bigint] NOT NULL CONSTRAINT [DF__PT_ID_CAR__PT_KE__4277DAAA] DEFAULT ((-1)),
[ID_CARD_TP_KEY] [bigint] NOT NULL CONSTRAINT [DF__PT_ID_CAR__ID_CA__4460231C] DEFAULT ((-1)),
[CNTRY_KEY] [bigint] NOT NULL CONSTRAINT [DF__PT_ID_CAR__CNTRY__46486B8E] DEFAULT ((-1)),
[ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[CARD_SERIE_ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[CARD_SERIE_NO] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[ISSUE_DATE] [date] NOT NULL CONSTRAINT [DF__PT_ID_CAR__ISSUE__4830B400] DEFAULT ('1000-01-01'),
[EXPR_DATE] [date] NOT NULL CONSTRAINT [DF__PT_ID_CAR__EXPR___4924D839] DEFAULT ('2999-12-31'),
[ISSUER] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[NOTE] [nvarchar] (2000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[SRC_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[SRC_SYS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[DEL_FLAG] [smallint] NOT NULL CONSTRAINT [DF__PT_ID_CAR__DEL_F__4A18FC72] DEFAULT ((0)),
[INS_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[INS_DT] [datetime] NOT NULL CONSTRAINT [DF__PT_ID_CAR__INS_D__4C0144E4] DEFAULT (getdate()),
[UPD_PROCESS_ID] [varchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[UPD_DT] [datetime] NOT NULL CONSTRAINT [DF__PT_ID_CAR__UPD_D__4CF5691D] DEFAULT (getdate())
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PT_ID_CARD] ADD CONSTRAINT [CK__PT_ID_CAR__DEL_F__4B0D20AB] CHECK (([DEL_FLAG]=(0) OR [DEL_FLAG]=(1)))
GO
ALTER TABLE [dbo].[PT_ID_CARD] ADD CONSTRAINT [PK_PTIDCARD] PRIMARY KEY CLUSTERED  ([PT_ID_CARD_KEY]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PT_ID_CARD] ADD CONSTRAINT [AK_PTIDCARD] UNIQUE NONCLUSTERED  ([SRC_ID], [SRC_SYS_ID]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PT_ID_CARD] ADD CONSTRAINT [FK_PTIDCARD_CNTRY] FOREIGN KEY ([CNTRY_KEY]) REFERENCES [dict].[CNTRY] ([CNTRY_KEY])
GO
ALTER TABLE [dbo].[PT_ID_CARD] ADD CONSTRAINT [FK_PTIDCARD_IDCARDTP] FOREIGN KEY ([ID_CARD_TP_KEY]) REFERENCES [dict].[ID_CARD_TP] ([ID_CARD_TP_KEY])
GO
ALTER TABLE [dbo].[PT_ID_CARD] ADD CONSTRAINT [FK_PTIDCARD_PT] FOREIGN KEY ([PT_KEY]) REFERENCES [dbo].[PT] ([PT_KEY])
GO
