-- Create Engineering Reconciliation table for immutable engineering notes
-- This table is APPEND-ONLY (no updates allowed) for audit trail integrity
USE [ClearLinePipeline]
GO

-- Drop table if it exists (for clean creation)
IF OBJECT_ID('dbo.EngineeringReconciliation', 'U') IS NOT NULL
    DROP TABLE dbo.EngineeringReconciliation
GO

CREATE TABLE [dbo].[EngineeringReconciliation](
    [NoteID] [bigint] IDENTITY(1,1) NOT NULL,
    [ReadingID] [bigint] NULL,
    [Timestamp] [datetime2](7) NOT NULL DEFAULT (GETDATE()),
    [ReconcilerID] [int] NOT NULL,
    [ReconcilerName] [varchar](255) NOT NULL,
    [AssetID] [varchar](50) NOT NULL,
    [QI_Status] [varchar](50) NOT NULL DEFAULT ('Pending'),
    [NoteText] [varchar](max) NOT NULL,
    [VersionNumber] [int] NOT NULL DEFAULT (1),
    [SupersededByID] [bigint] NULL,
    [Status] [varchar](20) NOT NULL DEFAULT ('CURRENT'),
    [OriginalDataHash] [varchar](64) NULL,
    [ReconciliationHash] [varchar](64) NOT NULL,
    PRIMARY KEY CLUSTERED ([NoteID] ASC),
    FOREIGN KEY([ReconcilerID]) REFERENCES [dbo].[Users]([UserID]),
    FOREIGN KEY([AssetID]) REFERENCES [dbo].[Assets]([SegmentID]),
    FOREIGN KEY([SupersededByID]) REFERENCES [dbo].[EngineeringReconciliation]([NoteID]),
    FOREIGN KEY([ReadingID]) REFERENCES [dbo].[Readings]([ReadingID]),
    CHECK ([QI_Status] IN ('Pending', 'QI_Reviewing', 'QI_Approved', 'QI_Rejected', 'Closed')),
    CHECK ([Status] IN ('CURRENT', 'SUPERSEDED'))
)
GO

-- Create indexes for better performance
CREATE NONCLUSTERED INDEX [IX_EngReconciliation_Timestamp] ON [dbo].[EngineeringReconciliation]([Timestamp] DESC)
GO

CREATE NONCLUSTERED INDEX [IX_EngReconciliation_Asset] ON [dbo].[EngineeringReconciliation]([AssetID], [Status])
GO

CREATE NONCLUSTERED INDEX [IX_EngReconciliation_Status] ON [dbo].[EngineeringReconciliation]([Status], [VersionNumber])
GO

CREATE NONCLUSTERED INDEX [IX_EngReconciliation_QI] ON [dbo].[EngineeringReconciliation]([QI_Status], [Timestamp] DESC)
GO

CREATE NONCLUSTERED INDEX [IX_EngReconciliation_RecHash] ON [dbo].[EngineeringReconciliation]([ReconciliationHash])
GO

CREATE NONCLUSTERED INDEX [IX_EngReconciliation_Reading] ON [dbo].[EngineeringReconciliation]([ReadingID], [Status])
GO

-- Create trigger to prevent updates (immutable table - insert only)
-- ONLY allows updating SupersededByID and Status fields for versioning
CREATE TRIGGER TR_EngineeringReconciliation_NoUpdates
ON [dbo].[EngineeringReconciliation]
INSTEAD OF UPDATE
AS
BEGIN
    -- Check if ONLY SupersededByID or Status fields are being updated
    -- Hash fields are immutable and cannot be updated
    IF EXISTS (
        SELECT * FROM inserted i
        INNER JOIN deleted d ON i.NoteID = d.NoteID
        WHERE i.Timestamp <> d.Timestamp
           OR i.ReconcilerID <> d.ReconcilerID
           OR i.ReconcilerName <> d.ReconcilerName
           OR i.AssetID <> d.AssetID
           OR i.QI_Status <> d.QI_Status
           OR i.NoteText <> d.NoteText
           OR i.VersionNumber <> d.VersionNumber
           OR ISNULL(i.ReadingID, 0) <> ISNULL(d.ReadingID, 0)
           OR ISNULL(i.OriginalDataHash, '') <> ISNULL(d.OriginalDataHash, '')
           OR i.ReconciliationHash <> d.ReconciliationHash
    )
    BEGIN
        RAISERROR ('EngineeringReconciliation table is immutable. Only SupersededByID and Status can be updated for versioning. Use INSERT to supersede a note.', 16, 1)
        ROLLBACK TRANSACTION
        RETURN
    END

    -- Allow update only for SupersededByID and Status
    UPDATE e
    SET e.SupersededByID = i.SupersededByID,
        e.Status = i.Status
    FROM [dbo].[EngineeringReconciliation] e
    INNER JOIN inserted i ON e.NoteID = i.NoteID
END
GO

-- Create trigger to prevent deletes (immutable table - insert only)
CREATE TRIGGER TR_EngineeringReconciliation_NoDeletes
ON [dbo].[EngineeringReconciliation]
INSTEAD OF DELETE
AS
BEGIN
    RAISERROR ('EngineeringReconciliation table is immutable. Deletes are not allowed. Use INSERT to supersede a note.', 16, 1)
    ROLLBACK TRANSACTION
END
GO

PRINT 'EngineeringReconciliation table created successfully!'
PRINT 'Table is IMMUTABLE - Only INSERT operations allowed'
PRINT 'To correct a note, INSERT a new note that supersedes the original'
GO
