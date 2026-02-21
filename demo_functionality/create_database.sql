-- ============================================================
-- ClearLine Pipeline - PostgreSQL Database Creation Script
-- Database: Clearline
-- ============================================================
-- Run this in psql as a superuser:
--   psql -U postgres -f create_database.sql
--
-- Or step by step:
--   1. Create the database first (outside of this script):
--        CREATE DATABASE "Clearline";
--   2. Then connect and run the rest:
--        \c Clearline
--        \i create_database.sql
-- ============================================================


-- ============================================================
-- TABLE: Users
-- ============================================================
CREATE TABLE IF NOT EXISTS Users (
    UserID      SERIAL PRIMARY KEY,
    FirstName   VARCHAR(255) NOT NULL,
    LastName    VARCHAR(255) NOT NULL,
    Email       VARCHAR(255) NOT NULL UNIQUE,
    Role        VARCHAR(50)  NOT NULL
);


-- ============================================================
-- TABLE: Assets  (Pipeline Segments)
-- ============================================================
CREATE TABLE IF NOT EXISTS Assets (
    AssetID             SERIAL PRIMARY KEY,
    SegmentID           VARCHAR(50)     NOT NULL UNIQUE,
    Name                VARCHAR(255)    NOT NULL,
    PipeGrade           VARCHAR(10)     NOT NULL,
    DiameterInches      NUMERIC(10, 4)  NOT NULL,
    WallThicknessInches NUMERIC(10, 4)  NOT NULL,
    SeamType            VARCHAR(50)     NOT NULL,
    HeatNumber          VARCHAR(50)     NOT NULL,
    Manufacturer        VARCHAR(255)    NOT NULL,
    MTR_Link            VARCHAR(500)    NOT NULL,
    GPSLatitude         NUMERIC(10, 8)  NOT NULL,
    GPSLongitude        NUMERIC(11, 8)  NOT NULL,
    MAOP_PSIG           NUMERIC(10, 2)  NOT NULL,
    ClassLocation       VARCHAR(50)     NOT NULL,
    Jurisdiction        VARCHAR(50)     NOT NULL
);


-- ============================================================
-- TABLE: Sensors
-- ============================================================
CREATE TABLE IF NOT EXISTS Sensors (
    SensorID              SERIAL PRIMARY KEY,
    SerialNumber          VARCHAR(50)  NOT NULL UNIQUE,
    SegmentID             VARCHAR(50)  NOT NULL REFERENCES Assets(SegmentID),
    LastCalibrationDate   DATE         NOT NULL,
    CalibrationCertLink   VARCHAR(500) NOT NULL,
    CalibratedBy          VARCHAR(255) NOT NULL,
    HealthScore           INTEGER      NOT NULL
);


-- ============================================================
-- TABLE: Readings  (Append-only — hash chain)
-- ============================================================
CREATE TABLE IF NOT EXISTS Readings (
    ReadingID      BIGSERIAL PRIMARY KEY,
    Timestamp      TIMESTAMPTZ     NOT NULL,
    SegmentID      VARCHAR(50)     NOT NULL REFERENCES Assets(SegmentID),
    SensorID       INTEGER         NOT NULL REFERENCES Sensors(SensorID),
    PressurePSIG   NUMERIC(10, 2)  NOT NULL,
    MAOP_PSIG      NUMERIC(10, 2)  NOT NULL,
    RecordedBy     VARCHAR(255)    NOT NULL,
    DataSource     VARCHAR(50)     NOT NULL,
    DataQuality    VARCHAR(20)     NOT NULL,
    Notes          TEXT,
    hash_signature VARCHAR(64)     NOT NULL
);

CREATE INDEX IF NOT EXISTS IX_Readings_Timestamp   ON Readings (Timestamp DESC);
CREATE INDEX IF NOT EXISTS IX_Readings_SegmentID   ON Readings (SegmentID, Timestamp DESC);


-- ============================================================
-- TABLE: AuditTrail  (Append-only — immutable via trigger)
-- ============================================================
CREATE TABLE IF NOT EXISTS AuditTrail (
    AuditID        BIGSERIAL PRIMARY KEY,
    Timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UserID         INTEGER REFERENCES Users(UserID),
    EventType      VARCHAR(50)  NOT NULL,
    TableAffected  VARCHAR(50)  NOT NULL,
    RecordID       VARCHAR(255) NOT NULL,
    Details        TEXT         NOT NULL,
    ChangeReason   TEXT
);

-- Trigger function: block UPDATE and DELETE on AuditTrail
CREATE OR REPLACE FUNCTION fn_audit_trail_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'AuditTrail is immutable. Updates are not allowed.';
    ELSIF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'AuditTrail is immutable. Deletes are not allowed.';
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS TR_AuditTrail_NoUpdates ON AuditTrail;
CREATE TRIGGER TR_AuditTrail_NoUpdates
    BEFORE UPDATE OR DELETE ON AuditTrail
    FOR EACH ROW EXECUTE FUNCTION fn_audit_trail_immutable();


-- ============================================================
-- TABLE: Compliance
-- ============================================================
CREATE TABLE IF NOT EXISTS Compliance (
    ComplianceID   SERIAL PRIMARY KEY,
    SegmentID      VARCHAR(50)  NOT NULL REFERENCES Assets(SegmentID),
    AlertTime      TIMESTAMPTZ  NOT NULL,
    AlertType      VARCHAR(50)  NOT NULL,
    Status         VARCHAR(50)  NOT NULL,
    AcknowledgedBy INTEGER REFERENCES Users(UserID),
    AcknowledgedAt TIMESTAMPTZ
);


-- ============================================================
-- TABLE: PressureTestRecords
-- ============================================================
CREATE TABLE IF NOT EXISTS PressureTestRecords (
    TestID      SERIAL PRIMARY KEY,
    SegmentID   VARCHAR(50)    NOT NULL REFERENCES Assets(SegmentID),
    TestDate    DATE           NOT NULL,
    TestPressure NUMERIC(10, 2) NOT NULL,
    PassFail    VARCHAR(10)    NOT NULL,
    Inspector   VARCHAR(255)   NOT NULL,
    Notes       TEXT
);


-- ============================================================
-- TABLE: EngineeringReconciliation  (Immutable with versioning)
-- ============================================================
CREATE TABLE IF NOT EXISTS EngineeringReconciliation (
    NoteID              BIGSERIAL PRIMARY KEY,
    ReadingID           BIGINT REFERENCES Readings(ReadingID),
    Timestamp           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ReconcilerID        INTEGER      NOT NULL REFERENCES Users(UserID),
    ReconcilerName      VARCHAR(255) NOT NULL,
    AssetID             VARCHAR(50)  NOT NULL REFERENCES Assets(SegmentID),
    QI_Status           VARCHAR(50)  NOT NULL DEFAULT 'Pending',
    NoteText            TEXT         NOT NULL,
    VersionNumber       INTEGER      NOT NULL DEFAULT 1,
    SupersededByID      BIGINT REFERENCES EngineeringReconciliation(NoteID),
    Status              VARCHAR(20)  NOT NULL DEFAULT 'CURRENT',
    OriginalDataHash    VARCHAR(64),
    ReconciliationHash  VARCHAR(64)  NOT NULL,

    CONSTRAINT chk_qi_status CHECK (QI_Status IN ('Pending', 'QI_Reviewing', 'QI_Approved', 'QI_Rejected', 'Closed')),
    CONSTRAINT chk_status    CHECK (Status IN ('CURRENT', 'SUPERSEDED'))
);

CREATE INDEX IF NOT EXISTS IX_EngReconciliation_Timestamp  ON EngineeringReconciliation (Timestamp DESC);
CREATE INDEX IF NOT EXISTS IX_EngReconciliation_Asset      ON EngineeringReconciliation (AssetID, Status);
CREATE INDEX IF NOT EXISTS IX_EngReconciliation_Status     ON EngineeringReconciliation (Status, VersionNumber);
CREATE INDEX IF NOT EXISTS IX_EngReconciliation_QI         ON EngineeringReconciliation (QI_Status, Timestamp DESC);
CREATE INDEX IF NOT EXISTS IX_EngReconciliation_RecHash    ON EngineeringReconciliation (ReconciliationHash);
CREATE INDEX IF NOT EXISTS IX_EngReconciliation_Reading    ON EngineeringReconciliation (ReadingID, Status);

-- Trigger function: only allow updates to SupersededByID and Status (versioning fields)
CREATE OR REPLACE FUNCTION fn_eng_reconciliation_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'EngineeringReconciliation is immutable. Deletes are not allowed. Use INSERT to supersede a note.';
    END IF;

    -- On UPDATE, only SupersededByID and Status may change
    IF (NEW.Timestamp          IS DISTINCT FROM OLD.Timestamp          OR
        NEW.ReconcilerID       IS DISTINCT FROM OLD.ReconcilerID       OR
        NEW.ReconcilerName     IS DISTINCT FROM OLD.ReconcilerName     OR
        NEW.AssetID            IS DISTINCT FROM OLD.AssetID            OR
        NEW.QI_Status          IS DISTINCT FROM OLD.QI_Status          OR
        NEW.NoteText           IS DISTINCT FROM OLD.NoteText           OR
        NEW.VersionNumber      IS DISTINCT FROM OLD.VersionNumber      OR
        NEW.ReadingID          IS DISTINCT FROM OLD.ReadingID          OR
        NEW.OriginalDataHash   IS DISTINCT FROM OLD.OriginalDataHash   OR
        NEW.ReconciliationHash IS DISTINCT FROM OLD.ReconciliationHash)
    THEN
        RAISE EXCEPTION 'EngineeringReconciliation is immutable. Only SupersededByID and Status can be updated for versioning. Use INSERT to supersede a note.';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS TR_EngReconciliation_Immutable ON EngineeringReconciliation;
CREATE TRIGGER TR_EngReconciliation_Immutable
    BEFORE UPDATE OR DELETE ON EngineeringReconciliation
    FOR EACH ROW EXECUTE FUNCTION fn_eng_reconciliation_immutable();


-- ============================================================
-- Done
-- ============================================================
DO $$ BEGIN
    RAISE NOTICE 'Clearline database schema created successfully.';
    RAISE NOTICE 'Tables: Users, Assets, Sensors, Readings, AuditTrail, Compliance, PressureTestRecords, EngineeringReconciliation';
    RAISE NOTICE 'Next step: python populate_demo_data.py';
END $$;
