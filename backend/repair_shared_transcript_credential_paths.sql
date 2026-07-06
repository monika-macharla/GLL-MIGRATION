-- Repair legacy transcript credential paths that were populated with the old shared transcript location.
--
-- Replace these database names before running:
--   source_gllreports_db      = legacy/source reports database
--   destination_reports_db    = migrated destination reports database
--
-- This intentionally excludes Grand Prairie ISD rows. GPISD keeps:
-- /uploads/users/greenlight/GPISD-PDF-Transcripts/signed/{student_number}.pdf

START TRANSACTION;

-- Preview affected legacy transcript rows.
SELECT
    COUNT(*) AS bad_legacy_transcript_paths
FROM destination_reports_db.credentials_transcripts ct
JOIN destination_reports_db.credentials_all ca
    ON ca.transcripts = ct.uuid
WHERE ct.deleted_at IS NULL
  AND ca.deleted_at IS NULL
  AND ct.credential_path LIKE '/uploads/shared/%/pdf_transcript'
  AND COALESCE(ca.institution_name, '') <> 'Grand Prairie ISD';

-- Generic legacy transcript:
-- bad:  /uploads/shared/{transcript.id}/pdf_transcript
-- good credential_path: /uploads/transcripts/{transcript.id}/pdf_transcript_student
-- good view_transcript: /uploads/transcripts/{transcript.id}/pdf_transcript
UPDATE destination_reports_db.credentials_transcripts ct
JOIN destination_reports_db.credentials_all ca
    ON ca.transcripts = ct.uuid
JOIN (
    SELECT
        uuid,
        CONCAT(
            '/uploads/transcripts/',
            SUBSTRING_INDEX(SUBSTRING_INDEX(credential_path, '/', -2), '/', 1),
            '/pdf_transcript_student'
        ) AS fixed_path,
        CONCAT(
            '/uploads/transcripts/',
            SUBSTRING_INDEX(SUBSTRING_INDEX(credential_path, '/', -2), '/', 1),
            '/pdf_transcript'
        ) AS fixed_view_path
    FROM destination_reports_db.credentials_transcripts
    WHERE deleted_at IS NULL
      AND credential_path LIKE '/uploads/shared/%/pdf_transcript'
) fixed
    ON fixed.uuid = ct.uuid
SET ct.credential_path = fixed.fixed_path,
    ct.view_transcript = fixed.fixed_view_path,
    ca.credential_path = fixed.fixed_path
WHERE ct.deleted_at IS NULL
  AND ca.deleted_at IS NULL
  AND ct.credential_path LIKE '/uploads/shared/%/pdf_transcript'
  AND COALESCE(ca.institution_name, '') <> 'Grand Prairie ISD';


COMMIT;
