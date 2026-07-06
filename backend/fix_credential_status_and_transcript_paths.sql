-- Repair migrated credential statuses and transcript view/download paths.
-- Run this against the reports destination database, e.g. gllreportsnewnew.

START TRANSACTION;

UPDATE credentials_transcripts
SET status = 2
WHERE status = 1;

UPDATE credentials_digital_badges
SET status = 2
WHERE status = 1;

UPDATE credentials_recommendation_letters
SET status = 2
WHERE status = 1;

UPDATE credentials_resume
SET status = 2
WHERE status = 1;

UPDATE credentials_self_uploads
SET status = 2
WHERE status = 1;

UPDATE credentials_all
SET status = 2
WHERE status = 1
  AND credential_type IN (1, 2, 3, 4, 5);

UPDATE credentials_transcripts
SET view_transcript = CONCAT(
    LEFT(
        view_transcript,
        LENGTH(view_transcript) - LENGTH('/pdf_transcript_student')
    ),
    '/pdf_transcript'
)
WHERE view_transcript LIKE '%/pdf_transcript_student';

UPDATE credentials_all ca
JOIN credentials_transcripts ct
  ON ca.transcripts = ct.uuid
SET ca.credential_path = ct.credential_path
WHERE ca.transcripts IS NOT NULL
  AND ct.credential_path IS NOT NULL;

COMMIT;
