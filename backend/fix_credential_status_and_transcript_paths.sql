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

ALTER TABLE credentials_transcripts
ADD COLUMN _tmp_view_transcript_swap varchar(2048) NULL;

UPDATE credentials_transcripts
SET _tmp_view_transcript_swap = view_transcript,
    view_transcript = credential_path,
    credential_path = _tmp_view_transcript_swap
WHERE credential_path IS NOT NULL
  AND view_transcript IS NOT NULL
  AND credential_path <> view_transcript;

ALTER TABLE credentials_transcripts
DROP COLUMN _tmp_view_transcript_swap;

UPDATE credentials_all ca
JOIN credentials_transcripts ct
  ON ca.transcripts = ct.uuid
SET ca.credential_path = ct.credential_path
WHERE ca.transcripts IS NOT NULL
  AND ct.credential_path IS NOT NULL;

COMMIT;
