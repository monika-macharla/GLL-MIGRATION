-- Repair transcript view URLs that incorrectly point to the student/download PDF.
-- Example:
--   bad:  /uploads/transcripts/8499951/pdf_transcript_student
--   good: /uploads/transcripts/8499951/pdf_transcript
-- Run this against the reports destination database.

START TRANSACTION;

-- Preview rows that will be changed.
SELECT
    COUNT(*) AS bad_view_transcript_urls
FROM credentials_transcripts
WHERE view_transcript LIKE '%/pdf_transcript_student';

UPDATE credentials_transcripts
SET view_transcript = CONCAT(
    LEFT(
        view_transcript,
        LENGTH(view_transcript) - LENGTH('/pdf_transcript_student')
    ),
    '/pdf_transcript'
)
WHERE view_transcript LIKE '%/pdf_transcript_student';

SELECT ROW_COUNT() AS fixed_view_transcript_urls;

-- Verify no bad suffixes remain before committing.
SELECT
    COUNT(*) AS remaining_bad_view_transcript_urls
FROM credentials_transcripts
WHERE view_transcript LIKE '%/pdf_transcript_student';

COMMIT;
