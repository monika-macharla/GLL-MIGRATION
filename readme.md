1. gllauthservice:

```
source db:                          destination db

1. insert roles

2. institution                         institutions,campuses(gllauthservicemigration)

3. gl_user                             users(gllauthservicemigration)

4. gl_user                              user_role

5. gl_user                              user_institution

gl_parent_student                       parent_student


6. gl_user                              user_profile(gllauthservicemigration) 

7. jhi_user                            user_hashed_password(gllauthservicemigration)

jhi_user                               user_hashed_password_expires_at
8. enrollment                          user_enrollments(gllauthservicemigration)


9. institution_registrar              registrars

10. student_preference                 my_preferences

11. ferpa                             ferpa

12. nsapi_criteria                    scholarship_prefernces

13. student_credential_visibility     module_permissions

14. institution_user                  user_campus

16. scholarship_activity             scholarship_user_activity

17. esc_sftp_user                    institution_sftp_credentials

==========================================================================================================================
2. gllreports


17. transcript                           credentials_transcript, credentials_all
                                         credential_path = /uploads/highschool/{transcript.credential_id}/pdf_transcript_student
18. certificate                     p     credentials_certifications, credentials_all


13. badge                           credentials_digital_badges,(gllreports)(support gllauthservicemigration)
                                    credentials_digital_badge_info,credentials_all
14. resume                                                          credentials_resume, credentials_all
15. recommendation_letter,recommendation_letter_request             credentials_recommendation_letters. credentials_all

16. other-credentials                    self-uploads, credentials_all
19. badge_shared                         credentials_shared

15. employment_history                employment





===========================================================================================================


//3. glldataingestion

1. gl_student                               import_students
2. gl_parent                                import_parents
3. hs_other_creds                           import_apibs

4. hs_class_rank_gpa                        import_class_rank_gpa

5. hs_course_information                    import_course_information

6. hs_test_assessment                       import_student_test_assessments

7. hs_student_graduation_profile           import_student_graduation_profile

8. hs_awarding_credit                      import_schools_awarding_credits

9.hs_transcript                           import_credit_summary

10. covid_vaccine_meta_data                vaccination_certificate_data

11. inst_holds_ext                         holds

12. transcript_ext                           import_edi_transcript_ext(fixes)

13. cc_degree_awarded                      import_edi_award

14. cc_courses / cc_course                 import_edi_courses / import_edi_course

15. cc_courses                             import_edi_courses

16. cc_term                                import_edi_semester

17.

Blockchain Mapping CSV Import
------------------------------------------------------------

Import every CSV file from a folder into `gllreportsdevmigration.blockchain_mapping`:

```bash
python3 backend/import_blockchain_mapping_csv.py /home/xelpmoc/Documents/Projects/GLL-CODE/blockchain/pdf-hash-batch-outputs
```

The script reads MySQL connection settings from `destination_db` in `config.yaml`.
You can override the target database or credentials when needed:

```bash
MYSQL_HOST=localhost MYSQL_USER=root MYSQL_PASSWORD=your_password \
python3 backend/import_blockchain_mapping_csv.py /path/to/csv-folder \
  --database gllreportsdevmigration
```

CSV headers should match columns in `blockchain_mapping`. Extra CSV columns are
ignored, blank values are imported as `NULL`, and files are inserted in batches.
















Badge Migration Logic
------------------------------------------------------------

Source Table:
badge

Destination Tables:
1. credentials_digital_badges
2. credentials_digital_badge_info


WHERE CONDITIONS FOR TESTING
------------------------------------------------------------

Only migrate records where:

badge.user_id = 501

AND

badge.issuer_id IS NOT NULL


USER_ID MAPPING LOGIC
------------------------------------------------------------

badge.user_id
    → gl_user.id
    → gl_user.username
    → users.user_name
    → users.uuid
    → credentials_digital_badges.user_id


INSTITUTION_ID MAPPING LOGIC
------------------------------------------------------------

users.uuid
    → user_institution.user_uuid
    → user_institution.institution_uuid
    → credentials_digital_badges.institution_id


CREATED_BY MAPPING LOGIC
------------------------------------------------------------

badge.issuer_id
    → gl_user.id
    → gl_user.username
    → users.user_name
    → users.uuid
    → credentials_digital_badges.created_by


BADGE INFO TABLE RELATION
------------------------------------------------------------

credentials_digital_badges.uuid
    → credentials_digital_badge_info.badge_id

credentials_digital_badges.uuid
    → credentials_digital_badge_info.badgeId


DEFAULT VALUES
------------------------------------------------------------

credentials_digital_badges.credential_type = 3

credentials_digital_badges.status = 2

credentials_digital_badge_info.pdf_path = NULL


FILE MAPPING
------------------------------------------------------------

badge.image
    → credentials_digital_badges.file_path

badge.image
    → credentials_digital_badges.file_name
      (filename extracted from URL)

File Extension Mapping:

.png  → image/png
.jpg  → image/jpeg
.jpeg → image/jpeg
.json → application/json
.pdf  → application/pdf


BADGE INFO TABLE MAPPING
------------------------------------------------------------

badge.badge_name
    → badge_name

badge.description
    → badge_description

badge.criteria
    → earning_criteria

badge.badge_issuer_details.name
    → issuer_name

badge.expires
    → expires_on

badge.image
    → badge_image_url

NULL
    → pdf_path



    ======================================

# Resume Migration Mapping

Source Table:
resume

Destination Tables:
1. credentials_resume
2. credentials_all


------------------------------------------------------------
WHERE CONDITION
------------------------------------------------------------

resume.user_id IS NOT NULL


------------------------------------------------------------
USER_ID MAPPING
------------------------------------------------------------

resume.user_id
    → gl_user.id
    → gl_user.username
    → users.user_name
    → users.uuid
    → credentials_resume.user_id


------------------------------------------------------------
INSTITUTION_ID MAPPING
------------------------------------------------------------

users.uuid
    → user_institution.user_uuid
    → user_institution.institution_uuid
    → credentials_resume.institution_id


------------------------------------------------------------
ENROLLMENT_CODE MAPPING
------------------------------------------------------------

resume.user_id
    → user_enrollments.student_number
    → user_enrollments.enrollment_code
    → credentials_resume.enrollment_code


------------------------------------------------------------
CREATED_BY / UPDATED_BY MAPPING
------------------------------------------------------------

credentials_resume.user_id
    → credentials_resume.created_by

credentials_resume.user_id
    → credentials_resume.updated_by


------------------------------------------------------------
DATE MAPPING
------------------------------------------------------------

resume.uploaded_date
    → credentials_resume.created_at

resume.uploaded_date
    → credentials_resume.updated_at


------------------------------------------------------------
credentials_resume COLUMN MAPPING
------------------------------------------------------------

UUID v4
    → uuid

resume.uploaded_date
    → created_at

resume.uploaded_date
    → updated_at

mapped users.uuid
    → user_id

mapped institution_uuid
    → institution_id

"default filepath"
    → file_path

"default filepath"
    → file_name

"default file type"
    → file_type

2
    → status

resume.blockchain_hash
    → credential_path

6
    → credential_type

mapped users.uuid
    → created_by

mapped users.uuid
    → updated_by

mapped enrollment_code
    → enrollment_code


------------------------------------------------------------
credentials_all COLUMN MAPPING
------------------------------------------------------------

UUID v4
    → uuid

mapped users.uuid
    → user_id

mapped institution_uuid
    → institution_id

6
    → credential_type

2
    → status

credentials_resume.uuid
    → resume

resume.blockchain_hash
    → credential_path

mapped enrollment_code
    → enrollment_code

resume.uploaded_date
    → issued_on

NULL
    → generated_on


------------------------------------------------------------
DEFAULT VALUES
------------------------------------------------------------

credentials_resume.credential_type = 6

credentials_resume.status = 2

credentials_resume.file_path = "map_filepath_here"

credentials_resume.file_name = "map_filepath_here"

credentials_resume.file_type = "map_file_type"

credentials_resume.enrollment_code = "map_enrollment_code"

credentials_all.credential_type = 6

credentials_all.status = 2

credentials_all.generated_on = NULL


------------------------------------------------------------
LOOKUP TABLES USED
------------------------------------------------------------

gl_user
    → source username lookup

users
    → destination uuid lookup

user_institution
    → institution_uuid lookup

user_enrollments
    → enrollment_code lookup


    =================================================

# Recommendation Letter Migration Mapping

Source Table:
recommendation_request

Destination Tables:
1. credentials_recommendation_letters
2. credentials_all


------------------------------------------------------------
WHERE CONDITION
------------------------------------------------------------

recommendation_request.user_id IS NOT NULL


------------------------------------------------------------
USER_ID MAPPING
------------------------------------------------------------

recommendation_request.user_id
    → gl_user.id
    → gl_user.username
    → users.user_name
    → users.uuid
    → credentials_recommendation_letters.user_id


------------------------------------------------------------
INSTITUTION_ID MAPPING
------------------------------------------------------------

users.uuid
    → user_institution.user_uuid
    → user_institution.institution_uuid
    → credentials_recommendation_letters.institution_id


------------------------------------------------------------
ENROLLMENT_CODE MAPPING
------------------------------------------------------------

recommendation_request.user_id
    → user_enrollments.student_number
    → user_enrollments.enrollment_code
    → credentials_recommendation_letters.enrollment_code


------------------------------------------------------------
DATE MAPPING
------------------------------------------------------------

recommendation_request.date_of_request
    → credentials_recommendation_letters.created_at

recommendation_request.date_of_request
    → credentials_recommendation_letters.updated_at


------------------------------------------------------------
RECOMMENDER FULL NAME MAPPING
------------------------------------------------------------

recommendation_request.first_name
+
recommendation_request.last_name
    → credentials_recommendation_letters.recommender_full_name


------------------------------------------------------------
credentials_recommendation_letters COLUMN MAPPING
------------------------------------------------------------

UUID v4
    → uuid

recommendation_request.date_of_request
    → created_at

recommendation_request.date_of_request
    → updated_at

NULL
    → deleted_at

first_name + last_name
    → recommender_full_name

1
    → is_confidential

"map_due_date_to_recommender"
    → due_date_to_recommender

1
    → recommendation_type

1
    → recommendation_input_type

recommendation_request.request_status
    → recommendation_input

recommendation_request.personalized_message
    → message_to_recommender

NULL
    → supporting_materials_path

NULL
    → supporting_materials_file_name

NULL
    → upload_letter

NULL
    → upload_letter_file_name

NULL
    → description

mapped users.uuid
    → user_id

mapped institution_uuid
    → institution_id

recommendation_request.recommender_email
    → recommender_email

4
    → credential_type

3
    → status

NULL
    → credential_path

"map_defsult_crated_at"
    → created_by

"map_defsult_updated_at"
    → updated_by

NULL
    → deleted_by

mapped enrollment_code
    → enrollment_code

NULL
    → generated_on


------------------------------------------------------------
credentials_all COLUMN MAPPING
------------------------------------------------------------

UUID v4
    → uuid

recommendation_request.date_of_request
    → created_at

recommendation_request.date_of_request
    → updated_at

NULL
    → deleted_at

mapped users.uuid
    → user_id

mapped institution_uuid
    → institution_id

1
    → is_registered

4
    → credential_type

3
    → status

credentials_recommendation_letters.uuid
    → recommendation_letters

NULL
    → credential_path

mapped enrollment_code
    → enrollment_code

"map_defsult_crated_at"
    → created_by

"map_defsult_updated_at"
    → updated_by

NULL
    → deleted_by

recommendation_request.date_of_request
    → issued_on

NULL
    → generated_on


------------------------------------------------------------
DEFAULT VALUES
------------------------------------------------------------

credentials_recommendation_letters.is_confidential = 1

credentials_recommendation_letters.due_date_to_recommender =
"map_due_date_to_recommender"

credentials_recommendation_letters.recommendation_type = 1

credentials_recommendation_letters.recommendation_input_type = 1

credentials_recommendation_letters.credential_type = 4

credentials_recommendation_letters.status = 3

credentials_recommendation_letters.created_by =
"map_defsult_crated_at"

credentials_recommendation_letters.updated_by =
"map_defsult_updated_at"

credentials_recommendation_letters.supporting_materials_path = NULL

credentials_recommendation_letters.supporting_materials_file_name = NULL

credentials_recommendation_letters.upload_letter = NULL

credentials_recommendation_letters.upload_letter_file_name = NULL

credentials_recommendation_letters.description = NULL

credentials_recommendation_letters.credential_path = NULL

credentials_recommendation_letters.generated_on = NULL

credentials_all.is_registered = 1

credentials_all.credential_type = 4

credentials_all.status = 3

credentials_all.credential_path = NULL

credentials_all.generated_on = NULL


------------------------------------------------------------
LOOKUP TABLES USED
------------------------------------------------------------

gl_user
    → source username lookup

users
    → destination uuid lookup

user_institution
    → institution_uuid lookup

user_enrollments
    → enrollment_code lookup


 ======================================================================   
    resume                                                          credentials_resume, credentials_all (already code is there)
    recommendation_letter,recommendation_letter_request             credentials_recommendation_letter. credentials_all(need to write a code)




Shared credential destination URL mapping:

Base S3 URL:
`https://greenlightlocker-com.s3.us-west-2.amazonaws.com`

| Type | Credential | Source | Source table | ID used | Destination S3 URL | Verify URL |
|---|---|---|---|---|---|---|
| HS | High School Transcript | share | hs_transcript_shared | hs_transcript_shared.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/highschoolshare/{id}/pdf_transcript` | `/verify?type=HS&trackId={track_id}` |
| CTC | Community College Transcript | share | cc_transcript_shared | cc_transcript_shared.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/communitycollegeshare/{id}/pdf_transcript` | `/verify?type=CTC&trackId={track_id}` |
| HE | Four-Year College Transcript | share | 4yr_transcript_shared | 4yr_transcript_shared.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/fouryear-share/{id}/pdf_transcript` | `/verify?type=HE&trackId={track_id}` |
| B | Badge | share | badge_shared | badge.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/badges/{badge_id}/pdf_badge` | `/verify?type=B&trackId={track_id}` |
| CC | Transcript (General) | share | transcript_shared | transcript_shared.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/shared/{id}/pdf_transcript` | `/verify?type=CC&trackId={track_id}` |
| C | Certificate | share | certificate_share | certificate.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/certificate/{certificate_id}/certificate_data` | `/verify?type=C&trackId={track_id}` |
| RL | Recommendation Letter | share | recommendation_letter_share | recommendation_letter.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/recommendationletter/{id}/file` | `/verify?type=RL&trackId={track_id}` |
| R | Resume | share | resume_share | resume.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/resume/{resume_id}/resume_data` | `/verify?type=R&trackId={track_id}` |
| AP | Accomplishment Portfolio | share | accomplishment_portfolio_share | accomplishment_portfolio.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/accomplishmentShare/{id}/pdf_accomplishment` | `/verify?type=AP&trackId={track_id}` |
| S | SAR Report | share | sar_report_share | credential.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/sar/{credential_id}/sar_pdf` | `/verify?type=S&trackId={track_id}` |
| OD | Other Credentials | share | other_credential_share | other_credential.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/other_credential/{id}/credential_data` | `/verify?type=OD&trackId={track_id}` |
| AHS | Asia Highschool | share | credential_shared | credential_shared.id | `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/credentialshare/{id}/pdf_transcript` | `/verify?type=AHS&trackId={track_id}` |

Special case formats:

SPEEDE / EDI formats:
- High School SPEEDE path: `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/speede/{share.id}/{student.firstName}{student.lastName}Edi.edi`
- Four-Year College SPEEDE path: `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/speede/{share.id}/{student.firstName}{student.lastName}Edi.edi`
- White spaces and single quotes are stripped from the student's name in EDI file names.

NSC receiver formats:
- NSC S3 path: `https://greenlightlocker-com.s3.us-west-2.amazonaws.com/nsc/TRANSCRIPT_TO-{nscReceiverId}_{YYYYMMDD}_GreenLight_{student.lastName}.pdf`
- Applicable types: HS, CTC, HE
