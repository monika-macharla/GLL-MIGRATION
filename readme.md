1. gllauthservice:

```
source db:                          destination db

1. insert roles

2. institution                         institutions,campuses(gllauthservicemigration)

3. gl_user                             users,user_role,institution_user(gllauthservicemigration)

4. gl_user                              user_role

5. gl_user                              user_institution

gl_parent_student                       parent_student


6. gl_user                              user_profile(gllauthservicemigration) 

7. jhi_user                            user_hashed_password(gllauthservicemigration)

8. enrollment                          user_enrollments(gllauthservicemigration)


9. institution_registrar              registrars

10. student_preference                 my_preferences

11. ferpa                             ferpa

12. nsapi_criteria                    scholarship_prefernces

13. student_credential_visibility     module_permissions

//user_campus


2. gllreports


17. transcript                           credentials_transcript, credentials_all
18. certificate                          credentials_certifications, credentials_all


13. badge                           credentials_digital_badges,(gllreports)(support gllauthservicemigration)
                                    credentials_digital_badge_info,credentials_all
14. resume                                                          credentials_resume, credentials_all
15. recommendation_letter,recommendation_letter_request             credentials_recommendation_letters. credentials_all

16. other-credentials                    self-uploads, credentials_all
19. badge_shared                         credentials_shared,students_credentials_share_history





















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
