1. gllauthservice:

```
source db:                          destination db

institutions                        institutions,campuses

gl_user                             users,user_role,user_profile

gl_student                          users,user_role,user_profile

institution_user                    user_institution

jhi_user                            user_hashed_password

enrollment                          user_enrollments

badge                               credentials_digital_badges,
                                    credentials_digital_badge_info


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