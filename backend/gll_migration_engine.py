import logging

from sqlalchemy import (
    create_engine,
    MetaData
)

from sqlalchemy.engine import URL

from s3_service import S3StorageService

from migrators import (

    InstitutionMigrator,

    UsersMigrator,

    UserRoleMigrator,

    UserProfileMigrator,

    ParentStudentMigrator,

    PasswordMigrator,

    PasswordExpirationMigrator,

    UserInstitutionMigrator,

    UserEnrollmentMigrator,

    GLStudentMigrator,

    DigitalBadgesMigrator,
    
    ResumeMigrator,
    
    RecommendationLetterMigrator,

    SelfUploadMigrator,

    TranscriptMigrator,

    CertificateMigrator,

    CredentialsSharedMigrator,

    RegistrarsMigrator,

    PreferencesMigrator,

    FerpaMigrator,

    NsapiPreferencesMigrator,

    CredentialVisibilityMigrator,

    ImportStudentsMigrator,

    ImportParentsMigrator,

    HSOtherCredsMigrator,

    HSClassRankGPAMigrator,

    HSCourseInformationMigrator,

    TranscriptExtMigrator,

    HSTestAssessmentMigrator,

    HSStudentGraduationProfileMigrator,

    HSAwardingCreditMigrator,

    HSCreditSummaryMigrator,

    CCDegreeAwardedMigrator,

    CCCoursesMigrator,

    CCTermMigrator,

    CovidVaccineMigrator,

    HoldsMigrator,

    UserCampusMigrator,

    EmploymentMigrator,

    ScholarshipActivityMigrator,

    InstitutionSftpCredentialsMigrator

)

# -----------------------------------------
# Logging
# -----------------------------------------

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# -----------------------------------------
# GLL Migration Engine
# -----------------------------------------


class GLLMigrationEngine:

    def __init__(self, config: dict):

        # -----------------------------------------
        # Config
        # -----------------------------------------

        self.config = config

        # -----------------------------------------
        # Source DB
        # -----------------------------------------

        logger.info(
            "Creating source DB engine"
        )

        self.source_engine = self._get_engine(
            self.config['source_db']
        )

        logger.info(
            "Source DB engine created"
        )

        # -----------------------------------------
        # Destination DB
        # -----------------------------------------

        logger.info(
            "Creating destination DB engine"
        )

        self.dest_engine = self._get_engine(
            self.config['destination_db']
        )

        logger.info(
            "Destination DB engine created"
        )

        # -----------------------------------------
        # Metadata
        # -----------------------------------------

        self.metadata_source = MetaData()

        self.metadata_dest = MetaData()

        # -----------------------------------------
        # ID Mapping
        # -----------------------------------------

        self.id_map = {}

        # -----------------------------------------
        # Storage
        # -----------------------------------------

        self.storage = S3StorageService()

        # -----------------------------------------
        # Lookup DB Engines
        # -----------------------------------------

        self.lookup_engines = {}

        lookup_databases = self.config.get(
            "lookup_databases",
            []
        )

        logger.info(
            f"Received lookup DB configs: "
            f"{lookup_databases}"
        )

        for db in lookup_databases:

            try:

                logger.info(
                    f"Creating lookup DB engine: "
                    f"{db['name']}"
                )

                self.lookup_engines[
                    db['name']
                ] = self._get_engine(db)

                logger.info(
                    f"Successfully created "
                    f"lookup engine for "
                    f"{db['name']}"
                )

            except Exception as e:

                logger.exception(
                    f"Failed creating lookup "
                    f"engine for "
                    f"{db['name']}: {e}"
                )

    # -----------------------------------------
    # Create Engine
    # -----------------------------------------

    def _get_engine(self, db_config: dict):

        logger.info(
            f"Creating SQLAlchemy engine "
            f"for DB config: "
            f"{db_config}"
        )

        # -----------------------------------------
        # Direct URL
        # -----------------------------------------

        if 'url' in db_config:

            logger.info(
                "Using direct DB URL"
            )

            return create_engine(
                db_config['url']
            )

        db_type = db_config.get(
            'type',
            'mysql'
        )

        # -----------------------------------------
        # SQLite
        # -----------------------------------------

        if db_type == 'sqlite':

            logger.info(
                "Creating SQLite engine"
            )

            return create_engine(

                URL.create(
                    "sqlite",
                    database=db_config['database']
                )
            )

        # -----------------------------------------
        # PostgreSQL / MySQL
        # -----------------------------------------

        drivername = (

            "postgresql"

            if db_type == "postgresql"

            else "mysql+pymysql"
        )

        default_port = (

            5432

            if db_type == "postgresql"

            else 3306
        )

        url = URL.create(

            drivername=drivername,

            username=db_config.get(
                'username'
            ),

            password=db_config.get(
                'password'
            ),

            host=db_config.get(
                'host',
                '127.0.0.1'
            ),

            port=db_config.get(
                'port'
            ) or default_port,

            database=db_config.get(
                'database'
            )
        )

        logger.info(
            f"Engine URL created for "
            f"database: "
            f"{db_config.get('database')}"
        )

        return create_engine(url)

    # -----------------------------------------
    # Main Migration
    # -----------------------------------------

    def migrate(self):

        logger.info(
            "====================================="
        )

        logger.info(
            "Starting GLL Migration..."
        )

        logger.info(
            "====================================="
        )

        logger.info(
            f"Available lookup engines: "
            f"{list(self.lookup_engines.keys())}"
        )

        total_migrated = 0

        mappings = self.config.get(
            "mappings",
            []
        )

        logger.info(
            f"Received mappings: "
            f"{mappings}"
        )

        migrators = []

        # -----------------------------------------
        # gl_user USERS MIGRATION
        # -----------------------------------------

        user_selected = any(

            (
                m.get("source_table")
                == "gl_user"
            )

            and

            (
                m.get("destination_table")
                == "users"
            )

            for m in mappings
        )

        # -----------------------------------------
        # USER ROLE MIGRATION
        # -----------------------------------------

        user_role_selected = any(

            (
                m.get("source_table")
                == "gl_user"
            )

            and

            (
                m.get("destination_table")
                == "user_role"
            )

            for m in mappings
        )

        # -----------------------------------------
        # gl_user USER PROFILE MIGRATION
        # -----------------------------------------

        user_profile_selected = any(

            (
                m.get("source_table")
                == "gl_user"
            )

            and

            (
                m.get("destination_table")
                == "user_profile"
            )

            for m in mappings
        )

        # -----------------------------------------
        # gl_student MIGRATION
        # -----------------------------------------

        gl_student_selected = any(

            (
                m.get("source_table")
                == "gl_student"
            )

            and

            (
                m.get("destination_table")
                in [

                    "users",

                    "user_profile",

                    "user_role"
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # PASSWORD MIGRATION
        # -----------------------------------------

        password_selected = any(

            m.get("destination_table") in [

                "password",

                "user_hashed_password"
            ]

            for m in mappings
        )

        # -----------------------------------------
        # PASSWORD EXPIRATION MIGRATION
        # -----------------------------------------

        password_expiration_selected = any(

            m.get("destination_table")
            == "user_hashed_password_expires_at"

            for m in mappings
        )

        # -----------------------------------------
        # INSTITUTION MIGRATION
        # -----------------------------------------

        institution_selected = any(

            (
                m.get("source_table")
                == "institution"
            )

            or

            (
                m.get("destination_table")
                == "institutions"
            )

            or

            (
                m.get("destination_table")
                == "institution_campuses"
            )

            for m in mappings
        )

        # -----------------------------------------
        # USER INSTITUTION MIGRATION
        # -----------------------------------------

        user_institution_selected = any(

            m.get("destination_table")
            == "user_institution"

            for m in mappings
        )

        # -----------------------------------------
        # PARENT STUDENT MIGRATION
        # -----------------------------------------

        parent_student_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "gl_parent_student"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "parent_student"
            )

            for m in mappings
        )

        # -----------------------------------------
        # USER ENROLLMENT MIGRATION
        # -----------------------------------------

        user_enrollment_selected = any(

            m.get("destination_table")
            == "user_enrollments"

            for m in mappings
        )

        # -----------------------------------------
        # DIGITAL BADGES MIGRATION
        # -----------------------------------------

        digital_badges_selected = any(

            (
                m.get("source_table")
                == "badge"
            )

            and

            (
                m.get("destination_table")
                == "credentials_digital_badges"
            )

            for m in mappings
        )
        
        
                # -----------------------------------------
        # RESUME MIGRATION
        # -----------------------------------------

        resume_selected = any(

            (
                m.get("source_table")
                == "resume"
            )

            and

            (
                m.get("destination_table")
                == "credentials_resume"
            )

            for m in mappings
        )

        # -----------------------------------------
        # SELF UPLOAD MIGRATION
        # -----------------------------------------

        self_upload_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "other_credentials"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "credentials_self_uploads",
                    "self_uploads",
                    "self-uploads",
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # TRANSCRIPT MIGRATION
        # -----------------------------------------

        transcript_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "transcript"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "credentials_transcripts",
                    "credentials_transcript",
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # CERTIFICATE MIGRATION
        # -----------------------------------------

        certificate_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "certificate"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "credentials_certifications",
                    "credentials_cerifications",
                    "credentials_cerificate",
                    "credentials_certificate"
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # RECOMMENDATION LETTER MIGRATION
        # -----------------------------------------

        recommendation_letter_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in [
                    "recommendation_request",
                    "recommendation_letter"
                ]
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "credentials_recommendation_letters"
            )

            for m in mappings
        )

        # -----------------------------------------
        # CREDENTIALS SHARED MIGRATION
        # -----------------------------------------

        credentials_shared_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in [
                    "badge_shared",
                    "certificate_share",
                    "certificate_shared",
                    "other_credential_share",
                    "recommendation_letter_share",
                    "self_uploaded_transcript_share",
                    "transcript_shared",
                    "hs_transcript_shared",
                    "cc_transcript_shared",
                    "4yr_transcript_shared",
                    "resume_share"
                ]
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "credentials_shared",
                    "student_credentials_share_history",
                    "students_credentials_share_history"
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # REGISTRARS MIGRATION
        # -----------------------------------------

        registrars_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in [
                    "institution_registrat",
                    "institution_registrar",
                    "institution_registrars"
                ]
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "registrars"
            )

            for m in mappings
        )

        # -----------------------------------------
        # PREFERENCES MIGRATION
        # -----------------------------------------

        preferences_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "student_preference"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "my_preferences"
            )

            for m in mappings
        )

        # -----------------------------------------
        # FERPA MIGRATION
        # -----------------------------------------

        ferpa_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "ferpa"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "ferpa"
            )

            for m in mappings
        )

        # -----------------------------------------
        # NSAPI PREFERENCES MIGRATION
        # -----------------------------------------

        nsapi_preferences_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "nsapi_criteria"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "scholarship_prefernces"
            )

            for m in mappings
        )

        # -----------------------------------------
        # CREDENTIAL VISIBILITY / MODULE PERMISSIONS
        # -----------------------------------------

        credential_visibility_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in [
                    "student_credential_visibility",
                    "student_crdential_visibility"
                ]
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "module_permissions",
                    "permissions"
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # GL_STUDENT -> IMPORT_STUDENTS MIGRATION
        # -----------------------------------------

        import_students_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "gl_student"
            )

            and

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_students"
            )

            for m in mappings
        )

        # -----------------------------------------
        # GL_PARENT -> IMPORT_PARENTS MIGRATION
        # -----------------------------------------

        import_parents_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "gl_parent"
            )

            and

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "import_parent",
                    "import_parents"
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS OTHER CREDS IMPORT MIGRATION
        # -----------------------------------------

        hs_other_creds_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in [
                    "hs_other_creds",
                    "hs_othser_creds"
                ]
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in [
                    "import_apibs",
                    "import_biliteracies",
                    "import_cert_lics",
                    "import_dual_credits",
                    "import_college_assessments",
                    "import_college_aasesments",
                    "import_other_requirements"
                ]
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS CLASS RANK GPA IMPORT MIGRATION
        # -----------------------------------------

        hs_class_rank_gpa_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "hs_class_rank_gpa"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_class_rank_gpa"
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS COURSE INFORMATION IMPORT MIGRATION
        # -----------------------------------------

        hs_course_information_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "hs_course_information"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_course_information"
            )

            for m in mappings
        )

        # -----------------------------------------
        # TRANSCRIPT EXT IMPORT MIGRATION
        # -----------------------------------------

        transcript_ext_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "transcript_ext"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_edi_transcript_ext"
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS TEST ASSESSMENT IMPORT MIGRATION
        # -----------------------------------------

        hs_test_assessment_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "hs_test_assessment"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_student_test_assessments"
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS STUDENT GRADUATION PROFILE IMPORT MIGRATION
        # -----------------------------------------

        hs_student_graduation_profile_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "hs_student_graduation_profile"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_student_graduation_profile"
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS AWARDING CREDIT IMPORT MIGRATION
        # -----------------------------------------

        hs_awarding_credit_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "hs_awarding_credit"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_schools_awarding_credits"
            )

            for m in mappings
        )

        # -----------------------------------------
        # HS CREDIT SUMMARY IMPORT MIGRATION
        # -----------------------------------------

        hs_credit_summary_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "hs_transcript"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_credit_summary"
            )

            for m in mappings
        )

        # -----------------------------------------
        # CC DEGREE AWARDED IMPORT MIGRATION
        # -----------------------------------------

        cc_degree_awarded_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in (
                    "cc_degree_awarded",
                    "cc_degree_awaeded",
                )
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "import_edi_award"
            )

            for m in mappings
        )

        # -----------------------------------------
        # CC COURSES IMPORT MIGRATION
        # -----------------------------------------

        cc_courses_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in (
                    "cc_courses",
                    "cc_course",
                )
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in (
                    "import_edi_courses",
                    "import_edi_course",
                )
            )

            for m in mappings
        )

        # -----------------------------------------
        # CC TERM IMPORT MIGRATION
        # -----------------------------------------

        cc_term_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "cc_term"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                in (
                    "import_edi_semester",
                    "import_edi_semesters",
                )
            )

            for m in mappings
        )

        # -----------------------------------------
        # COVID VACCINE IMPORT MIGRATION
        # -----------------------------------------

        covid_vaccine_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "covid_vaccine_meta_data"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "vaccination_certificate_data"
            )

            for m in mappings
        )

        # -----------------------------------------
        # HOLDS MIGRATION
        # -----------------------------------------

        holds_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "inst_holds_ext"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "holds"
            )

            for m in mappings
        )

        # -----------------------------------------
        # USER CAMPUS MIGRATION
        # -----------------------------------------

        user_campus_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "institution_user"
                and
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "user_campus"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "user_campus"
            )

            for m in mappings
        )

        # -----------------------------------------
        # EMPLOYMENT MIGRATION
        # -----------------------------------------

        employment_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "employment_history"
                and
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "employment"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "employment"
            )

            for m in mappings
        )

        # -----------------------------------------
        # SCHOLARSHIP ACTIVITY MIGRATION
        # -----------------------------------------

        scholarship_activity_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                == "scholarship_activity"
                and
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "scholarship_user_activity"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "scholarship_user_activity"
            )

            for m in mappings
        )

        # -----------------------------------------
        # INSTITUTION SFTP CREDENTIALS MIGRATION
        # -----------------------------------------

        institution_sftp_credentials_selected = any(

            (
                str(
                    m.get("source_table") or ""
                ).strip().lower()
                in [
                    "esc_sftp_user",
                    "esc_sftp_user_public_key"
                ]
                and
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "institution_sftp_credentials"
            )

            or

            (
                str(
                    m.get("destination_table") or ""
                ).strip().lower()
                == "institution_sftp_credentials"
            )

            for m in mappings
        )

        # -----------------------------------------
        # Logs
        # -----------------------------------------

        logger.info(
            f"user_selected="
            f"{user_selected}"
        )

        logger.info(
            f"user_profile_selected="
            f"{user_profile_selected}"
        )

        logger.info(
            f"user_role_selected="
            f"{user_role_selected}"
        )

        logger.info(
            f"gl_student_selected="
            f"{gl_student_selected}"
        )

        logger.info(
            f"password_selected="
            f"{password_selected}"
        )

        logger.info(
            f"institution_selected="
            f"{institution_selected}"
        )

        logger.info(
            f"user_institution_selected="
            f"{user_institution_selected}"
        )

        logger.info(
            f"parent_student_selected="
            f"{parent_student_selected}"
        )

        logger.info(
            f"user_enrollment_selected="
            f"{user_enrollment_selected}"
        )

        logger.info(
            f"digital_badges_selected="
            f"{digital_badges_selected}"
        )
        
        logger.info(
            f"resume_selected="
            f"{resume_selected}"
        )

        logger.info(
            f"self_upload_selected="
            f"{self_upload_selected}"
        )

        logger.info(
            f"transcript_selected="
            f"{transcript_selected}"
        )

        logger.info(
            f"certificate_selected="
            f"{certificate_selected}"
        )
        
        logger.info(
            f"recommendation_letter_selected="
            f"{recommendation_letter_selected}"
        )

        logger.info(
            f"credentials_shared_selected="
            f"{credentials_shared_selected}"
        )

        logger.info(
            f"registrars_selected="
            f"{registrars_selected}"
        )

        logger.info(
            f"preferences_selected="
            f"{preferences_selected}"
        )

        logger.info(
            f"ferpa_selected="
            f"{ferpa_selected}"
        )

        logger.info(
            f"nsapi_preferences_selected="
            f"{nsapi_preferences_selected}"
        )

        logger.info(
            f"credential_visibility_selected="
            f"{credential_visibility_selected}"
        )

        logger.info(
            f"import_students_selected="
            f"{import_students_selected}"
        )

        logger.info(
            f"import_parents_selected="
            f"{import_parents_selected}"
        )

        logger.info(
            f"hs_other_creds_selected="
            f"{hs_other_creds_selected}"
        )

        logger.info(
            f"hs_class_rank_gpa_selected="
            f"{hs_class_rank_gpa_selected}"
        )

        logger.info(
            f"hs_course_information_selected="
            f"{hs_course_information_selected}"
        )

        logger.info(
            f"transcript_ext_selected="
            f"{transcript_ext_selected}"
        )

        logger.info(
            f"hs_test_assessment_selected="
            f"{hs_test_assessment_selected}"
        )

        logger.info(
            f"hs_student_graduation_profile_selected="
            f"{hs_student_graduation_profile_selected}"
        )

        logger.info(
            f"hs_awarding_credit_selected="
            f"{hs_awarding_credit_selected}"
        )

        logger.info(
            f"hs_credit_summary_selected="
            f"{hs_credit_summary_selected}"
        )

        logger.info(
            f"cc_degree_awarded_selected="
            f"{cc_degree_awarded_selected}"
        )

        logger.info(
            f"cc_courses_selected="
            f"{cc_courses_selected}"
        )

        logger.info(
            f"cc_term_selected="
            f"{cc_term_selected}"
        )

        logger.info(
            f"covid_vaccine_selected="
            f"{covid_vaccine_selected}"
        )

        logger.info(
            f"holds_selected="
            f"{holds_selected}"
        )

        logger.info(
            f"user_campus_selected="
            f"{user_campus_selected}"
        )

        logger.info(
            f"employment_selected="
            f"{employment_selected}"
        )

        logger.info(
            f"scholarship_activity_selected="
            f"{scholarship_activity_selected}"
        )

        logger.info(
            f"institution_sftp_credentials_selected="
            f"{institution_sftp_credentials_selected}"
        )

        logger.info(
            f"password_expiration_selected="
            f"{password_expiration_selected}"
        )

        # -----------------------------------------
        # Add UsersMigrator
        # -----------------------------------------

        if user_selected:

            logger.info(
                "Adding UsersMigrator"
            )

            migrators.append(

                UsersMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add UserRoleMigrator
        # -----------------------------------------

        if user_role_selected:

            logger.info(
                "Adding UserRoleMigrator"
            )

            migrators.append(

                UserRoleMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add UserProfileMigrator
        # -----------------------------------------

        if user_profile_selected:

            logger.info(
                "Adding UserProfileMigrator"
            )

            migrators.append(

                UserProfileMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add GLStudentMigrator
        # -----------------------------------------

        if gl_student_selected:

            logger.info(
                "Adding GLStudentMigrator"
            )

            migrators.append(

                GLStudentMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add PasswordMigrator
        # -----------------------------------------

        if password_selected:

            logger.info(
                "Adding PasswordMigrator"
            )

            migrators.append(

                PasswordMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add PasswordExpirationMigrator
        # -----------------------------------------

        if password_expiration_selected:

            logger.info(
                "Adding PasswordExpirationMigrator"
            )

            migrators.append(

                PasswordExpirationMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add InstitutionMigrator
        # -----------------------------------------

        if institution_selected:

            logger.info(
                "Adding InstitutionMigrator"
            )

            migrators.append(

                InstitutionMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add UserInstitutionMigrator
        # -----------------------------------------

        if user_institution_selected:

            logger.info(
                "Adding UserInstitutionMigrator"
            )

            migrators.append(

                UserInstitutionMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add ParentStudentMigrator
        # -----------------------------------------

        if parent_student_selected:

            logger.info(
                "Adding ParentStudentMigrator"
            )

            migrators.append(

                ParentStudentMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add UserEnrollmentMigrator
        # -----------------------------------------

        if user_enrollment_selected:

            logger.info(
                "Adding UserEnrollmentMigrator"
            )

            migrators.append(

                UserEnrollmentMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add DigitalBadgesMigrator
        # -----------------------------------------

        if digital_badges_selected:

            logger.info(
                "Adding DigitalBadgesMigrator"
            )

            migrators.append(

                DigitalBadgesMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )
            
            # -----------------------------------------
            # Add ResumeMigrator
            # -----------------------------------------

        if resume_selected:

                logger.info(
                    "Adding ResumeMigrator"
                )

                migrators.append(

                    ResumeMigrator(

                        self,

                        self.source_engine,

                        self.dest_engine,

                        self.storage,

                        self.config
                    )
                )

        # -----------------------------------------
        # Add SelfUploadMigrator
        # -----------------------------------------

        if self_upload_selected:

            logger.info(
                "Adding SelfUploadMigrator"
            )

            migrators.append(

                SelfUploadMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add TranscriptMigrator
        # -----------------------------------------

        if transcript_selected:

            logger.info(
                "Adding TranscriptMigrator"
            )

            migrators.append(

                TranscriptMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CertificateMigrator
        # -----------------------------------------

        if certificate_selected:

            logger.info(
                "Adding CertificateMigrator"
            )

            migrators.append(

                CertificateMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add RecommendationLetterMigrator
        # -----------------------------------------

        if recommendation_letter_selected:

            logger.info(
                "Adding RecommendationLetterMigrator"
            )

            migrators.append(

                RecommendationLetterMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CredentialsSharedMigrator
        # -----------------------------------------

        if credentials_shared_selected:

            logger.info(
                "Adding CredentialsSharedMigrator"
            )

            migrators.append(

                CredentialsSharedMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add RegistrarsMigrator
        # -----------------------------------------

        if registrars_selected:

            logger.info(
                "Adding RegistrarsMigrator"
            )

            migrators.append(

                RegistrarsMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add PreferencesMigrator
        # -----------------------------------------

        if preferences_selected:

            logger.info(
                "Adding PreferencesMigrator"
            )

            migrators.append(

                PreferencesMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add FerpaMigrator
        # -----------------------------------------

        if ferpa_selected:

            logger.info(
                "Adding FerpaMigrator"
            )

            migrators.append(

                FerpaMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add NsapiPreferencesMigrator
        # -----------------------------------------

        if nsapi_preferences_selected:

            logger.info(
                "Adding NsapiPreferencesMigrator"
            )

            migrators.append(

                NsapiPreferencesMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CredentialVisibilityMigrator
        # -----------------------------------------

        if credential_visibility_selected:

            logger.info(
                "Adding CredentialVisibilityMigrator"
            )

            migrators.append(

                CredentialVisibilityMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add ImportStudentsMigrator
        # -----------------------------------------

        if import_students_selected:

            logger.info(
                "Adding ImportStudentsMigrator"
            )

            migrators.append(

                ImportStudentsMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add ImportParentsMigrator
        # -----------------------------------------

        if import_parents_selected:

            logger.info(
                "Adding ImportParentsMigrator"
            )

            migrators.append(

                ImportParentsMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSOtherCredsMigrator
        # -----------------------------------------

        if hs_other_creds_selected:

            logger.info(
                "Adding HSOtherCredsMigrator"
            )

            migrators.append(

                HSOtherCredsMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSClassRankGPAMigrator
        # -----------------------------------------

        if hs_class_rank_gpa_selected:

            logger.info(
                "Adding HSClassRankGPAMigrator"
            )

            migrators.append(

                HSClassRankGPAMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSCourseInformationMigrator
        # -----------------------------------------

        if hs_course_information_selected:

            logger.info(
                "Adding HSCourseInformationMigrator"
            )

            migrators.append(

                HSCourseInformationMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add TranscriptExtMigrator
        # -----------------------------------------

        if transcript_ext_selected:

            logger.info(
                "Adding TranscriptExtMigrator"
            )

            migrators.append(

                TranscriptExtMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSTestAssessmentMigrator
        # -----------------------------------------

        if hs_test_assessment_selected:

            logger.info(
                "Adding HSTestAssessmentMigrator"
            )

            migrators.append(

                HSTestAssessmentMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSStudentGraduationProfileMigrator
        # -----------------------------------------

        if hs_student_graduation_profile_selected:

            logger.info(
                "Adding HSStudentGraduationProfileMigrator"
            )

            migrators.append(

                HSStudentGraduationProfileMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSAwardingCreditMigrator
        # -----------------------------------------

        if hs_awarding_credit_selected:

            logger.info(
                "Adding HSAwardingCreditMigrator"
            )

            migrators.append(

                HSAwardingCreditMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HSCreditSummaryMigrator
        # -----------------------------------------

        if hs_credit_summary_selected:

            logger.info(
                "Adding HSCreditSummaryMigrator"
            )

            migrators.append(

                HSCreditSummaryMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CCDegreeAwardedMigrator
        # -----------------------------------------

        if cc_degree_awarded_selected:

            logger.info(
                "Adding CCDegreeAwardedMigrator"
            )

            migrators.append(

                CCDegreeAwardedMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CCCoursesMigrator
        # -----------------------------------------

        if cc_courses_selected:

            logger.info(
                "Adding CCCoursesMigrator"
            )

            migrators.append(

                CCCoursesMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CCTermMigrator
        # -----------------------------------------

        if cc_term_selected:

            logger.info(
                "Adding CCTermMigrator"
            )

            migrators.append(

                CCTermMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add CovidVaccineMigrator
        # -----------------------------------------

        if covid_vaccine_selected:

            logger.info(
                "Adding CovidVaccineMigrator"
            )

            migrators.append(

                CovidVaccineMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add HoldsMigrator
        # -----------------------------------------

        if holds_selected:

            logger.info(
                "Adding HoldsMigrator"
            )

            migrators.append(

                HoldsMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add UserCampusMigrator
        # -----------------------------------------

        if user_campus_selected:

            logger.info(
                "Adding UserCampusMigrator"
            )

            migrators.append(

                UserCampusMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add EmploymentMigrator
        # -----------------------------------------

        if employment_selected:

            logger.info(
                "Adding EmploymentMigrator"
            )

            migrators.append(

                EmploymentMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add ScholarshipActivityMigrator
        # -----------------------------------------

        if scholarship_activity_selected:

            logger.info(
                "Adding ScholarshipActivityMigrator"
            )

            migrators.append(

                ScholarshipActivityMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # Add InstitutionSftpCredentialsMigrator
        # -----------------------------------------

        if institution_sftp_credentials_selected:

            logger.info(
                "Adding InstitutionSftpCredentialsMigrator"
            )

            migrators.append(

                InstitutionSftpCredentialsMigrator(

                    self,

                    self.source_engine,

                    self.dest_engine,

                    self.storage,

                    self.config
                )
            )

        # -----------------------------------------
        # No Migrators
        # -----------------------------------------

        if not migrators:

            logger.warning(
                "No migrators selected."
            )

            return 0

        # -----------------------------------------
        # Execute Migrators
        # -----------------------------------------

        for migrator in migrators:

            logger.info(
                f"====================================="
            )

            logger.info(
                f"Running "
                f"{migrator.__class__.__name__}"
            )

            logger.info(
                f"Starting execution of "
                f"{migrator.__class__.__name__}"
            )

            migrated_count = migrator.migrate()

            logger.info(
                f"Completed execution of "
                f"{migrator.__class__.__name__}"
            )

            logger.info(
                f"{migrator.__class__.__name__} "
                f"migrated "
                f"{migrated_count} records."
            )

            logger.info(
                f"====================================="
            )

            total_migrated += migrated_count

        # -----------------------------------------
        # Final Log
        # -----------------------------------------

        logger.info(
            "====================================="
        )

        logger.info(
            f"Total migrated records: "
            f"{total_migrated}"
        )

        logger.info(
            "====================================="
        )

        return total_migrated
