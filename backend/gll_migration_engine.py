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

    HSCourseInformationMigrator

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
