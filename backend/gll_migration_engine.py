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

    NsapiPreferencesMigrator

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
        # gl_user MIGRATION
        # -----------------------------------------

        user_selected = any(

            (
                m.get("source_table")
                == "gl_user"
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
                == "credentials_self_uploads"
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
                == "credentials_transcripts"
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
                    "certificate_shared",
                    "other_credential_share",
                    "recommendation_letter_share",
                    "self_uploaded_transcript_share",
                    "transcript_shared",
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
                    "student_credentials_share_history"
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
        # Logs
        # -----------------------------------------

        logger.info(
            f"user_selected="
            f"{user_selected}"
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
