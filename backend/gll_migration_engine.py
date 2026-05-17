import logging
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import URL

from s3_service import S3StorageService

from migrators import (
    InstitutionMigrator,
    UsersMigrator,
    PasswordMigrator,
    UserInstitutionMigrator,
    UserEnrollmentMigrator,
    GLStudentMigrator,
    
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

        self.config = config

        # -----------------------------------------
        # Source DB
        # -----------------------------------------

        self.source_engine = self._get_engine(
            self.config['source_db']
        )

        # -----------------------------------------
        # Destination DB
        # -----------------------------------------

        self.dest_engine = self._get_engine(
            self.config['destination_db']
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
    # Create Engine
    # -----------------------------------------

    def _get_engine(self, db_config: dict):

        # -----------------------------------------
        # Direct URL
        # -----------------------------------------

        if 'url' in db_config:

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

        # -----------------------------------------
        # Add UsersMigrator
        # gl_user
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
        # gl_student
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
        # MUST RUN AFTER USERS
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
                f"Running "
                f"{migrator.__class__.__name__}"
            )

            migrated_count = migrator.migrate()

            logger.info(
                f"{migrator.__class__.__name__} "
                f"migrated "
                f"{migrated_count} records."
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