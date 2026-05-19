import uuid
import logging

from sqlalchemy import (
    select,
    insert,
    and_
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class RecommendationLetterMigrator(BaseMigrator):

    def __init__(
        self,
        engine,
        source_engine,
        dest_engine,
        storage,
        config
    ):

        super().__init__(
            engine,
            source_engine,
            dest_engine,
            storage
        )

        self.config = config

    # -------------------------------------------------
    # MAIN MIGRATION
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================"
        )

        logger.info(
            "Starting Recommendation Letter Migration..."
        )

        logger.info(
            "======================================"
        )

        # -------------------------------------------------
        # AUTH LOOKUP DB
        # -------------------------------------------------

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        logger.info(
            "auth_db lookup engine loaded"
        )

        # -------------------------------------------------
        # SOURCE TABLE
        # -------------------------------------------------

        recommendation_table = self._manual_reflect(
            'recommendation_request',
            self.source_engine,
            self.metadata_source
        )

        logger.info(
            f"Recommendation table columns: "
            f"{recommendation_table.columns.keys()}"
        )

        # -------------------------------------------------
        # DESTINATION TABLES
        # -------------------------------------------------

        recommendation_dest_table = self._manual_reflect(
            'credentials_recommendation_letters',
            self.dest_engine,
            self.metadata_dest
        )

        credentials_table = self._manual_reflect(
            'credentials_all',
            self.dest_engine,
            self.metadata_dest
        )

        logger.info(
            f"credentials_all columns: "
            f"{credentials_table.columns.keys()}"
        )

        logger.info(
            "Successfully reflected destination tables"
        )

        # -------------------------------------------------
        # FETCH SOURCE RECORDS
        # -------------------------------------------------

        query = select(
            recommendation_table
        ).where(
            and_(
                recommendation_table.c.user_id == 501
            )
        ).limit(100)

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                query
            ).fetchall()

            if not rows:

                logger.warning(
                    "No recommendation records found"
                )

                return 0

            logger.info(
                f"Found {len(rows)} recommendation records"
            )

            recommendation_insert_data = []

            credentials_insert_data = []

            # -------------------------------------------------
            # PROCESS ROWS
            # -------------------------------------------------

            for index, row in enumerate(
                rows,
                start=1
            ):

                try:

                    logger.info(
                        f"Processing row {index}"
                    )

                    row_dict = row._mapping

                    # -------------------------------------------------
                    # SOURCE USER ID
                    # -------------------------------------------------

                    source_gl_user_id = row_dict.get(
                        recommendation_table.c.user_id
                    )

                    logger.info(
                        f"Source user_id: "
                        f"{source_gl_user_id}"
                    )

                    if not source_gl_user_id:

                        logger.warning(
                            "user_id is null"
                        )

                        continue

                    # -------------------------------------------------
                    # FETCH SOURCE gl_user
                    # -------------------------------------------------

                    source_gl_user = (
                        self.fetch_one_by_column(

                            self.source_engine,

                            "gl_user",

                            "id",

                            source_gl_user_id
                        )
                    )

                    if not source_gl_user:

                        logger.warning(
                            f"No gl_user found for id: "
                            f"{source_gl_user_id}"
                        )

                        continue

                    source_username = (
                        source_gl_user.get(
                            "username"
                        )
                    )

                    logger.info(
                        f"Source username: "
                        f"{source_username}"
                    )

                    if not source_username:

                        logger.warning(
                            "username is null"
                        )

                        continue

                    # -------------------------------------------------
                    # FETCH DESTINATION USER
                    # -------------------------------------------------

                    dest_user = (
                        self.fetch_one_by_column(

                            auth_db_engine,

                            "users",

                            "user_name",

                            source_username
                        )
                    )

                    if not dest_user:

                        logger.warning(
                            f"No destination user found "
                            f"for username: "
                            f"{source_username}"
                        )

                        continue

                    destination_user_uuid = (
                        dest_user.get(
                            "uuid"
                        )
                    )

                    logger.info(
                        f"Destination user UUID: "
                        f"{destination_user_uuid}"
                    )

                    # -------------------------------------------------
                    # FETCH USER INSTITUTION
                    # -------------------------------------------------

                    user_institution = (
                        self.fetch_one_by_column(

                            auth_db_engine,

                            "user_institution",

                            "user_uuid",

                            destination_user_uuid
                        )
                    )

                    if not user_institution:

                        logger.warning(
                            f"No institution mapping found "
                            f"for user UUID: "
                            f"{destination_user_uuid}"
                        )

                        continue

                    institution_uuid = (
                        user_institution.get(
                            "institution_uuid"
                        )
                    )

                    logger.info(
                        f"Institution UUID: "
                        f"{institution_uuid}"
                    )

                    if not institution_uuid:

                        logger.warning(
                            "institution_uuid is null"
                        )

                        continue

                    # -------------------------------------------------
                    # DATE MAPPING
                    # -------------------------------------------------

                    created_at = row_dict.get(
                        recommendation_table.c.date_of_request
                    )

                    logger.info(
                        f"Date of request: "
                        f"{created_at}"
                    )

                    # -------------------------------------------------
                    # FULL NAME
                    # -------------------------------------------------

                    first_name = row_dict.get(
                        recommendation_table.c.first_name
                    ) or ""

                    last_name = row_dict.get(
                        recommendation_table.c.last_name
                    ) or ""

                    recommender_full_name = (
                        f"{first_name} {last_name}"
                    ).strip()

                    logger.info(
                        f"Recommender Full Name: "
                        f"{recommender_full_name}"
                    )

                    # -------------------------------------------------
                    # ENROLLMENT CODE
                    # -------------------------------------------------

                    enrollment_code = (
                        ""
                    )

                    source_enrollment = (
                        self.fetch_one_by_column(

                            auth_db_engine,

                            "user_enrollments",

                            "student_number",

                            str(source_gl_user_id)
                        )
                    )

                    if source_enrollment:

                        fetched_enrollment_code = (
                            source_enrollment.get(
                                "enrollment_code"
                            )
                        )

                        if fetched_enrollment_code:

                            enrollment_code = (
                                fetched_enrollment_code
                            )

                    logger.info(
                        f"Enrollment code: "
                        f"{enrollment_code}"
                    )

                    # -------------------------------------------------
                    # RECOMMENDATION LETTER UUID
                    # -------------------------------------------------

                    recommendation_uuid = str(
                        uuid.uuid4()
                    )

                    # -------------------------------------------------
                    # credentials_recommendation_letters
                    # -------------------------------------------------

                    recommendation_row = {

                        'uuid': recommendation_uuid,

                        'created_at': created_at,

                        'updated_at': created_at,

                        'deleted_at': None,

                        'recommender_full_name': (
                            recommender_full_name
                        ),

                        'is_confidential': 1,

                        'due_date_to_recommender': (
                            ''
                        ),

                        'recommendation_type': 1,

                        'recommendation_input_type': 1,

                        'recommendation_input': (
                            row_dict.get(
                                recommendation_table.c.request_status
                            )
                        ),

                        'message_to_recommender': (
                            row_dict.get(
                                recommendation_table.c.personalized_message
                            )
                        ),

                        'supporting_materials_path': None,

                        'supporting_materials_file_name': None,

                        'upload_letter': None,

                        'upload_letter_file_name': None,

                        'description': None,

                        'user_id': (
                            destination_user_uuid
                        ),

                        'institution_id': (
                            institution_uuid
                        ),

                       'recommender_email': (
                            row_dict.get(
                                recommendation_table.c.recommender_email
                            )

                            or

                            ''
                        ),

                        'credential_type': 4,

                        'status': 3,

                        'credential_path': None,

                        'created_by': (
                            ''
                        ),

                        'updated_by': (
                            ''
                        ),

                        'deleted_by': None,

                        'enrollment_code': (
                            enrollment_code
                        ),

                        'generated_on': None,
                    }

                    logger.info(
                        f"Recommendation row: "
                        f"{recommendation_row}"
                    )

                    recommendation_insert_data.append(
                        recommendation_row
                    )

                    # -------------------------------------------------
                    # credentials_all
                    # -------------------------------------------------

                    credentials_row = {

                        'uuid': str(
                            uuid.uuid4()
                        ),

                        'created_at': created_at,

                        'updated_at': created_at,

                        'deleted_at': None,

                        'user_id': (
                            destination_user_uuid
                        ),

                        'institution_id': (
                            institution_uuid
                        ),

                        'credential_type': 4,

                        'status': 3,

                        'recommendation_letters': (
                            recommendation_uuid
                        ),

                        'credential_path': None,

                        'enrollment_code': (
                            enrollment_code
                        ),

                        'created_by': (
                            ''
                        ),

                        'updated_by': (
                            ''
                        ),

                        'deleted_by': None,

                        'issued_on': str(
                            created_at
                        ),

                        'generated_on': None,
                        
                        'is_registered': 1,
                    }

                    logger.info(
                        f"Credentials row: "
                        f"{credentials_row}"
                    )

                    credentials_insert_data.append(
                        credentials_row
                    )

                except Exception as e:

                    logger.exception(
                        f"Failed processing row "
                        f"{index}: {e}"
                    )

            # -------------------------------------------------
            # INSERT DATA
            # -------------------------------------------------

            if recommendation_insert_data:

                logger.info(
                    f"Prepared "
                    f"{len(recommendation_insert_data)} "
                    f"records for insertion"
                )

                with self.dest_engine.begin() as dest_conn:

                    # -----------------------------------------
                    # INSERT RECOMMENDATION TABLE
                    # -----------------------------------------

                    result = dest_conn.execute(

                        insert(
                            recommendation_dest_table
                        ),

                        recommendation_insert_data
                    )

                    logger.info(
                        f"Inserted recommendation rows: "
                        f"{result.rowcount}"
                    )

                    # -----------------------------------------
                    # INSERT credentials_all
                    # -----------------------------------------

                    credentials_result = dest_conn.execute(

                        insert(credentials_table),

                        credentials_insert_data
                    )

                    logger.info(
                        f"Inserted credentials rows: "
                        f"{credentials_result.rowcount}"
                    )

                logger.info(
                    "======================================"
                )

                logger.info(
                    f"Successfully migrated "
                    f"{len(recommendation_insert_data)} "
                    f"recommendation records"
                )

                logger.info(
                    "======================================"
                )

                return len(
                    recommendation_insert_data
                )

            logger.warning(
                "No valid records available for insertion"
            )

            return 0