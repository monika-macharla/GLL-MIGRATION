import uuid
import logging

from sqlalchemy import (
    select,
    insert,
    inspect
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

        recommendation_letter_table = self._manual_reflect(
            'recommendation_letter',
            self.source_engine,
            self.metadata_source
        )

        logger.info(
            f"Recommendation table columns: "
            f"{recommendation_table.columns.keys()}"
        )

        logger.info(
            f"Recommendation letter table columns: "
            f"{recommendation_letter_table.columns.keys()}"
        )

        # -------------------------------------------------
        # DESTINATION TABLES
        # -------------------------------------------------

        recommendation_destination_table_name = (
            self._get_recommendation_destination_table_name()
        )

        recommendation_dest_table = self._manual_reflect(
            recommendation_destination_table_name,
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
        )

        if self.config.get('limit'):

            query = query.limit(
                self.config['limit']
            )

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

                    source_gl_student = (
                        self.fetch_one_by_column(

                            self.source_engine,

                            "gl_student",

                            "user_id",

                            source_gl_user_id
                        )
                    )

                    source_student_id = None

                    student_number = None

                    student_first_name = None

                    student_last_name = None

                    student_date_of_birth = None

                    if source_gl_student:

                        source_student_id = (
                            source_gl_student.get("id")
                        )

                        student_number = (
                            source_gl_student.get("school_student_id")
                            or
                            source_gl_student.get("student_number")
                            or
                            source_student_id
                        )

                        student_first_name = (
                            source_gl_student.get("first_name")
                        )

                        student_last_name = (
                            source_gl_student.get("last_name")
                        )

                        student_date_of_birth = (
                            source_gl_student.get("date_of_birth")
                        )

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

                    institution_row = (
                        self.fetch_one_by_column(

                            auth_db_engine,

                            "institutions",

                            "uuid",

                            institution_uuid
                        )
                    )

                    institution_name = None

                    if institution_row:

                        institution_name = (
                            institution_row.get("name")
                        )

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

                    updated_at = (
                        row_dict.get(
                            recommendation_table.c.date_issued
                        )
                        or
                        row_dict.get(
                            recommendation_table.c.date_of_initial_response
                        )
                        or
                        created_at
                    )

                    due_date_to_recommender = (
                        row_dict.get(
                            recommendation_table.c.issued_by_date
                        )
                    )

                    # -------------------------------------------------
                    # SOURCE LETTER DETAILS
                    # -------------------------------------------------

                    source_request_id = row_dict.get(
                        recommendation_table.c.id
                    )

                    source_letter = (
                        self.fetch_one_by_column(

                            self.source_engine,

                            "recommendation_letter",

                            "reference_id",

                            source_request_id
                        )
                    )

                    issuer_first_name = ""

                    issuer_middle_name = ""

                    issuer_last_name = ""

                    recommender_email = (
                        row_dict.get(
                            recommendation_table.c.recommender_email
                        )
                        or
                        ""
                    )

                    upload_letter = None

                    blockchain_hash = None

                    if source_letter:

                        issuer_first_name = (
                            source_letter.get("issuer_first_name")
                            or
                            ""
                        )

                        issuer_middle_name = (
                            source_letter.get("issuer_middle_name")
                            or
                            ""
                        )

                        issuer_last_name = (
                            source_letter.get("issuer_last_name")
                            or
                            ""
                        )

                        recommender_email = (
                            source_letter.get("issuer_email")
                            or
                            recommender_email
                        )

                        source_letter_id = (
                            source_letter.get("id")
                        )

                        if source_letter_id:

                            upload_letter = (
                                f"recommendationletter/"
                                f"{source_letter_id}/"
                                f"file"
                            )

                        else:

                            upload_letter = (
                                source_letter.get(
                                    "pdf_letter_s3_link"
                                )
                            )

                        blockchain_hash = (
                            source_letter.get("blockchain_hash")
                        )

                    request_first_name = (
                        row_dict.get(
                            recommendation_table.c.first_name
                        )
                        or
                        ""
                    )

                    request_middle_name = (
                        row_dict.get(
                            recommendation_table.c.middle_name
                        )
                        or
                        ""
                    )

                    request_last_name = (
                        row_dict.get(
                            recommendation_table.c.last_name
                        )
                        or
                        ""
                    )

                    issuer_full_name = self._join_name(
                        issuer_first_name,
                        issuer_middle_name,
                        issuer_last_name
                    )

                    logger.info(
                        f"Issuer full name: "
                        f"{issuer_full_name}"
                    )

                    recommender_full_name = issuer_full_name

                    if not recommender_full_name:

                        recommender_full_name = self._join_name(
                            request_first_name,
                            request_middle_name,
                            request_last_name
                        )

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

                    source_enrollment = None

                    if source_student_id:

                        source_enrollment = (
                            self.fetch_one_by_column(

                                self.source_engine,

                                "enrollment",

                                "student_id",

                                source_student_id
                            )
                        )

                    if source_enrollment:

                        fetched_enrollment_code = (
                            source_enrollment.get(
                                "enrollment_UUID"
                            )
                            or
                            source_enrollment.get(
                                "enrollment_code"
                            )
                        )

                        if fetched_enrollment_code:

                            enrollment_code = (
                                fetched_enrollment_code
                            )

                    if not enrollment_code:

                        destination_enrollment = (
                            self.fetch_one_by_column(

                                auth_db_engine,

                                "user_enrollments",

                                "user_uuid",

                                destination_user_uuid
                            )
                        )

                        if destination_enrollment:

                            enrollment_code = (
                                destination_enrollment.get(
                                    "enrollment_code"
                                )
                                or
                                ""
                            )

                            if not student_number:

                                student_number = (
                                    destination_enrollment.get(
                                        "student_number"
                                    )
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

                    credential_path = upload_letter

                    upload_letter_file_name = (
                        self._extract_file_name(
                            upload_letter
                        )
                    )

                    request_status = row_dict.get(
                        recommendation_table.c.request_status
                    )

                    status = self._map_status(
                        request_status
                    )

                    is_confidential = (
                        1
                        if row_dict.get(
                            recommendation_table.c.blind_recommendation_letter
                        )
                        else
                        0
                    )

                    # -------------------------------------------------
                    # credentials_recommendation_letters
                    # -------------------------------------------------

                    recommendation_row = {

                        'uuid': recommendation_uuid,

                        'created_at': created_at,

                        'updated_at': updated_at,

                        'deleted_at': None,

                        'recommender_full_name': (
                            recommender_full_name
                        ),

                        'is_confidential': is_confidential,

                        'due_date_to_recommender': (
                            due_date_to_recommender
                        ),

                        'recommendation_type': 1,

                        'recommendation_input_type': 1,

                        'recommendation_input': (
                            request_status
                        ),

                        'message_to_recommender': (
                            row_dict.get(
                                recommendation_table.c.personalized_message
                            )
                        ),

                        'supporting_materials_path': None,

                        'supporting_materials_file_name': None,

                        'upload_letter': upload_letter,

                        'upload_letter_file_name': (
                            upload_letter_file_name
                        ),

                        'description': (
                            row_dict.get(
                                recommendation_table.c.request_status_reason
                            )
                        ),

                        'user_id': (
                            destination_user_uuid
                        ),

                        'institution_id': (
                            institution_uuid
                        ),

                        'recommender_email': (
                            recommender_email
                        ),

                        'credential_type': 4,

                        'status': status,

                        'credential_path': credential_path,

                        'created_by': (
                            destination_user_uuid
                        ),

                        'updated_by': (
                            destination_user_uuid
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

                        'updated_at': updated_at,

                        'deleted_at': None,

                        'user_id': (
                            destination_user_uuid
                        ),

                        'institution_id': (
                            institution_uuid
                        ),

                        'student_user_name': (
                            source_username
                        ),

                        'institution_name': (
                            institution_name
                        ),

                        'student_id': (
                            str(student_number)
                            if student_number is not None
                            else None
                        ),

                        'student_email': (
                            source_username
                        ),

                        'credential_claim_status': 0,

                        'credential_type': 4,

                        'status': status,

                        'recommendation_letters': (
                            recommendation_uuid
                        ),

                        'credential_path': credential_path,

                        'enrollment_code': (
                            enrollment_code
                        ),

                        'created_by': (
                            destination_user_uuid
                        ),

                        'updated_by': (
                            destination_user_uuid
                        ),

                        'deleted_by': None,

                        'issued_on': str(
                            (
                                row_dict.get(
                                    recommendation_table.c.date_issued
                                )
                                or
                                created_at
                            )
                        ),

                        'generated_on': None,
                        
                        'is_registered': 1,
                    }

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "student_first_name",
                        student_first_name
                    )

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "student_last_name",
                        student_last_name
                    )

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "dateofbirth",
                        student_date_of_birth
                    )

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "date_of_birth",
                        student_date_of_birth
                    )

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "student_number",
                        (
                            str(student_number)
                            if student_number is not None
                            else None
                        )
                    )

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "enrollmentcode",
                        enrollment_code
                    )

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "blockchain_hash",
                        blockchain_hash
                    )

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

    # -------------------------------------------------
    # HELPERS
    # -------------------------------------------------

    def _set_if_column(
        self,
        row: dict,
        table,
        column_name: str,
        value
    ) -> None:

        if column_name in table.c:

            row[column_name] = value

    def _get_recommendation_destination_table_name(
        self
    ) -> str:

        inspector = inspect(
            self.dest_engine
        )

        table_names = set(
            inspector.get_table_names()
        )

        candidate_tables = [
            "credentials_recommendation_letters"
        ]

        for mapping in self.config.get(
            "mappings",
            []
        ):

            destination_table = (
                mapping.get(
                    "destination_table"
                )
                or
                ""
            ).strip()

            if (
                destination_table
                in candidate_tables
                and
                destination_table in table_names
            ):

                return destination_table

        for candidate_table in candidate_tables:

            if candidate_table in table_names:

                logger.info(
                    "Using recommendation destination table: "
                    f"{candidate_table}"
                )

                return candidate_table

        recommendation_tables = sorted(
            table_name
            for table_name in table_names
            if "recommendation" in table_name.lower()
        )

        raise ValueError(
            "Recommendation destination table not found. "
            "Expected one of: "
            f"{candidate_tables}. "
            "Available recommendation tables: "
            f"{recommendation_tables}"
        )

    def _is_recommendation_destination_table(
        self,
        table_name: str
    ) -> bool:

        return (
            table_name or ""
        ).strip().lower() in [
            "credentials_recommendation_letters"
        ]

    def _join_name(
        self,
        *parts
    ) -> str:

        return " ".join(
            str(part).strip()
            for part in parts
            if part
            and
            str(part).strip()
        )

    def _extract_file_name(
        self,
        file_path
    ):

        if not file_path:

            return None

        return str(file_path).rstrip("/").split("/")[-1]

    def _map_status(
        self,
        request_status
    ) -> int:

        normalized_status = (
            str(request_status or "")
            .strip()
            .lower()
        )

        if normalized_status == "issued":

            return 3

        return 1
