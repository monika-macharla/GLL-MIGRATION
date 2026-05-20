import uuid
import logging
import mimetypes

from urllib.parse import urlparse
from pathlib import Path

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class DigitalBadgesMigrator(BaseMigrator):

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
            "Starting Digital Badges Migration..."
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
                "auth_db lookup engine "
                "not configured"
            )

        logger.info(
            "auth_db lookup engine loaded"
        )

        # -------------------------------------------------
        # SOURCE TABLES
        # -------------------------------------------------

        badge_table = self._manual_reflect(

            'badge',

            self.source_engine,

            self.metadata_source
        )

        # -------------------------------------------------
        # DESTINATION TABLES
        # -------------------------------------------------

        dest_table = self._manual_reflect(

            'credentials_digital_badges',

            self.dest_engine,

            self.metadata_dest
        )

        badge_info_table = self._manual_reflect(

            'credentials_digital_badge_info',

            self.dest_engine,

            self.metadata_dest
        )

        credentials_table = self._manual_reflect(

            'credentials_all',

            self.dest_engine,

            self.metadata_dest
        )

        logger.info(
            "Successfully reflected tables"
        )

        # -------------------------------------------------
        # FETCH RECORDS
        # -------------------------------------------------

        query = select(
            badge_table
        )

        if self.config.get("limit"):

            query = query.limit(
                self.config["limit"]
            )

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                query
            ).fetchall()

            if not rows:

                logger.warning(
                    "No badge records found"
                )

                return 0

            logger.info(
                f"Found {len(rows)} "
                f"badge records"
            )

            insert_data = []

            badge_info_insert_data = []

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
                        f"Processing row "
                        f"{index}"
                    )

                    row_dict = row._mapping

                    # -------------------------------------------------
                    # SOURCE USER / STUDENT IDS
                    # -------------------------------------------------

                    source_gl_user_id = self._get_source_value(
                        row_dict,
                        badge_table,
                        "user_id"
                    )

                    source_student_id = row_dict.get(
                        badge_table.c.student_id
                    ) if "student_id" in badge_table.c else None

                    logger.info(
                        f"Source user_id: "
                        f"{source_gl_user_id}, "
                        f"Source student_id: "
                        f"{source_student_id}"
                    )

                    if not source_gl_user_id and not source_student_id:

                        logger.warning(
                            "user_id and student_id are null"
                        )

                        continue

                    # -------------------------------------------------
                    # FETCH gl_student
                    # -------------------------------------------------

                    source_gl_student = None

                    if source_student_id:

                        source_gl_student = (
                            self.fetch_one_by_column(

                                self.source_engine,

                                "gl_student",

                                "id",

                                source_student_id
                            )
                        )

                    if source_student_id and not source_gl_student:

                        logger.warning(
                            f"No gl_student found "
                            f"for student_id: "
                            f"{source_student_id}"
                        )

                        continue

                    if source_gl_student and not source_gl_user_id:

                        source_gl_user_id = (
                            source_gl_student.get(
                                "user_id"
                            )
                        )

                    logger.info(
                        f"gl_user.id: "
                        f"{source_gl_user_id}"
                    )

                    if not source_gl_user_id:

                        logger.warning(
                            "gl_student.user_id is null"
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
                            f"No gl_user found "
                            f"for id: "
                            f"{source_gl_user_id}"
                        )

                        continue

                    # -------------------------------------------------
                    # SOURCE USERNAME
                    # -------------------------------------------------

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

                    if not source_gl_student:

                        source_gl_student = (
                            self.fetch_one_by_column(

                                self.source_engine,

                                "gl_student",

                                "user_id",

                                source_gl_user_id
                            )
                        )

                        if source_gl_student and not source_student_id:

                            source_student_id = (
                                source_gl_student.get(
                                    "id"
                                )
                            )

                    # -------------------------------------------------
                    # FETCH DEST USERS TABLE
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
                            f"No destination user "
                            f"found for username: "
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
                            f"No institution mapping "
                            f"found for user UUID: "
                            f"{destination_user_uuid}"
                        )

                        continue

                    institution_uuid = (
                        user_institution.get(
                            "institution_uuid"
                        )
                    )

                    if not institution_uuid:

                        logger.warning(
                            f"institution_uuid is null "
                            f"for user UUID: "
                            f"{destination_user_uuid}"
                        )

                        continue

                    logger.info(
                        f"Institution UUID: "
                        f"{institution_uuid}"
                    )

                    # -------------------------------------------------
                    # FETCH INSTITUTION NAME
                    # -------------------------------------------------

                    institution_row = (
                        self.fetch_one_by_column(

                            auth_db_engine,

                            "institutions",

                            "uuid",

                            institution_uuid
                        )
                    )

                    issuer_name = None

                    if institution_row:

                        issuer_name = institution_row.get(
                            "name"
                        )

                    logger.info(
                        f"Issuer institution name: "
                        f"{issuer_name}"
                    )

                    # -------------------------------------------------
                    # CREATED_BY LOGIC
                    # -------------------------------------------------

                    issuer_id = row_dict.get(
                        badge_table.c.issuer_id
                    )

                    logger.info(
                        f"Issuer ID: "
                        f"{issuer_id}"
                    )

                    if not issuer_id:

                        logger.warning(
                            "issuer_id is null. "
                            "Skipping record."
                        )

                        continue

                    created_by_uuid = None

                    issuer_gl_user = (
                        self.fetch_one_by_column(

                            self.source_engine,

                            "gl_user",

                            "id",

                            issuer_id
                        )
                    )

                    if not issuer_gl_user:

                        logger.warning(
                            f"No gl_user found "
                            f"for issuer_id: "
                            f"{issuer_id}"
                        )

                        continue

                    issuer_username = (
                        issuer_gl_user.get(
                            "username"
                        )
                    )

                    logger.info(
                        f"Issuer username: "
                        f"{issuer_username}"
                    )

                    if not issuer_username:

                        logger.warning(
                            "issuer_username is null. "
                            "Skipping record."
                        )

                        continue

                    issuer_dest_user = (
                        self.fetch_one_by_column(

                            auth_db_engine,

                            "users",

                            "user_name",

                            issuer_username
                        )
                    )

                    if not issuer_dest_user:

                        logger.warning(
                            f"No destination user found "
                            f"for issuer username: "
                            f"{issuer_username}"
                        )

                        continue

                    created_by_uuid = (
                        issuer_dest_user.get(
                            "uuid"
                        )
                    )

                    logger.info(
                        f"Created by UUID: "
                        f"{created_by_uuid}"
                    )

                    if not created_by_uuid:

                        logger.warning(
                            "created_by_uuid is null. "
                            "Skipping record."
                        )

                        continue

                    # -------------------------------------------------
                    # ENROLLMENT CODE LOGIC
                    # -------------------------------------------------

                    enrollment_code = None

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

                        source_enrollment_code = (
                            source_enrollment.get(
                                "enrollment_UUID"
                            )
                            or
                            source_enrollment.get(
                                "enrollment_code"
                            )
                        )

                        logger.info(
                            f"Source enrollment_code: "
                            f"{source_enrollment_code}"
                        )

                        enrollment_code = (
                            source_enrollment_code
                        )

                    logger.info(
                        f"Final enrollment_code: "
                        f"{enrollment_code}"
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

                    if enrollment_code is None:

                        enrollment_code = ""

                    # -------------------------------------------------
                    # IMAGE PATH
                    # -------------------------------------------------

                    image_path = self._get_source_value(
                        row_dict,
                        badge_table,
                        "image"
                    )

                    # -------------------------------------------------
                    # FILE NAME
                    # -------------------------------------------------

                    file_name = (
                        self._extract_file_name(
                            image_path
                        )
                    )

                    # -------------------------------------------------
                    # FILE TYPE
                    # -------------------------------------------------

                    file_type = (
                        self._get_file_type(
                            file_name
                        )
                    )

                    # -------------------------------------------------
                    # CREATED DATE
                    # -------------------------------------------------

                    created_at = self._get_source_value(
                        row_dict,
                        badge_table,
                        "issued_on",
                        "created_at",
                        "created_date"
                    )

                    # -------------------------------------------------
                    # GENERATED BADGE UUID
                    # -------------------------------------------------

                    badge_uuid = str(
                        uuid.uuid4()
                    )

                    # -------------------------------------------------
                    # MAIN BADGE TABLE
                    # -------------------------------------------------

                    mapped_row = {

                        'uuid': badge_uuid,

                        'created_at': created_at,

                        'updated_at': created_at,

                        'user_id':
                            destination_user_uuid,

                        'institution_id':
                            institution_uuid,

                        'created_by':
                            created_by_uuid,

                        'updated_by':
                            created_by_uuid,

                        'file_path':
                            image_path,

                        'file_name':
                            file_name,

                        'file_type':
                            file_type,

                        'badge_json':
                            self._get_source_value(
                                row_dict,
                                badge_table,
                                "assertion_json"
                            ),

                        'credential_type': 3,

                        'status': 2,

                        'enrollment_code':
                            enrollment_code,
                    }

                    logger.info(
                        f"Mapped row: "
                        f"{mapped_row}"
                    )

                    insert_data.append(
                        self._filter_to_table_columns(
                            mapped_row,
                            dest_table
                        )
                    )

                    # -------------------------------------------------
                    # BADGE INFO TABLE
                    # -------------------------------------------------

                    badge_info_row = {

                        'uuid': str(
                            uuid.uuid4()
                        ),

                        'created_at': created_at,

                        'updated_at': created_at,

                        'badge_id': badge_uuid,

                        'badgeId': badge_uuid,

                        'badge_name': self._get_source_value(
                            row_dict,
                            badge_table,
                            "badge_name"
                        ),

                        'badge_description': self._get_source_value(
                            row_dict,
                            badge_table,
                            "description"
                        ),

                        'earning_criteria': self._get_source_value(
                            row_dict,
                            badge_table,
                            "criteria"
                        ),

                        'issuer_name': issuer_name,

                        'expires_on': self._get_source_value(
                            row_dict,
                            badge_table,
                            "expires"
                        ),

                        'badge_image_url': image_path,

                        'pdf_path': None,
                    }

                    logger.info(
                        f"Badge info row: "
                        f"{badge_info_row}"
                    )

                    badge_info_insert_data.append(
                        self._filter_to_table_columns(
                            badge_info_row,
                            badge_info_table
                        )
                    )

                    # -------------------------------------------------
                    # CREDENTIALS TABLE
                    # -------------------------------------------------

                    credentials_row = {

                        'uuid': str(
                            uuid.uuid4()
                        ),

                        'created_at': created_at,

                        'updated_at': created_at,

                        'user_id':
                            destination_user_uuid,

                        'student_user_name':
                            source_username,

                        'institution_name':
                            issuer_name,

                        'student_id':
                            (
                                str(source_student_id)
                                if source_student_id is not None
                                else None
                            ),

                        'student_email':
                            source_username,

                        'credential_claim_status': 0,

                        'is_registered': 1,

                        'institution_id':
                            institution_uuid,

                        'status': 2,

                        'credential_type': 3,

                        'credential_path': None,

                        'enrollment_code':
                            enrollment_code,

                        'created_by':
                            created_by_uuid,

                        'updated_by':
                            created_by_uuid,

                        'digital_badges':
                            badge_uuid,

                        'issued_on':
                            str(created_at),

                        'badge_image':
                            image_path,
                    }

                    logger.info(
                        f"Credentials row: "
                        f"{credentials_row}"
                    )

                    credentials_insert_data.append(
                        self._filter_to_table_columns(
                            credentials_row,
                            credentials_table
                        )
                    )

                except Exception as e:

                    logger.exception(
                        f"Failed processing "
                        f"row {index}: {e}"
                    )

            # -------------------------------------------------
            # INSERT DATA
            # -------------------------------------------------

            if insert_data:

                logger.info(
                    f"Prepared "
                    f"{len(insert_data)} "
                    f"records for insertion"
                )

                with self.dest_engine.begin() as dest_conn:

                    # -----------------------------------------
                    # INSERT DIGITAL BADGES
                    # -----------------------------------------

                    result = dest_conn.execute(

                        insert(dest_table),

                        insert_data
                    )

                    logger.info(
                        f"Inserted digital badge rows: "
                        f"{result.rowcount}"
                    )

                    # -----------------------------------------
                    # INSERT BADGE INFO
                    # -----------------------------------------

                    if badge_info_insert_data:

                        badge_info_result = dest_conn.execute(

                            insert(badge_info_table),

                            badge_info_insert_data
                        )

                        logger.info(
                            f"Inserted badge info rows: "
                            f"{badge_info_result.rowcount}"
                        )

                    # -----------------------------------------
                    # INSERT CREDENTIALS
                    # -----------------------------------------

                    if credentials_insert_data:

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
                    f"{len(insert_data)} "
                    f"digital badges"
                )

                logger.info(
                    "======================================"
                )

                return len(insert_data)

            logger.warning(
                "No valid records "
                "available for insertion"
            )

            return 0

    # -------------------------------------------------
    # EXTRACT FILE NAME
    # -------------------------------------------------

    def _get_source_value(
        self,
        row,
        table,
        *column_names
    ):

        for column_name in column_names:

            if column_name in table.c:

                value = row.get(
                    table.c[column_name]
                )

                if value is not None:

                    return value

        return None

    def _filter_to_table_columns(
        self,
        row,
        table
    ):

        return {
            column_name: value
            for column_name, value in row.items()
            if column_name in table.c
        }

    def _extract_file_name(
        self,
        file_path: str
    ) -> str:

        if not file_path:

            return None

        try:

            parsed = urlparse(file_path)

            filename = Path(
                parsed.path
            ).name

            if filename:

                return filename

            return Path(
                file_path
            ).name

        except Exception:

            return None

    # -------------------------------------------------
    # DETECT MIME TYPE
    # -------------------------------------------------

    def _get_file_type(
        self,
        file_name: str
    ) -> str:

        if not file_name:

            return (
                'application/octet-stream'
            )

        ext = Path(
            file_name
        ).suffix.lower()

        mime_mapping = {

            '.png': 'image/png',

            '.jpg': 'image/jpeg',

            '.jpeg': 'image/jpeg',

            '.json': 'application/json',

            '.pdf': 'application/pdf',
        }

        if ext in mime_mapping:

            return mime_mapping[ext]

        mime_type, _ = mimetypes.guess_type(
            file_name
        )

        return (
            mime_type
            or
            'application/octet-stream'
        )
