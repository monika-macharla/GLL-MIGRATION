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


class ResumeMigrator(BaseMigrator):

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
            f"Resume migration config limit: "
            f"{self.config.get('limit')}"
        )

        logger.info(
            "Starting Resume Migration..."
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
        # SOURCE TABLE
        # -------------------------------------------------

        resume_table = self._manual_reflect(
            'resume',
            self.source_engine,
            self.metadata_source
        )

        logger.info(
            f"Resume table columns: "
            f"{resume_table.columns.keys()}"
        )

        # -------------------------------------------------
        # DESTINATION TABLES
        # -------------------------------------------------

        resume_dest_table = self._manual_reflect(
            'credentials_resume',
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
        # FETCH SOURCE RECORDS
        # -------------------------------------------------

        query = select(
            resume_table
        )

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                query
            ).fetchall()

            if not rows:

                logger.warning(
                    "No resume records found"
                )

                return 0

            logger.info(
                f"Found {len(rows)} "
                f"resume records"
            )

            resume_insert_data = []

            credentials_insert_data = []

            skipped_missing_user_id = 0

            skipped_missing_source_user = 0

            skipped_missing_username = 0

            skipped_missing_dest_user = 0

            skipped_missing_user_institution = 0

            skipped_missing_institution_uuid = 0

            failed_rows = 0

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
                    # SOURCE USER ID
                    # -------------------------------------------------

                    source_gl_user_id = row_dict.get(
                        resume_table.c.user_id
                    )

                    logger.info(
                        f"Source gl_user.id: "
                        f"{source_gl_user_id}"
                    )

                    if not source_gl_user_id:

                        skipped_missing_user_id += 1

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

                        skipped_missing_source_user += 1

                        logger.warning(
                            f"No gl_user found "
                            f"for id: "
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

                        skipped_missing_username += 1

                        logger.warning(
                            "username is null"
                        )

                        continue

                    # -------------------------------------------------
                    # FETCH SOURCE STUDENT DETAILS
                    # -------------------------------------------------

                    source_gl_student = (
                        self.fetch_one_by_column(
                            self.source_engine,
                            "gl_student",
                            "user_id",
                            source_gl_user_id
                        )
                    )

                    source_student_id = None

                    student_first_name = None

                    student_last_name = None

                    student_date_of_birth = None

                    student_number = None

                    if source_gl_student:

                        source_student_id = (
                            source_gl_student.get("id")
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

                        student_number = (
                            source_gl_student.get("school_student_id")
                            or
                            source_gl_student.get("student_number")
                            or
                            source_student_id
                        )

                    logger.info(
                        f"Source student details: "
                        f"student_id={source_student_id}, "
                        f"student_number={student_number}, "
                        f"first_name={student_first_name}, "
                        f"last_name={student_last_name}, "
                        f"date_of_birth={student_date_of_birth}"
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

                        skipped_missing_dest_user += 1

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

                        skipped_missing_user_institution += 1

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

                        skipped_missing_institution_uuid += 1

                        logger.warning(
                            "institution_uuid is null"
                        )

                        continue

                    logger.info(
                        f"Institution UUID: "
                        f"{institution_uuid}"
                    )

                    # -------------------------------------------------
                    # FETCH INSTITUTION
                    # -------------------------------------------------

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
                            institution_row.get(
                                "name"
                            )
                        )

                    logger.info(
                        f"Institution Name: "
                        f"{institution_name}"
                    )

                    # -------------------------------------------------
                    # CREATED BY
                    # -------------------------------------------------

                    created_by_uuid = (
                        destination_user_uuid
                    )

                    logger.info(
                        f"Created by UUID: "
                        f"{created_by_uuid}"
                    )

                    # -------------------------------------------------
                    # ENROLLMENT CODE
                    # -------------------------------------------------

                    enrollment_code = ""

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

                            fetched_enrollment_code = (
                                destination_enrollment.get(
                                    "enrollment_code"
                                )
                            )

                            if fetched_enrollment_code:

                                enrollment_code = (
                                    fetched_enrollment_code
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
                    # CREATED AT / UPDATED AT
                    # -------------------------------------------------

                    created_at = row_dict.get(
                        resume_table.c.uploaded_date
                    )

                    logger.info(
                        f"uploaded_date: "
                        f"{created_at}"
                    )

                    # -------------------------------------------------
                    # FILE PATH
                    # -------------------------------------------------

                    # FINAL FORMAT:
                    # resume/user_id/resume-data

                    file_path = (
                        f"resume/"
                        f"{source_gl_user_id}/"
                        f"resume-data"
                    )

                    file_name = row_dict.get(
                        resume_table.c.resume_name
                    )

                    if not file_name:

                        file_name = "resume-data"

                    file_type = self._get_file_type(
                        file_name
                    )

                    logger.info(
                        f"Resume file path: "
                        f"{file_path}"
                    )

                    logger.info(
                        f"Resume file name: "
                        f"{file_name}"
                    )

                    logger.info(
                        f"Resume file type: "
                        f"{file_type}"
                    )

                    # -------------------------------------------------
                    # RESUME UUID
                    # -------------------------------------------------

                    resume_uuid = str(
                        uuid.uuid4()
                    )

                    # -------------------------------------------------
                    # credentials_resume
                    # -------------------------------------------------

                    resume_row = {

                        'uuid': resume_uuid,

                        'created_at': created_at,

                        'updated_at': created_at,

                        'deleted_at': None,

                        'user_id':
                            destination_user_uuid,

                        'institution_id':
                            institution_uuid,

                        'file_path':
                            file_path,

                        'file_name':
                            file_name,

                        'file_type':
                            file_type,

                        'status': 2,

                        'credential_path':
                            file_path,

                        'credential_type': 6,

                        'created_by':
                            created_by_uuid,

                        'updated_by':
                            created_by_uuid,

                        'deleted_by': None,

                        'enrollment_code':
                            enrollment_code,

                        'generated_on': None,
                    }

                    logger.info(
                        f"Resume row: "
                        f"{resume_row}"
                    )

                    resume_insert_data.append(
                        resume_row
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

                        'user_id':
                            destination_user_uuid,

                        'student_user_name':
                            source_username,

                        'institution_name':
                            institution_name,

                        'student_id':
                            (
                                str(student_number)
                                if student_number is not None
                                else None
                            ),

                        'student_email':
                            source_username,

                        'credential_claim_status': 0,

                        'is_registered': 1,

                        'institution_id':
                            institution_uuid,

                        'status': 2,

                        'credential_type': 6,

                        'credential_path':
                            file_path,

                        'enrollment_code':
                            enrollment_code,

                        'created_by':
                            created_by_uuid,

                        'updated_by':
                            created_by_uuid,

                        'resume':
                            resume_uuid,

                        'issued_on':
                            str(created_at),

                        'generated_on': None,
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

                    logger.info(
                        f"Credentials row: "
                        f"{credentials_row}"
                    )

                    credentials_insert_data.append(
                        credentials_row
                    )

                except Exception as e:

                    failed_rows += 1

                    logger.exception(
                        f"Failed processing "
                        f"row {index}: {e}"
                    )

            # -------------------------------------------------
            # INSERT DATA
            # -------------------------------------------------

            if resume_insert_data:

                skipped_count = (
                    skipped_missing_user_id
                    + skipped_missing_source_user
                    + skipped_missing_username
                    + skipped_missing_dest_user
                    + skipped_missing_user_institution
                    + skipped_missing_institution_uuid
                    + failed_rows
                )

                logger.info(
                    "Resume migration summary: "
                    f"source_rows={len(rows)}, "
                    f"prepared_rows={len(resume_insert_data)}, "
                    f"skipped_rows={skipped_count}, "
                    f"missing_user_id={skipped_missing_user_id}, "
                    f"missing_source_user={skipped_missing_source_user}, "
                    f"missing_username={skipped_missing_username}, "
                    f"missing_dest_user={skipped_missing_dest_user}, "
                    f"missing_user_institution={skipped_missing_user_institution}, "
                    f"missing_institution_uuid={skipped_missing_institution_uuid}, "
                    f"failed_rows={failed_rows}"
                )

                logger.info(
                    f"Prepared "
                    f"{len(resume_insert_data)} "
                    f"records for insertion"
                )

                with self.dest_engine.begin() as dest_conn:

                    result = dest_conn.execute(
                        insert(resume_dest_table),
                        resume_insert_data
                    )

                    logger.info(
                        f"Inserted resume rows: "
                        f"{result.rowcount}"
                    )

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
                    f"{len(resume_insert_data)} "
                    f"resume records"
                )

                logger.info(
                    "======================================"
                )

                return len(
                    resume_insert_data
                )

            logger.warning(
                "No valid records "
                "available for insertion"
            )

            logger.info(
                "Resume migration summary: "
                f"source_rows={len(rows)}, "
                "prepared_rows=0, "
                f"missing_user_id={skipped_missing_user_id}, "
                f"missing_source_user={skipped_missing_source_user}, "
                f"missing_username={skipped_missing_username}, "
                f"missing_dest_user={skipped_missing_dest_user}, "
                f"missing_user_institution={skipped_missing_user_institution}, "
                f"missing_institution_uuid={skipped_missing_institution_uuid}, "
                f"failed_rows={failed_rows}"
            )

            return 0

    # -------------------------------------------------
    # SET DESTINATION COLUMN IF IT EXISTS
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

    # -------------------------------------------------
    # EXTRACT FILE NAME
    # -------------------------------------------------

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

            '.pdf': 'application/pdf',

            '.doc': 'application/msword',

            '.docx':
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
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
