import uuid
import logging
import mimetypes

from pathlib import Path

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class SelfUploadMigrator(BaseMigrator):

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

    def migrate(self) -> int:

        logger.info(
            "Starting Self Upload Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        source_table = self._manual_reflect(
            "other_credentials",
            self.source_engine,
            self.metadata_source
        )

        self_upload_table = self._manual_reflect(
            "credentials_self_uploads",
            self.dest_engine,
            self.metadata_dest
        )

        credentials_table = self._manual_reflect(
            "credentials_all",
            self.dest_engine,
            self.metadata_dest
        )

        query = select(
            source_table
        )

        if self.config.get("limit"):

            query = query.limit(
                self.config["limit"]
            )

        self_upload_insert_data = []

        credentials_insert_data = []

        skipped_count = 0

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                query
            ).fetchall()

        logger.info(
            f"Found {len(rows)} other_credentials records"
        )

        for index, row in enumerate(
            rows,
            start=1
        ):

            try:

                row_dict = row._mapping

                source_other_credential_id = row_dict.get(
                    source_table.c.id
                )

                source_gl_user_id = row_dict.get(
                    source_table.c.user_id
                )

                if not source_gl_user_id:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping row {index}: user_id is null"
                    )

                    continue

                source_gl_user = self.fetch_one_by_column(
                    self.source_engine,
                    "gl_user",
                    "id",
                    source_gl_user_id
                )

                if not source_gl_user:

                    skipped_count += 1

                    logger.warning(
                        "Skipping row "
                        f"{index}: no gl_user for id "
                        f"{source_gl_user_id}"
                    )

                    continue

                source_username = source_gl_user.get(
                    "username"
                )

                if not source_username:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping row {index}: username is null"
                    )

                    continue

                dest_user = self.fetch_one_by_column(
                    auth_db_engine,
                    "users",
                    "user_name",
                    source_username
                )

                if not dest_user:

                    skipped_count += 1

                    logger.warning(
                        "Skipping row "
                        f"{index}: no destination user for "
                        f"{source_username}"
                    )

                    continue

                destination_user_uuid = dest_user.get(
                    "uuid"
                )

                user_institution = self.fetch_one_by_column(
                    auth_db_engine,
                    "user_institution",
                    "user_uuid",
                    destination_user_uuid
                )

                if not user_institution:

                    skipped_count += 1

                    logger.warning(
                        "Skipping row "
                        f"{index}: no user_institution for "
                        f"{destination_user_uuid}"
                    )

                    continue

                institution_uuid = user_institution.get(
                    "institution_uuid"
                )

                if not institution_uuid:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping row {index}: institution_uuid is null"
                    )

                    continue

                institution_row = self.fetch_one_by_column(
                    auth_db_engine,
                    "institutions",
                    "uuid",
                    institution_uuid
                )

                institution_name = (
                    institution_row.get("name")
                    if institution_row
                    else None
                )

                source_gl_student = self.fetch_one_by_column(
                    self.source_engine,
                    "gl_student",
                    "user_id",
                    source_gl_user_id
                )

                source_student_id = None

                student_number = None

                if source_gl_student:

                    source_student_id = source_gl_student.get(
                        "id"
                    )

                    student_number = (
                        source_gl_student.get("school_student_id")
                        or
                        source_gl_student.get("student_number")
                        or
                        source_student_id
                    )

                enrollment_code = self._get_enrollment_code(
                    auth_db_engine,
                    destination_user_uuid,
                    source_student_id
                )

                created_at = row_dict.get(
                    source_table.c.upload_date
                )

                file_path = (
                    f"other_credential/"
                    f"{source_other_credential_id}/"
                    f"credential_data"
                )

                file_name = (
                    row_dict.get(
                        source_table.c.uploaded_document_name
                    )
                    or
                    row_dict.get(
                        source_table.c.document_name
                    )
                )

                file_type = self._get_file_type(
                    file_name
                )

                self_upload_uuid = str(
                    uuid.uuid4()
                )

                document_title = row_dict.get(
                    source_table.c.document_name
                )

                document_type = self._map_document_type(
                    row_dict.get(
                        source_table.c.document_type
                    )
                )

                status = (
                    2
                    if row_dict.get(
                        source_table.c.active
                    )
                    else
                    1
                )

                self_upload_row = {
                    "uuid": self_upload_uuid,
                    "created_at": created_at,
                    "updated_at": created_at,
                    "deleted_at": None,
                    "document_title": document_title,
                    "document_type": document_type,
                    "user_id": destination_user_uuid,
                    "institution_id": institution_uuid,
                    "file_path": file_path,
                    "file_name": file_name,
                    "credential_type": 5,
                    "credential_path": file_path,
                    "file_type": file_type,
                    "status": status,
                    "created_by": destination_user_uuid,
                    "updated_by": destination_user_uuid,
                    "deleted_by": None,
                    "enrollment_code": enrollment_code,
                    "generated_on": None,
                }

                self_upload_insert_data.append(
                    self_upload_row
                )

                credentials_row = {
                    "uuid": str(uuid.uuid4()),
                    "created_at": created_at,
                    "updated_at": created_at,
                    "deleted_at": None,
                    "user_id": destination_user_uuid,
                    "student_user_name": source_username,
                    "institution_name": institution_name,
                    "student_id": (
                        str(student_number)
                        if student_number is not None
                        else None
                    ),
                    "student_email": source_username,
                    "credential_claim_status": 0,
                    "is_registered": 1,
                    "institution_id": institution_uuid,
                    "status": status,
                    "credential_type": 5,
                    "credential_path": file_path,
                    "enrollment_code": enrollment_code,
                    "created_by": destination_user_uuid,
                    "updated_by": destination_user_uuid,
                    "deleted_by": None,
                    "issued_on": str(created_at),
                    "generated_on": None,
                }

                self._set_if_column(
                    credentials_row,
                    credentials_table,
                    "self_uploads",
                    self_upload_uuid
                )

                self._set_if_column(
                    credentials_row,
                    credentials_table,
                    "self_upload",
                    self_upload_uuid
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
                    "blockchain_hash",
                    row_dict.get(
                        source_table.c.blockchain_hash
                    )
                )

                credentials_insert_data.append(
                    credentials_row
                )

            except Exception as error:

                skipped_count += 1

                logger.exception(
                    f"Failed processing row {index}: {error}"
                )

        if not self_upload_insert_data:

            logger.warning(
                "No valid self upload records available for insertion"
            )

            return 0

        with self.dest_engine.begin() as dest_conn:

            result = dest_conn.execute(
                insert(self_upload_table),
                self_upload_insert_data
            )

            logger.info(
                f"Inserted self upload rows: {result.rowcount}"
            )

            credentials_result = dest_conn.execute(
                insert(credentials_table),
                credentials_insert_data
            )

            logger.info(
                f"Inserted credentials_all rows: "
                f"{credentials_result.rowcount}"
            )

        logger.info(
            "Self Upload Migration summary: "
            f"inserted={len(self_upload_insert_data)}, "
            f"skipped={skipped_count}"
        )

        return len(
            self_upload_insert_data
        )

    def _get_enrollment_code(
        self,
        auth_db_engine,
        destination_user_uuid,
        source_student_id
    ):

        if source_student_id:

            source_enrollment = self.fetch_one_by_column(
                self.source_engine,
                "enrollment",
                "student_id",
                source_student_id
            )

            if source_enrollment:

                enrollment_code = (
                    source_enrollment.get("enrollment_UUID")
                    or
                    source_enrollment.get("enrollment_code")
                )

                if enrollment_code:

                    return enrollment_code

        destination_enrollment = self.fetch_one_by_column(
            auth_db_engine,
            "user_enrollments",
            "user_uuid",
            destination_user_uuid
        )

        if destination_enrollment:

            return (
                destination_enrollment.get("enrollment_code")
                or
                ""
            )

        return ""

    def _map_document_type(
        self,
        document_type
    ) -> int:

        normalized_document_type = (
            str(document_type or "")
            .strip()
            .lower()
        )

        mapping = {
            "immunization record": 1,
            "transcript": 2,
            "test score": 3,
            "other": 4,
        }

        return mapping.get(
            normalized_document_type,
            4
        )

    def _get_file_type(
        self,
        file_name
    ):

        if not file_name:

            return "application/octet-stream"

        ext = Path(
            str(file_name)
        ).suffix.lower()

        mime_mapping = {
            ".pdf": "application/pdf",
            ".doc": "application/msword",
            ".docx": (
                "application/vnd.openxmlformats-officedocument."
                "wordprocessingml.document"
            ),
        }

        if ext in mime_mapping:

            return mime_mapping[ext]

        mime_type, _ = mimetypes.guess_type(
            str(file_name)
        )

        return (
            mime_type
            or
            "application/octet-stream"
        )

    def _set_if_column(
        self,
        row,
        table,
        column_name,
        value
    ) -> None:

        if column_name in table.c:

            row[column_name] = value
