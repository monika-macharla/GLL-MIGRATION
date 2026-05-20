import logging
import mimetypes
import uuid

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import (
    insert,
    inspect,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CertificateMigrator(BaseMigrator):

    CREDENTIAL_TYPE = 2
    SOURCE_TABLE = "certificate"
    DESTINATION_TABLE = "credentials_certifications"
    DESTINATION_TABLE_CANDIDATES = [
        "credentials_certifications",
        "credentials_cerificate",
        "credentials_certificate"
    ]

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
            "Starting Certificate Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        certificate_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        certificate_destination_table_name = (
            self._get_certificate_destination_table_name()
        )

        certificate_dest_table = self._manual_reflect(
            certificate_destination_table_name,
            self.dest_engine,
            self.metadata_dest
        )

        credentials_table = self._manual_reflect(
            "credentials_all",
            self.dest_engine,
            self.metadata_dest
        )

        logger.info(
            f"Certificate source columns: "
            f"{certificate_table.columns.keys()}"
        )

        logger.info(
            f"Certificate destination columns: "
            f"{certificate_dest_table.columns.keys()}"
        )

        query = select(
            certificate_table
        )

        if self.config.get("limit"):

            query = query.limit(
                self.config["limit"]
            )

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                query
            ).fetchall()

        logger.info(
            f"Found {len(rows)} certificate records"
        )

        certificate_insert_data = []

        credentials_insert_data = []

        skipped_count = 0

        for index, row in enumerate(
            rows,
            start=1
        ):

            try:

                row_dict = row._mapping

                source_certificate_id = self._get_source_value(
                    row_dict,
                    certificate_table,
                    "id",
                    "certificate_id"
                )

                source_gl_user_id = self._get_source_value(
                    row_dict,
                    certificate_table,
                    "user_id",
                    "student_user_id",
                    "created_for"
                )

                source_student_id = self._get_source_value(
                    row_dict,
                    certificate_table,
                    "student_id"
                )

                source_gl_student = None

                if source_student_id:

                    source_gl_student = self.fetch_one_by_column(
                        self.source_engine,
                        "gl_student",
                        "id",
                        source_student_id
                    )

                    if source_gl_student and not source_gl_user_id:

                        source_gl_user_id = source_gl_student.get(
                            "user_id"
                        )

                if not source_gl_user_id:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping certificate row {index}: "
                        "user_id is null"
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
                        "Skipping certificate row "
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
                        f"Skipping certificate row {index}: "
                        "username is null"
                    )

                    continue

                if not source_gl_student:

                    source_gl_student = self.fetch_one_by_column(
                        self.source_engine,
                        "gl_student",
                        "user_id",
                        source_gl_user_id
                    )

                dest_user = self.fetch_one_by_column(
                    auth_db_engine,
                    "users",
                    "user_name",
                    source_username
                )

                if not dest_user:

                    skipped_count += 1

                    logger.warning(
                        "Skipping certificate row "
                        f"{index}: no destination user for "
                        f"{source_username}"
                    )

                    continue

                destination_user_uuid = dest_user.get(
                    "uuid"
                )

                institution_uuid = (
                    self._get_destination_institution_uuid(
                        auth_db_engine,
                        self._get_source_value(
                            row_dict,
                            certificate_table,
                            "institution_id"
                        )
                    )
                    or
                    self._get_user_institution_uuid(
                        auth_db_engine,
                        destination_user_uuid
                    )
                )

                if not institution_uuid:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping certificate row {index}: "
                        "institution_uuid is null"
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

                student_number = None

                student_first_name = None

                student_last_name = None

                student_date_of_birth = None

                if source_gl_student:

                    source_student_id = (
                        source_student_id
                        or
                        source_gl_student.get("id")
                    )

                    student_number = (
                        source_gl_student.get("school_student_id")
                        or
                        source_gl_student.get("student_number")
                        or
                        source_student_id
                    )

                    student_first_name = source_gl_student.get(
                        "first_name"
                    )

                    student_last_name = source_gl_student.get(
                        "last_name"
                    )

                    student_date_of_birth = source_gl_student.get(
                        "date_of_birth"
                    )

                enrollment_code = self._get_enrollment_code(
                    auth_db_engine,
                    destination_user_uuid,
                    self._get_source_value(
                        row_dict,
                        certificate_table,
                        "enrollment_id"
                    ),
                    source_student_id
                )

                created_at = self._get_source_value(
                    row_dict,
                    certificate_table,
                    "issued_date",
                    "issued_on",
                    "created_at",
                    "created_date",
                    "upload_date",
                    "uploaded_date",
                    "requested_time"
                )

                if not created_at:

                    created_at = datetime.utcnow()

                status = self._map_status(
                    self._get_source_value(
                        row_dict,
                        certificate_table,
                        "status",
                        "active"
                    )
                )

                source_file_value = self._get_source_value(
                    row_dict,
                    certificate_table,
                    "file_path",
                    "certificate_path",
                    "certificate_url",
                    "certificate_file",
                    "uploaded_document",
                    "document",
                    "image"
                )

                file_name = (
                    self._get_source_value(
                        row_dict,
                        certificate_table,
                        "file_name",
                        "certificate_name",
                        "certificate_file_name",
                        "uploaded_document_name",
                        "document_name",
                        "name",
                        "title"
                    )
                    or
                    self._extract_file_name(source_file_value)
                    or
                    "certificate-data"
                )

                file_type = self._get_file_type(
                    file_name
                )

                credential_path = (
                    source_file_value
                    or
                    f"certificate/"
                    f"{source_certificate_id or source_gl_user_id}/"
                    f"credential_data"
                )

                certificate_uuid = str(
                    uuid.uuid4()
                )

                created_by_uuid = destination_user_uuid

                certificate_row = {
                    "uuid": certificate_uuid,
                    "created_at": created_at,
                    "updated_at": created_at,
                    "deleted_at": None,
                    "user_id": destination_user_uuid,
                    "institution_id": institution_uuid,
                    "status": status,
                    "credential_type": self.CREDENTIAL_TYPE,
                    "credential_path": credential_path,
                    "file_path": credential_path,
                    "file_name": file_name,
                    "file_type": file_type,
                    "enrollment_code": enrollment_code,
                    "created_by": created_by_uuid,
                    "updated_by": created_by_uuid,
                    "deleted_by": None,
                    "generated_on": None,
                }

                self._set_first_existing_column(
                    certificate_row,
                    certificate_dest_table,
                    ["certificate_name", "document_title", "title"],
                    file_name
                )

                self._set_if_column(
                    certificate_row,
                    certificate_dest_table,
                    "issued_on",
                    str(created_at)
                )

                self._set_if_column(
                    certificate_row,
                    certificate_dest_table,
                    "description",
                    self._get_source_value(
                        row_dict,
                        certificate_table,
                        "description"
                    )
                )

                certificate_insert_data.append(
                    self._filter_to_table_columns(
                        certificate_row,
                        certificate_dest_table
                    )
                )

                credentials_row = {
                    "uuid": str(uuid.uuid4()),
                    "created_at": created_at,
                    "updated_at": created_at,
                    "deleted_at": None,
                    "user_id": destination_user_uuid,
                    "student_user_name": source_username,
                    "student_first_name": student_first_name,
                    "student_last_name": student_last_name,
                    "institution_name": institution_name,
                    "student_id": (
                        str(student_number)
                        if student_number is not None
                        else None
                    ),
                    "student_email": source_username,
                    "date_of_birth": (
                        str(student_date_of_birth)
                        if student_date_of_birth is not None
                        else None
                    ),
                    "credential_claim_status": 0,
                    "is_registered": 1,
                    "institution_id": institution_uuid,
                    "status": status,
                    "credential_type": self.CREDENTIAL_TYPE,
                    "credential_path": credential_path,
                    "enrollment_code": enrollment_code,
                    "created_by": created_by_uuid,
                    "updated_by": created_by_uuid,
                    "deleted_by": None,
                    "generated_on": None,
                    "issued_on": (
                        str(created_at)
                        if created_at is not None
                        else None
                    ),
                    "student_number": (
                        str(student_number)
                        if student_number is not None
                        else None
                    ),
                }

                self._set_first_existing_column(
                    credentials_row,
                    credentials_table,
                    [
                        "cerificate",
                        "certificate",
                        "certificates",
                        "credentials_cerificate"
                    ],
                    certificate_uuid
                )

                self._set_if_column(
                    credentials_row,
                    credentials_table,
                    "blockchain_hash",
                    self._get_source_value(
                        row_dict,
                        certificate_table,
                        "blockchain_hash"
                    )
                )

                credentials_insert_data.append(
                    self._filter_to_table_columns(
                        credentials_row,
                        credentials_table
                    )
                )

            except Exception as error:

                skipped_count += 1

                logger.exception(
                    f"Failed processing certificate row "
                    f"{index}: {error}"
                )

        if not certificate_insert_data:

            logger.warning(
                "No valid certificate records available for insertion"
            )

            return 0

        with self.dest_engine.begin() as dest_conn:

            result = dest_conn.execute(
                insert(certificate_dest_table),
                certificate_insert_data
            )

            logger.info(
                f"Inserted certificate rows: {result.rowcount}"
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
            "Certificate Migration summary: "
            f"inserted={len(certificate_insert_data)}, "
            f"skipped={skipped_count}"
        )

        return len(
            certificate_insert_data
        )

    def _get_certificate_destination_table_name(
        self
    ):

        inspector = inspect(
            self.dest_engine
        )

        destination_table_names = set(
            inspector.get_table_names()
        )

        configured_table_names = []

        for mapping in self.config.get(
            "mappings",
            []
        ):

            destination_table = str(
                mapping.get("destination_table") or ""
            ).strip()

            if destination_table:

                configured_table_names.append(
                    destination_table
                )

        for table_name in (
            configured_table_names
            + self.DESTINATION_TABLE_CANDIDATES
        ):

            if table_name in destination_table_names:

                logger.info(
                    "Using certificate destination table: "
                    f"{table_name}"
                )

                return table_name

        raise ValueError(
            "Certificate destination table not found. "
            "Tried: "
            f"{configured_table_names + self.DESTINATION_TABLE_CANDIDATES}"
        )

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

    def _get_destination_institution_uuid(
        self,
        auth_db_engine,
        source_institution_id
    ):

        if not source_institution_id:

            return None

        source_institution = self.fetch_one_by_column(
            self.source_engine,
            "institution",
            "id",
            source_institution_id
        )

        if not source_institution:

            return None

        institution_name = source_institution.get(
            "name"
        )

        if not institution_name:

            return None

        destination_institution = self.fetch_one_by_column(
            auth_db_engine,
            "institutions",
            "name",
            institution_name
        )

        if destination_institution:

            return destination_institution.get(
                "uuid"
            )

        return None

    def _get_user_institution_uuid(
        self,
        auth_db_engine,
        destination_user_uuid
    ):

        user_institution = self.fetch_one_by_column(
            auth_db_engine,
            "user_institution",
            "user_uuid",
            destination_user_uuid
        )

        if user_institution:

            return user_institution.get(
                "institution_uuid"
            )

        return None

    def _get_enrollment_code(
        self,
        auth_db_engine,
        destination_user_uuid,
        source_enrollment_id,
        source_student_id
    ):

        if source_enrollment_id:

            source_enrollment = self.fetch_one_by_column(
                self.source_engine,
                "enrollment",
                "id",
                source_enrollment_id
            )

            enrollment_code = self._extract_enrollment_code(
                source_enrollment
            )

            if enrollment_code:

                return enrollment_code

        if source_student_id:

            source_enrollment = self.fetch_one_by_column(
                self.source_engine,
                "enrollment",
                "student_id",
                source_student_id
            )

            enrollment_code = self._extract_enrollment_code(
                source_enrollment
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

    def _extract_enrollment_code(
        self,
        enrollment_row
    ):

        if not enrollment_row:

            return None

        return (
            enrollment_row.get("enrollment_UUID")
            or
            enrollment_row.get("enrollment_code")
        )

    def _map_status(
        self,
        status
    ) -> int:

        if isinstance(status, bool):

            return 2 if status else 1

        normalized_status = (
            str(status or "")
            .strip()
            .lower()
        )

        if normalized_status in [
            "issued",
            "active",
            "completed",
            "success",
            "generated",
            "true",
            "1"
        ]:

            return 2

        if normalized_status in [
            "inactive",
            "failed",
            "error",
            "rejected",
            "false",
            "0"
        ]:

            return 1

        return 2

    def _extract_file_name(
        self,
        file_path
    ):

        if not file_path:

            return None

        try:

            parsed = urlparse(
                str(file_path)
            )

            filename = Path(
                parsed.path
            ).name

            if filename:

                return filename

            return Path(
                str(file_path)
            ).name

        except Exception:

            return None

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
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
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

    def _set_first_existing_column(
        self,
        row,
        table,
        column_names,
        value
    ) -> None:

        for column_name in column_names:

            if column_name in table.c:

                row[column_name] = value

                return

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
