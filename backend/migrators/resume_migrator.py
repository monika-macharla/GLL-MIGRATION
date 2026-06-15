import logging
import mimetypes
import uuid

from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    insert,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class ResumeMigrator(BaseMigrator):

    SOURCE_TABLE = "resume"
    RESUME_TABLE = "credentials_resume"
    CREDENTIALS_TABLE = "credentials_all"
    CREDENTIAL_TYPE = 6
    CREDENTIAL_CLAIM_STATUS_NOT_CLAIMED = 2
    DYNAMIC_VALUE = "DYNAMIC"
    LEGACY_S3_BASE_URL = (
        "https://greenlightlocker-com.s3.us-west-2.amazonaws.com"
    )
    UPLOADS_PREFIX = "/uploads"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 10000

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
            "Starting Resume Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        resume_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        source_user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        source_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        resume_dest_table = self._manual_reflect(
            self.RESUME_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        credentials_table = self._manual_reflect(
            self.CREDENTIALS_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        auth_users_table = self._manual_reflect(
            "users",
            auth_db_engine,
            self.metadata_dest
        )

        auth_user_institution_table = self._manual_reflect(
            "user_institution",
            auth_db_engine,
            self.metadata_dest
        )

        auth_user_enrollments_table = self._manual_reflect(
            "user_enrollments",
            auth_db_engine,
            self.metadata_dest
        )

        auth_institution_table = self._manual_reflect(
            "institutions",
            auth_db_engine,
            self.metadata_dest
        )

        logger.info(
            f"resume source columns: {resume_table.columns.keys()}"
        )
        logger.info(
            f"credentials_resume columns: "
            f"{resume_dest_table.columns.keys()}"
        )
        logger.info(
            f"credentials_all columns: {credentials_table.columns.keys()}"
        )

        batch_size = self._get_batch_size()

        destination_user_lookup = self._build_destination_user_lookup(
            auth_users_table,
            auth_db_engine
        )

        user_institution_lookup = self._build_user_institution_lookup(
            auth_user_institution_table,
            auth_db_engine
        )

        user_enrollment_lookup = self._build_user_enrollment_lookup(
            auth_user_enrollments_table,
            auth_db_engine
        )

        institution_name_by_uuid = self._build_institution_name_lookup(
            auth_institution_table,
            auth_db_engine
        )

        existing_credential_paths = self._load_existing_credential_paths(
            resume_dest_table,
            credentials_table
        )

        inserted_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        skipped_existing = 0
        dynamic_user_count = 0
        dynamic_institution_count = 0
        row_error_count = 0

        last_source_id = 0
        remaining_limit = self.config.get("limit")

        if remaining_limit:

            remaining_limit = int(
                remaining_limit
            )

        while True:

            fetch_size = batch_size

            if remaining_limit is not None:

                if remaining_limit <= 0:

                    break

                fetch_size = min(
                    fetch_size,
                    remaining_limit
                )

            with self.source_engine.connect() as source_conn:

                rows = source_conn.execute(
                    select(
                        resume_table
                    )
                    .where(
                        resume_table.c.id > last_source_id
                    )
                    .order_by(
                        resume_table.c.id
                    )
                    .limit(
                        fetch_size
                    )
                ).fetchall()

            if not rows:

                break

            fetched_count += len(
                rows
            )

            if remaining_limit is not None:

                remaining_limit -= len(
                    rows
                )

            chunk_context = self._build_chunk_context(
                rows,
                resume_table,
                source_user_table,
                source_student_table
            )

            resume_insert_data = []
            credentials_insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_resume_id = self._get_source_value(
                    row_dict,
                    resume_table,
                    "id"
                )
                last_source_id = source_resume_id

                try:

                    file_path = self._resume_path(
                        source_resume_id
                    )

                    if file_path in existing_credential_paths:

                        skipped_count += 1
                        skipped_existing += 1

                        continue

                    source_user_id = self._get_source_value(
                        row_dict,
                        resume_table,
                        "user_id"
                    )

                    source_user = chunk_context[
                        "users"
                    ].get(
                        source_user_id
                    )

                    source_username = (
                        source_user.get("username")
                        if source_user
                        else None
                    )

                    destination_user_uuid = (
                        destination_user_lookup.get(
                            self._normalize(source_username)
                        )
                        if source_username
                        else None
                    )

                    if not destination_user_uuid:

                        destination_user_uuid = self.DYNAMIC_VALUE
                        dynamic_user_count += 1

                    institution_uuid = user_institution_lookup.get(
                        destination_user_uuid
                    )

                    if not institution_uuid:

                        institution_uuid = self.DYNAMIC_VALUE
                        dynamic_institution_count += 1

                    institution_name = institution_name_by_uuid.get(
                        institution_uuid
                    )

                    source_gl_student = chunk_context[
                        "students_by_user_id"
                    ].get(
                        source_user_id
                    )

                    source_student_id = None
                    student_number = None
                    student_first_name = None
                    student_last_name = None
                    student_date_of_birth = None

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
                        row_dict,
                        resume_table,
                        source_resume_id,
                        destination_user_uuid,
                        user_enrollment_lookup
                    )

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            resume_table,
                            "uploaded_date"
                        )
                        or
                        datetime.utcnow()
                    )

                    resume_uuid = self._resume_uuid(
                        source_resume_id
                    )

                    file_name = (
                        self._get_source_value(
                            row_dict,
                            resume_table,
                            "resume_name"
                        )
                        or
                        "resume_data"
                    )

                    file_type = self._get_file_type(
                        file_name
                    )

                    status = self._map_status(
                        self._get_source_value(
                            row_dict,
                            resume_table,
                            "active"
                        )
                    )

                    resume_row = {
                        "uuid": resume_uuid,
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "user_id": destination_user_uuid,
                        "institution_id": institution_uuid,
                        "file_path": file_path,
                        "file_name": file_name,
                        "file_type": file_type,
                        "status": status,
                        "credential_path": file_path,
                        "credential_type": self.CREDENTIAL_TYPE,
                        "created_by": destination_user_uuid,
                        "updated_by": destination_user_uuid,
                        "deleted_by": None,
                        "enrollment_code": enrollment_code,
                        "generated_on": None,
                    }

                    resume_insert_data.append(
                        self._filter_to_table_columns(
                            resume_row,
                            resume_dest_table
                        )
                    )

                    credentials_row = {
                        "uuid": str(uuid.uuid4()),
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "user_id": destination_user_uuid,
                        "student_user_name": (
                            source_username
                            or
                            f"resume-{source_resume_id}"
                        ),
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
                        "credential_claim_status": (
                            self.CREDENTIAL_CLAIM_STATUS_NOT_CLAIMED
                        ),
                        "is_registered": 1,
                        "institution_id": institution_uuid,
                        "status": status,
                        "credential_type": self.CREDENTIAL_TYPE,
                        "credential_path": file_path,
                        "enrollment_code": enrollment_code,
                        "created_by": destination_user_uuid,
                        "updated_by": destination_user_uuid,
                        "deleted_by": None,
                        "resume": resume_uuid,
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

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "blockchain_hash",
                        self._get_source_value(
                            row_dict,
                            resume_table,
                            "blockchain_hash"
                        )
                    )

                    credentials_insert_data.append(
                        self._filter_to_table_columns(
                            credentials_row,
                            credentials_table
                        )
                    )

                    existing_credential_paths.add(
                        file_path
                    )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing resume source id "
                        f"{source_resume_id}: {error}"
                    )

            if not resume_insert_data:

                logger.info(
                    "Resume chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting resume chunk: "
                f"prepared={len(resume_insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                resume_result = dest_conn.execute(
                    insert(resume_dest_table),
                    resume_insert_data
                )

                credentials_result = dest_conn.execute(
                    insert(credentials_table),
                    credentials_insert_data
                )

            inserted_now = resume_result.rowcount or len(
                resume_insert_data
            )

            inserted_count += inserted_now
            prepared_count += len(
                resume_insert_data
            )

            logger.info(
                "Resume chunk inserted: "
                f"resume={inserted_now}, "
                f"credentials_all="
                f"{credentials_result.rowcount or len(credentials_insert_data)}, "
                f"inserted_total={inserted_count}"
            )

        if not prepared_count:

            logger.warning(
                "No valid resume records available for insertion"
            )

        logger.info(
            "Resume Migration summary: "
            f"inserted={inserted_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped={skipped_count}, "
            f"skipped_existing={skipped_existing}, "
            f"dynamic_user={dynamic_user_count}, "
            f"dynamic_institution={dynamic_institution_count}, "
            f"row_errors={row_error_count}"
        )

        return inserted_count

    def _get_batch_size(self):

        batch_size = int(
            self.config.get(
                "resume_migration_batch_size",
                self.config.get(
                    "batch_size",
                    self.DEFAULT_BATCH_SIZE
                )
            )
        )

        if batch_size < 1:

            batch_size = self.DEFAULT_BATCH_SIZE

        return min(
            batch_size,
            self.MAX_BATCH_SIZE
        )

    def _build_chunk_context(
        self,
        rows,
        resume_table,
        source_user_table,
        source_student_table
    ):

        user_ids = set()

        for row in rows:

            user_id = self._get_source_value(
                row._mapping,
                resume_table,
                "user_id"
            )

            if user_id:

                user_ids.add(
                    user_id
                )

        users = self._fetch_lookup_by_ids(
            self.source_engine,
            source_user_table,
            source_user_table.c.id,
            user_ids
        )

        students_by_user_id = {}

        if user_ids and "user_id" in source_student_table.c:

            with self.source_engine.connect() as conn:

                student_rows = conn.execute(
                    select(
                        source_student_table
                    ).where(
                        source_student_table.c.user_id.in_(
                            list(user_ids)
                        )
                    )
                ).fetchall()

            for row in student_rows:

                row_dict = dict(
                    row._mapping
                )
                user_id = row_dict.get(
                    "user_id"
                )

                if user_id not in students_by_user_id:

                    students_by_user_id[
                        user_id
                    ] = row_dict

        return {
            "users": users,
            "students_by_user_id": students_by_user_id,
        }

    def _fetch_lookup_by_ids(
        self,
        engine,
        table,
        id_column,
        ids
    ):

        if not ids:

            return {}

        with engine.connect() as conn:

            rows = conn.execute(
                select(
                    table
                ).where(
                    id_column.in_(
                        list(ids)
                    )
                )
            ).fetchall()

        return {
            row._mapping.get(id_column): dict(row._mapping)
            for row in rows
        }

    def _build_destination_user_lookup(
        self,
        users_table,
        auth_db_engine
    ):

        lookup = {}
        selected_columns = [
            users_table.c.uuid
        ]

        for column_name in [
            "user_name",
            "email"
        ]:

            if column_name in users_table.c:

                selected_columns.append(
                    users_table.c[column_name]
                )

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    *selected_columns
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping
            user_uuid = row_map.get(
                users_table.c.uuid
            )

            for column_name in [
                "user_name",
                "email"
            ]:

                if column_name not in users_table.c:

                    continue

                value = row_map.get(
                    users_table.c[column_name]
                )

                if value:

                    lookup[
                        self._normalize(value)
                    ] = user_uuid

        logger.info(
            f"Built {len(lookup)} resume destination user lookups"
        )

        return lookup

    def _build_user_institution_lookup(
        self,
        user_institution_table,
        auth_db_engine
    ):

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    user_institution_table.c.user_uuid,
                    user_institution_table.c.institution_uuid
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping
            user_uuid = row_map.get(
                user_institution_table.c.user_uuid
            )
            institution_uuid = row_map.get(
                user_institution_table.c.institution_uuid
            )

            if user_uuid and institution_uuid and user_uuid not in lookup:

                lookup[
                    user_uuid
                ] = institution_uuid

        return lookup

    def _build_user_enrollment_lookup(
        self,
        user_enrollments_table,
        auth_db_engine
    ):

        lookup = {}

        if (
            "user_uuid" not in user_enrollments_table.c
            or
            "enrollment_code" not in user_enrollments_table.c
        ):

            return lookup

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    user_enrollments_table.c.user_uuid,
                    user_enrollments_table.c.enrollment_code
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping
            user_uuid = row_map.get(
                user_enrollments_table.c.user_uuid
            )
            enrollment_code = row_map.get(
                user_enrollments_table.c.enrollment_code
            )

            if user_uuid and enrollment_code and user_uuid not in lookup:

                lookup[
                    user_uuid
                ] = enrollment_code

        return lookup

    def _build_institution_name_lookup(
        self,
        institutions_table,
        auth_db_engine
    ):

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    institutions_table.c.uuid,
                    institutions_table.c.name
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping
            institution_uuid = row_map.get(
                institutions_table.c.uuid
            )
            name = row_map.get(
                institutions_table.c.name
            )

            if institution_uuid:

                lookup[
                    institution_uuid
                ] = name

        return lookup

    def _load_existing_credential_paths(
        self,
        resume_dest_table,
        credentials_table
    ):

        existing_paths = set()

        with self.dest_engine.connect() as conn:

            if "credential_path" in resume_dest_table.c:

                rows = conn.execute(
                    select(
                        resume_dest_table.c.credential_path
                    )
                ).fetchall()

                for row in rows:

                    credential_path = row._mapping.get(
                        resume_dest_table.c.credential_path
                    )

                    if credential_path:

                        existing_paths.add(
                            credential_path
                        )
                        existing_paths.add(
                            self._canonical_credential_path(
                                credential_path
                            )
                        )

            if "credential_path" in credentials_table.c:

                rows = conn.execute(
                    select(
                        credentials_table.c.credential_path
                    ).where(
                        credentials_table.c.credential_type
                        == self.CREDENTIAL_TYPE
                    )
                ).fetchall()

                for row in rows:

                    credential_path = row._mapping.get(
                        credentials_table.c.credential_path
                    )

                    if credential_path:

                        existing_paths.add(
                            credential_path
                        )
                        existing_paths.add(
                            self._canonical_credential_path(
                                credential_path
                            )
                        )

        logger.info(
            f"Loaded {len(existing_paths)} existing resume paths."
        )

        return existing_paths

    def _get_enrollment_code(
        self,
        row_dict,
        resume_table,
        source_resume_id,
        destination_user_uuid,
        user_enrollment_lookup
    ):

        if destination_user_uuid != self.DYNAMIC_VALUE:

            return (
                user_enrollment_lookup.get(
                    destination_user_uuid
                )
                or
                ""
            )

        return str(
            source_resume_id
        )

    def _resume_uuid(
        self,
        source_resume_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:resume:{source_resume_id}"
            )
        )

    def _resume_path(
        self,
        source_resume_id
    ):

        return (
            f"{self.UPLOADS_PREFIX}/resume/"
            f"{source_resume_id}/resume_data"
        )

    def _canonical_credential_path(
        self,
        credential_path
    ):

        if not credential_path:

            return credential_path

        path = str(
            credential_path
        ).strip()

        legacy_prefix = f"{self.LEGACY_S3_BASE_URL}/"

        if path.startswith(
            legacy_prefix
        ):

            return (
                f"{self.UPLOADS_PREFIX}/"
                f"{path[len(legacy_prefix):]}"
            )

        return path

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

    def _set_if_column(
        self,
        row,
        table,
        column_name,
        value
    ) -> None:

        if column_name in table.c:

            row[column_name] = value

    def _map_status(
        self,
        active
    ):

        if isinstance(active, bool):

            return 2 if active else 1

        if isinstance(active, bytes):

            return 2 if active == b"\x01" else 1

        return 2 if str(active).strip().lower() in [
            "1",
            "true",
            "yes",
            "y",
            "\\x01",
        ] else 1

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

            return mime_mapping[
                ext
            ]

        mime_type, _ = mimetypes.guess_type(
            str(file_name)
        )

        return (
            mime_type
            or
            "application/octet-stream"
        )

    def _normalize(
        self,
        value
    ):

        if value is None:

            return ""

        return (
            str(value)
            .strip()
            .lower()
            .replace(" ", "")
        )
