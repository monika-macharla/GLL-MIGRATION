import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    exists,
    inspect,
    insert,
    or_,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class TranscriptMigrator(BaseMigrator):

    SOURCE_TABLE = "credential"
    SOURCE_TRANSCRIPT_TABLES = {
        "highschool": "hs_transcript",
        "communitycollege": "cc_transcript",
        "fouryear": "4yr_transcript",
    }
    SOURCE_TRANSCRIPT_PATHS = {
        "highschool": ("highschool", "pdf_transcript_student"),
        "communitycollege": ("community", "pdf_transcript_student"),
        "fouryear": ("fouryear", "pdf_transcript"),
    }
    TRANSCRIPT_TABLE_CANDIDATES = [
        "credentials_transcripts",
        "credentials_transcript",
    ]
    CREDENTIALS_TABLE = "credentials_all"
    CREDENTIAL_TYPE = 2
    CREDENTIAL_CLAIM_STATUS_NOT_CLAIMED = 2
    S3_BASE_URL = "https://greenlightlocker-com.s3.us-west-2.amazonaws.com"
    DYNAMIC_USER_VALUE = "DYNAMIC"
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
            "Starting Transcript Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        transcript_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        source_transcript_tables = {
            source_type: self._manual_reflect(
                table_name,
                self.source_engine,
                self.metadata_source
            )
            for source_type, table_name in (
                self.SOURCE_TRANSCRIPT_TABLES.items()
            )
        }

        source_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        source_user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        source_institution_table = self._manual_reflect(
            "institution",
            self.source_engine,
            self.metadata_source
        )

        source_enrollment_table = self._manual_reflect(
            "enrollment",
            self.source_engine,
            self.metadata_source
        )

        transcript_dest_table = self._manual_reflect(
            self._resolve_transcript_destination_table_name(),
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
            f"transcript source columns: "
            f"{transcript_table.columns.keys()}"
        )

        logger.info(
            f"{transcript_dest_table.name} destination columns: "
            f"{transcript_dest_table.columns.keys()}"
        )

        logger.info(
            f"credentials_all destination columns: "
            f"{credentials_table.columns.keys()}"
        )

        batch_size = self._get_batch_size()

        logger.info(
            f"Using transcript migration chunk size: {batch_size}"
        )

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

        institution_lookup = self._build_institution_lookup(
            source_institution_table,
            auth_institution_table,
            auth_db_engine
        )

        existing_credential_paths = self._load_existing_credential_paths(
            transcript_dest_table,
            credentials_table
        )

        inserted_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        nullable_missing_source_user = 0
        nullable_missing_destination_user = 0
        skipped_missing_institution = 0
        nullable_missing_credential_id = 0
        skipped_existing = 0
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
                        transcript_table
                    )
                    .where(
                        transcript_table.c.id > last_source_id,
                        self._transcript_source_filter(
                            transcript_table,
                            source_transcript_tables
                        )
                    )
                    .order_by(
                        transcript_table.c.id
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
                transcript_table,
                source_student_table,
                source_user_table,
                source_enrollment_table
            )

            transcript_insert_data = []
            credentials_insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_transcript_id = self._get_source_value(
                    row_dict,
                    transcript_table,
                    "id"
                )
                last_source_id = source_transcript_id

                try:

                    source_student_id = self._get_source_value(
                        row_dict,
                        transcript_table,
                        "student_id"
                    )

                    source_gl_student = chunk_context[
                        "students"
                    ].get(
                        source_student_id
                    )

                    source_gl_user_id = (
                        self._get_source_value(
                            row_dict,
                            transcript_table,
                            "user_id"
                        )
                        or
                        (
                            source_gl_student.get("user_id")
                            if source_gl_student
                            else None
                        )
                    )

                    source_gl_user = chunk_context[
                        "users"
                    ].get(
                        source_gl_user_id
                    )

                    source_username = (
                        source_gl_user.get("username")
                        if source_gl_user
                        else None
                    )

                    destination_user_uuid = None

                    if source_username:

                        destination_user_uuid = (
                            destination_user_lookup.get(
                                self._normalize(source_username)
                            )
                        )

                    if not destination_user_uuid:

                        if source_username:

                            nullable_missing_destination_user += 1

                        else:

                            nullable_missing_source_user += 1

                        destination_user_uuid = self.DYNAMIC_USER_VALUE

                    institution_uuid = (
                        institution_lookup.get(
                            self._get_source_value(
                                row_dict,
                                transcript_table,
                                "institution_id"
                            )
                        )
                        or
                        user_institution_lookup.get(
                            destination_user_uuid
                        )
                    )

                    if not institution_uuid:

                        skipped_count += 1
                        skipped_missing_institution += 1

                        continue

                    institution_name = self._get_institution_name(
                        auth_institution_table,
                        auth_db_engine,
                        institution_uuid
                    )

                    transcript_uuid = self._transcript_uuid(
                        source_transcript_id
                    )

                    credential_path = self._credential_path(
                        source_transcript_id,
                        row_dict,
                        transcript_table
                    )

                    if not credential_path:

                        nullable_missing_credential_id += 1

                    if (
                        credential_path
                        and
                        credential_path in existing_credential_paths
                    ):

                        skipped_count += 1
                        skipped_existing += 1

                        continue

                    student_number = None
                    student_first_name = None
                    student_last_name = None
                    student_date_of_birth = None

                    if source_gl_student:

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
                        transcript_table,
                        source_student_id,
                        destination_user_uuid,
                        user_enrollment_lookup,
                        chunk_context
                    )

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            transcript_table,
                            "issued_date"
                        )
                        or
                        self._get_source_value(
                            row_dict,
                            transcript_table,
                            "requested_time"
                        )
                        or
                        datetime.utcnow()
                    )

                    status = self._map_status(
                        self._get_source_value(
                            row_dict,
                            transcript_table,
                            "status"
                        )
                    )

                    transcript_row = {
                        "uuid": transcript_uuid,
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "user_id": destination_user_uuid,
                        "institution_id": institution_uuid,
                        "status": status,
                        "credential_type": self.CREDENTIAL_TYPE,
                        "credential_path": credential_path,
                        "enrollment_code": enrollment_code,
                        "created_by": destination_user_uuid,
                        "updated_by": destination_user_uuid,
                        "deleted_by": None,
                        "generated_on": None,
                    }

                    credentials_row = {
                        "uuid": str(uuid.uuid4()),
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "user_id": destination_user_uuid,
                        "student_user_name": (
                            source_username
                            or
                            self._fallback_student_user_name(
                                row_dict,
                                transcript_table,
                                source_student_id
                            )
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
                        "credential_path": credential_path,
                        "enrollment_code": enrollment_code,
                        "created_by": destination_user_uuid,
                        "updated_by": destination_user_uuid,
                        "deleted_by": None,
                        "transcripts": transcript_uuid,
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
                            transcript_table,
                            "blockchain_hash"
                        )
                    )

                    transcript_insert_data.append(
                        self._filter_to_table_columns(
                            transcript_row,
                            transcript_dest_table
                        )
                    )

                    credentials_insert_data.append(
                        self._filter_to_table_columns(
                            credentials_row,
                            credentials_table
                        )
                    )

                    if credential_path:

                        existing_credential_paths.add(
                            credential_path
                        )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing transcript source id "
                        f"{source_transcript_id}: {error}"
                    )

            if not transcript_insert_data:

                logger.info(
                    "Transcript chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting transcript chunk: "
                f"prepared={len(transcript_insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                transcript_result = dest_conn.execute(
                    insert(transcript_dest_table),
                    transcript_insert_data
                )

                credentials_result = dest_conn.execute(
                    insert(credentials_table),
                    credentials_insert_data
                )

            inserted_now = (
                transcript_result.rowcount
                or
                len(transcript_insert_data)
            )

            inserted_count += inserted_now
            prepared_count += len(
                transcript_insert_data
            )

            logger.info(
                "Transcript chunk inserted: "
                f"transcripts={inserted_now}, "
                f"credentials_all="
                f"{credentials_result.rowcount or len(credentials_insert_data)}, "
                f"inserted_total={inserted_count}"
            )

        if not prepared_count:

            logger.warning(
                "No valid transcript records available for insertion"
            )

        logger.info(
            "Transcript Migration summary: "
            f"inserted={inserted_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped={skipped_count}, "
            f"nullable_missing_source_user="
            f"{nullable_missing_source_user}, "
            f"nullable_missing_destination_user="
            f"{nullable_missing_destination_user}, "
            f"skipped_missing_institution="
            f"{skipped_missing_institution}, "
            f"nullable_missing_credential_id="
            f"{nullable_missing_credential_id}, "
            f"skipped_existing={skipped_existing}, "
            f"row_errors={row_error_count}"
        )

        return inserted_count

    def _resolve_transcript_destination_table_name(self):

        inspector = inspect(
            self.dest_engine
        )

        table_names = inspector.get_table_names()
        table_lookup = {
            table_name.lower(): table_name
            for table_name in table_names
        }

        for table_name in self.TRANSCRIPT_TABLE_CANDIDATES:

            if table_name.lower() in table_lookup:

                return table_lookup[
                    table_name.lower()
                ]

        raise ValueError(
            "Transcript destination table was not found. "
            f"Tried: {self.TRANSCRIPT_TABLE_CANDIDATES}"
        )

    def _get_batch_size(self):

        batch_size = int(
            self.config.get(
                "transcript_migration_batch_size",
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
        transcript_table,
        source_student_table,
        source_user_table,
        source_enrollment_table
    ):

        student_ids = set()
        user_ids = set()
        enrollment_ids = set()

        for row in rows:

            row_dict = row._mapping

            student_id = self._get_source_value(
                row_dict,
                transcript_table,
                "student_id"
            )

            user_id = self._get_source_value(
                row_dict,
                transcript_table,
                "user_id"
            )

            enrollment_id = self._get_source_value(
                row_dict,
                transcript_table,
                "enrollment_id"
            )

            if student_id:

                student_ids.add(
                    student_id
                )

            if user_id:

                user_ids.add(
                    user_id
                )

            if enrollment_id:

                enrollment_ids.add(
                    enrollment_id
                )

        students = self._fetch_lookup_by_ids(
            self.source_engine,
            source_student_table,
            source_student_table.c.id,
            student_ids
        )

        for student in students.values():

            student_user_id = student.get(
                "user_id"
            )

            if student_user_id:

                user_ids.add(
                    student_user_id
                )

        users = self._fetch_lookup_by_ids(
            self.source_engine,
            source_user_table,
            source_user_table.c.id,
            user_ids
        )

        enrollments_by_id = self._fetch_lookup_by_ids(
            self.source_engine,
            source_enrollment_table,
            source_enrollment_table.c.id,
            enrollment_ids
        )

        enrollments_by_student_id = {}

        if student_ids and "student_id" in source_enrollment_table.c:

            with self.source_engine.connect() as conn:

                enrollment_rows = conn.execute(
                    select(
                        source_enrollment_table
                    ).where(
                        source_enrollment_table.c.student_id.in_(
                            list(student_ids)
                        )
                    )
                ).fetchall()

            for row in enrollment_rows:

                row_dict = dict(
                    row._mapping
                )

                student_id = row_dict.get(
                    "student_id"
                )

                if student_id not in enrollments_by_student_id:

                    enrollments_by_student_id[
                        student_id
                    ] = row_dict

        return {
            "students": students,
            "users": users,
            "enrollments_by_id": enrollments_by_id,
            "enrollments_by_student_id": enrollments_by_student_id,
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
            f"Built {len(lookup)} transcript destination user lookups"
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

        logger.info(
            f"Built {len(lookup)} transcript user institution lookups"
        )

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

        logger.info(
            f"Built {len(lookup)} transcript user enrollment lookups"
        )

        return lookup

    def _build_institution_lookup(
        self,
        source_institution_table,
        auth_institution_table,
        auth_db_engine
    ):

        source_lookup = {}

        with self.source_engine.connect() as source_conn:

            source_rows = source_conn.execute(
                select(
                    source_institution_table.c.id,
                    source_institution_table.c.name
                )
            ).fetchall()

        for row in source_rows:

            row_map = row._mapping
            source_lookup[
                row_map.get(source_institution_table.c.id)
            ] = self._normalize(
                row_map.get(source_institution_table.c.name)
            )

        destination_lookup = {}
        destination_name_by_uuid = {}

        with auth_db_engine.connect() as auth_conn:

            destination_rows = auth_conn.execute(
                select(
                    auth_institution_table.c.uuid,
                    auth_institution_table.c.name
                )
            ).fetchall()

        for row in destination_rows:

            row_map = row._mapping
            destination_uuid = row_map.get(
                auth_institution_table.c.uuid
            )
            destination_name = row_map.get(
                auth_institution_table.c.name
            )
            normalized_name = self._normalize(
                destination_name
            )

            if normalized_name:

                destination_lookup[
                    normalized_name
                ] = destination_uuid

            if destination_uuid:

                destination_name_by_uuid[
                    destination_uuid
                ] = destination_name

        self._institution_name_by_uuid = destination_name_by_uuid

        lookup = {}

        for source_id, source_name in source_lookup.items():

            destination_uuid = destination_lookup.get(
                source_name
            )

            if destination_uuid:

                lookup[
                    source_id
                ] = destination_uuid

        logger.info(
            f"Built {len(lookup)} transcript institution lookups"
        )

        return lookup

    def _load_existing_credential_paths(
        self,
        transcript_dest_table,
        credentials_table
    ):

        existing_paths = set()

        with self.dest_engine.connect() as conn:

            if "credential_path" in transcript_dest_table.c:

                rows = conn.execute(
                    select(
                        transcript_dest_table.c.credential_path
                    ).where(
                        transcript_dest_table.c.deleted_at.is_(None)
                    )
                ).fetchall()

                for row in rows:

                    credential_path = row._mapping.get(
                        transcript_dest_table.c.credential_path
                    )

                    if credential_path:

                        existing_paths.add(
                            credential_path
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

        logger.info(
            f"Loaded {len(existing_paths)} existing transcript "
            "credential paths for idempotent reruns."
        )

        return existing_paths

    def _get_institution_name(
        self,
        auth_institution_table,
        auth_db_engine,
        institution_uuid
    ):

        if hasattr(self, "_institution_name_by_uuid"):

            institution_name = self._institution_name_by_uuid.get(
                institution_uuid
            )

            if institution_name:

                return institution_name

        with auth_db_engine.connect() as conn:

            row = conn.execute(
                select(
                    auth_institution_table.c.name
                ).where(
                    auth_institution_table.c.uuid == institution_uuid
                )
            ).fetchone()

        if not row:

            return None

        return row._mapping.get(
            auth_institution_table.c.name
        )

    def _get_enrollment_code(
        self,
        row_dict,
        transcript_table,
        source_student_id,
        destination_user_uuid,
        user_enrollment_lookup,
        chunk_context
    ):

        source_enrollment_id = self._get_source_value(
            row_dict,
            transcript_table,
            "enrollment_id"
        )

        if source_enrollment_id:

            enrollment_code = self._extract_enrollment_code(
                chunk_context["enrollments_by_id"].get(
                    source_enrollment_id
                )
            )

            if enrollment_code:

                return enrollment_code

        if source_student_id:

            enrollment_code = self._extract_enrollment_code(
                chunk_context["enrollments_by_student_id"].get(
                    source_student_id
                )
            )

            if enrollment_code:

                return enrollment_code

        return (
            user_enrollment_lookup.get(
                destination_user_uuid
            )
            or
            ""
        )

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

    def _transcript_uuid(
        self,
        source_transcript_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:transcript:{source_transcript_id}"
            )
        )

    def _fallback_student_user_name(
        self,
        row_dict,
        transcript_table,
        source_student_id
    ):

        stu_identification = self._get_source_value(
            row_dict,
            transcript_table,
            "stu_identification"
        )

        if stu_identification:

            return str(
                stu_identification
            )

        if source_student_id:

            return str(
                source_student_id
            )

        source_transcript_id = self._get_source_value(
            row_dict,
            transcript_table,
            "id"
        )

        return f"transcript-{source_transcript_id}"

    def _credential_path(
        self,
        source_transcript_id,
        row_dict,
        transcript_table
    ):

        source_credential_id = self._get_source_value(
            row_dict,
            transcript_table,
            "id"
        )

        if not source_credential_id:

            logger.warning(
                "Transcript source id "
                f"{source_transcript_id} is missing credential id; "
                "credential_path will be NULL"
            )

            return None

        source_type = self._normalize(
            self._get_source_value(
                row_dict,
                transcript_table,
                "type"
            )
        )

        path_parts = self.SOURCE_TRANSCRIPT_PATHS.get(
            source_type
        )

        if not path_parts:

            logger.warning(
                "Transcript source id "
                f"{source_transcript_id} has unsupported "
                f"credential type {source_type}; "
                "credential_path will be NULL"
            )

            return None

        path_prefix, file_name = path_parts

        return (
            f"{self.S3_BASE_URL}/"
            f"{path_prefix}/"
            f"{source_credential_id}/"
            f"{file_name}"
        )

    def _transcript_source_filter(
        self,
        credential_table,
        source_transcript_tables
    ):

        filters = []

        for source_type, source_table in (
            source_transcript_tables.items()
        ):

            filters.append(
                (
                    credential_table.c.type == source_type
                )
                &
                exists(
                    select(
                        1
                    )
                    .select_from(
                        source_table
                    )
                    .where(
                        source_table.c.credential_id
                        == credential_table.c.id
                    )
                )
            )

        return or_(
            *filters
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

    def _map_status(
        self,
        status
    ) -> int:

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
            "generated"
        ]:

            return 3

        if normalized_status in [
            "inactive",
            "failed",
            "error",
            "rejected"
        ]:

            return 2

        return 1

    def _set_if_column(
        self,
        row,
        table,
        column_name,
        value
    ) -> None:

        if column_name in table.c:

            row[column_name] = value

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
