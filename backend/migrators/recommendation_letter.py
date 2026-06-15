import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    inspect,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class RecommendationLetterMigrator(BaseMigrator):

    REQUEST_TABLE = "recommendation_request"
    LETTER_TABLE = "recommendation_letter"
    DESTINATION_TABLE = "credentials_recommendation_letters"
    CREDENTIALS_TABLE = "credentials_all"
    CREDENTIAL_TYPE = 4
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
            "Starting Recommendation Letter Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        request_table = self._manual_reflect(
            self.REQUEST_TABLE,
            self.source_engine,
            self.metadata_source
        )

        letter_table = self._manual_reflect(
            self.LETTER_TABLE,
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

        recommendation_dest_table = self._manual_reflect(
            self._get_recommendation_destination_table_name(),
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
            f"recommendation_request columns: "
            f"{request_table.columns.keys()}"
        )
        logger.info(
            f"recommendation_letter columns: "
            f"{letter_table.columns.keys()}"
        )
        logger.info(
            f"credentials_recommendation_letters columns: "
            f"{recommendation_dest_table.columns.keys()}"
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
            recommendation_dest_table,
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
                        request_table
                    )
                    .where(
                        request_table.c.id > last_source_id
                    )
                    .order_by(
                        request_table.c.id
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
                request_table,
                letter_table,
                source_user_table,
                source_student_table
            )

            recommendation_insert_data = []
            credentials_insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_request_id = self._get_source_value(
                    row_dict,
                    request_table,
                    "id"
                )
                last_source_id = source_request_id

                try:

                    source_letter = chunk_context[
                        "letters_by_reference_id"
                    ].get(
                        source_request_id
                    )
                    credential_path = self._recommendation_path(
                        source_request_id
                    )

                    if credential_path in existing_credential_paths:

                        skipped_count += 1
                        skipped_existing += 1

                        continue

                    source_user_id = self._get_source_value(
                        row_dict,
                        request_table,
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

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            request_table,
                            "date_of_request"
                        )
                        or
                        datetime.utcnow()
                    )

                    updated_at = (
                        self._get_source_value(
                            row_dict,
                            request_table,
                            "date_issued",
                            "date_of_initial_response"
                        )
                        or
                        created_at
                    )

                    issued_on = (
                        self._get_source_value(
                            row_dict,
                            request_table,
                            "date_issued"
                        )
                        or
                        created_at
                    )

                    enrollment_code = self._get_enrollment_code(
                        source_request_id,
                        destination_user_uuid,
                        user_enrollment_lookup
                    )

                    recommendation_uuid = self._recommendation_uuid(
                        source_request_id
                    )

                    recommender_full_name = self._recommender_full_name(
                        row_dict,
                        request_table,
                        source_letter
                    )

                    recommender_email = (
                        (
                            source_letter.get("issuer_email")
                            if source_letter
                            else None
                        )
                        or
                        self._get_source_value(
                            row_dict,
                            request_table,
                            "recommender_email"
                        )
                        or
                        self.DYNAMIC_VALUE
                    )

                    request_status = self._get_source_value(
                        row_dict,
                        request_table,
                        "request_status"
                    )

                    status = self._map_status(
                        request_status
                    )

                    is_confidential = (
                        1
                        if self._is_truthy(
                            self._get_source_value(
                                row_dict,
                                request_table,
                                "blind_recommendation_letter"
                            )
                        )
                        else
                        0
                    )

                    recommendation_row = {
                        "uuid": recommendation_uuid,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "deleted_at": None,
                        "recommender_full_name": (
                            recommender_full_name
                            or
                            self.DYNAMIC_VALUE
                        ),
                        "is_confidential": is_confidential,
                        "due_date_to_recommender": (
                            str(
                                self._get_source_value(
                                    row_dict,
                                    request_table,
                                    "issued_by_date"
                                )
                                or
                                ""
                            )
                        ),
                        "recommendation_type": 1,
                        "recommendation_input_type": 1,
                        "recommendation_input": (
                            str(request_status or "")
                        ),
                        "message_to_recommender": self._get_source_value(
                            row_dict,
                            request_table,
                            "personalized_message"
                        ),
                        "supporting_materials_path": None,
                        "supporting_materials_file_name": None,
                        "upload_letter": None,
                        "upload_letter_file_name": None,
                        "description": self._get_source_value(
                            row_dict,
                            request_table,
                            "request_status_reason"
                        ),
                        "user_id": destination_user_uuid,
                        "institution_id": institution_uuid,
                        "recommender_email": recommender_email,
                        "credential_type": self.CREDENTIAL_TYPE,
                        "status": status,
                        "credential_path": credential_path,
                        "created_by": destination_user_uuid,
                        "updated_by": destination_user_uuid,
                        "deleted_by": None,
                        "enrollment_code": enrollment_code,
                        "generated_on": None,
                    }

                    recommendation_insert_data.append(
                        self._filter_to_table_columns(
                            recommendation_row,
                            recommendation_dest_table
                        )
                    )

                    credentials_row = {
                        "uuid": str(uuid.uuid4()),
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "deleted_at": None,
                        "user_id": destination_user_uuid,
                        "student_user_name": (
                            source_username
                            or
                            f"recommendation-{source_request_id}"
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
                        "recommendation_letters": recommendation_uuid,
                        "generated_on": None,
                        "issued_on": str(
                            issued_on
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
                        (
                            source_letter.get("blockchain_hash")
                            if source_letter
                            else None
                        )
                    )

                    credentials_insert_data.append(
                        self._filter_to_table_columns(
                            credentials_row,
                            credentials_table
                        )
                    )

                    existing_credential_paths.add(
                        credential_path
                    )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing recommendation source id "
                        f"{source_request_id}: {error}"
                    )

            if not recommendation_insert_data:

                logger.info(
                    "Recommendation chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting recommendation chunk: "
                f"prepared={len(recommendation_insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                recommendation_result = dest_conn.execute(
                    insert(recommendation_dest_table),
                    recommendation_insert_data
                )

                credentials_result = dest_conn.execute(
                    insert(credentials_table),
                    credentials_insert_data
                )

            inserted_now = (
                recommendation_result.rowcount
                or
                len(recommendation_insert_data)
            )

            inserted_count += inserted_now
            prepared_count += len(
                recommendation_insert_data
            )

            logger.info(
                "Recommendation chunk inserted: "
                f"recommendations={inserted_now}, "
                f"credentials_all="
                f"{credentials_result.rowcount or len(credentials_insert_data)}, "
                f"inserted_total={inserted_count}"
            )

        if not prepared_count:

            logger.warning(
                "No valid recommendation records available for insertion"
            )

        logger.info(
            "Recommendation Migration summary: "
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

    def _get_recommendation_destination_table_name(self):

        inspector = inspect(
            self.dest_engine
        )

        table_names = set(
            inspector.get_table_names()
        )

        if self.DESTINATION_TABLE in table_names:

            return self.DESTINATION_TABLE

        raise ValueError(
            "Recommendation destination table not found: "
            f"{self.DESTINATION_TABLE}"
        )

    def _get_batch_size(self):

        batch_size = int(
            self.config.get(
                "recommendation_letter_migration_batch_size",
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
        request_table,
        letter_table,
        source_user_table,
        source_student_table
    ):

        request_ids = set()
        user_ids = set()

        for row in rows:

            row_dict = row._mapping
            request_id = self._get_source_value(
                row_dict,
                request_table,
                "id"
            )
            user_id = self._get_source_value(
                row_dict,
                request_table,
                "user_id"
            )

            if request_id:

                request_ids.add(
                    request_id
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

        letters_by_reference_id = {}

        if request_ids:

            with self.source_engine.connect() as conn:

                letter_rows = conn.execute(
                    select(
                        letter_table
                    ).where(
                        letter_table.c.reference_id.in_(
                            list(request_ids)
                        )
                    )
                ).fetchall()

            for row in letter_rows:

                row_dict = dict(
                    row._mapping
                )
                reference_id = row_dict.get(
                    "reference_id"
                )

                if reference_id not in letters_by_reference_id:

                    letters_by_reference_id[
                        reference_id
                    ] = row_dict

        return {
            "users": users,
            "students_by_user_id": students_by_user_id,
            "letters_by_reference_id": letters_by_reference_id,
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
            f"Built {len(lookup)} recommendation user lookups"
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
        recommendation_dest_table,
        credentials_table
    ):

        existing_paths = set()

        with self.dest_engine.connect() as conn:

            if "credential_path" in recommendation_dest_table.c:

                rows = conn.execute(
                    select(
                        recommendation_dest_table.c.credential_path
                    )
                ).fetchall()

                for row in rows:

                    credential_path = row._mapping.get(
                        recommendation_dest_table.c.credential_path
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

        return existing_paths

    def _get_enrollment_code(
        self,
        source_request_id,
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
            source_request_id
        )

    def _recommendation_uuid(
        self,
        source_request_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:recommendation-letter:{source_request_id}"
            )
        )

    def _recommendation_path(
        self,
        source_id
    ):

        return (
            f"{self.UPLOADS_PREFIX}/recommendationletter/"
            f"{source_id}/file"
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

    def _recommender_full_name(
        self,
        row_dict,
        request_table,
        source_letter
    ):

        if source_letter:

            full_name = self._join_name(
                source_letter.get("issuer_first_name"),
                source_letter.get("issuer_middle_name"),
                source_letter.get("issuer_last_name")
            )

            if full_name:

                return full_name

        return self._join_name(
            self._get_source_value(
                row_dict,
                request_table,
                "first_name"
            ),
            self._get_source_value(
                row_dict,
                request_table,
                "middle_name"
            ),
            self._get_source_value(
                row_dict,
                request_table,
                "last_name"
            )
        )

    def _join_name(
        self,
        *parts
    ):

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

        if normalized_status in [
            "accepted",
            "initial",
        ]:

            return 2

        return 1

    def _is_truthy(
        self,
        value
    ):

        if isinstance(value, bool):

            return value

        if isinstance(value, bytes):

            return value == b"\x01"

        return str(value).strip().lower() in [
            "1",
            "true",
            "yes",
            "y",
            "\\x01",
        ]

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
    ):

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
