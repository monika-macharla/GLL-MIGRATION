import json
import logging
import mimetypes
import uuid

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import (
    insert,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class DigitalBadgesMigrator(BaseMigrator):

    SOURCE_TABLE = "badge"
    BADGES_TABLE = "credentials_digital_badges"
    BADGE_INFO_TABLE = "credentials_digital_badge_info"
    CREDENTIALS_TABLE = "credentials_all"
    CREDENTIAL_TYPE = 3
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
            "Starting Digital Badges Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        badge_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

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

        badges_table = self._manual_reflect(
            self.BADGES_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        badge_info_table = self._manual_reflect(
            self.BADGE_INFO_TABLE,
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
            f"badge source columns: {badge_table.columns.keys()}"
        )
        logger.info(
            f"credentials_digital_badges columns: "
            f"{badges_table.columns.keys()}"
        )
        logger.info(
            f"credentials_digital_badge_info columns: "
            f"{badge_info_table.columns.keys()}"
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
            badges_table,
            credentials_table
        )

        inserted_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        skipped_existing = 0
        skipped_inactive = 0
        skipped_duplicate_badge = 0
        dynamic_user_count = 0
        dynamic_issuer_count = 0
        dynamic_institution_count = 0
        row_error_count = 0
        migrated_badge_keys = set()

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
                        badge_table
                    )
                    .where(
                        badge_table.c.id > last_source_id
                    )
                    .order_by(
                        badge_table.c.id
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
                badge_table,
                source_student_table,
                source_user_table
            )

            badge_insert_data = []
            badge_info_insert_data = []
            credentials_insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_badge_id = self._get_source_value(
                    row_dict,
                    badge_table,
                    "id"
                )
                last_source_id = source_badge_id

                try:

                    if not self._should_migrate_badge(
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "active"
                        ),
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "revoked"
                        )
                    ):

                        skipped_count += 1
                        skipped_inactive += 1

                        continue

                    credential_path = self._badge_pdf_path(
                        source_badge_id
                    )

                    if credential_path in existing_credential_paths:

                        skipped_count += 1
                        skipped_existing += 1

                        continue

                    source_student_id = self._get_source_value(
                        row_dict,
                        badge_table,
                        "student_id"
                    )

                    source_gl_student = chunk_context[
                        "students"
                    ].get(
                        source_student_id
                    )

                    source_user_id = (
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "user_id"
                        )
                        or
                        (
                            source_gl_student.get("user_id")
                            if source_gl_student
                            else None
                        )
                    )

                    badge_key = self._source_badge_key(
                        row_dict,
                        badge_table,
                        source_user_id,
                        source_student_id,
                        source_badge_id
                    )

                    if badge_key in migrated_badge_keys:

                        skipped_count += 1
                        skipped_duplicate_badge += 1

                        continue

                    migrated_badge_keys.add(
                        badge_key
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

                    issuer_source_user = chunk_context[
                        "users"
                    ].get(
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "issuer_id"
                        )
                    )

                    issuer_username = (
                        issuer_source_user.get("username")
                        if issuer_source_user
                        else None
                    )

                    created_by_uuid = (
                        destination_user_lookup.get(
                            self._normalize(issuer_username)
                        )
                        if issuer_username
                        else None
                    )

                    if not created_by_uuid:

                        created_by_uuid = self.DYNAMIC_VALUE
                        dynamic_issuer_count += 1

                    institution_uuid = user_institution_lookup.get(
                        destination_user_uuid
                    )

                    if not institution_uuid:

                        institution_uuid = self.DYNAMIC_VALUE
                        dynamic_institution_count += 1

                    institution_name = institution_name_by_uuid.get(
                        institution_uuid
                    )

                    source_image = self._get_source_value(
                        row_dict,
                        badge_table,
                        "image"
                    )

                    file_name = (
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "badge_file_name"
                        )
                        or
                        self._extract_file_name(source_image)
                    )

                    file_type = self._get_file_type(
                        file_name
                    )

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "issued_on"
                        )
                        or
                        datetime.utcnow()
                    )

                    badge_uuid = self._badge_uuid(
                        source_badge_id
                    )

                    enrollment_code = self._get_enrollment_code(
                        row_dict,
                        badge_table,
                        source_badge_id,
                        destination_user_uuid,
                        user_enrollment_lookup
                    )

                    status = self._map_status(
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "active"
                        ),
                        self._get_source_value(
                            row_dict,
                            badge_table,
                            "revoked"
                        )
                    )

                    issuer_name = (
                        self._issuer_name_from_details(
                            self._get_source_value(
                                row_dict,
                                badge_table,
                                "badge_issuer_details"
                            )
                        )
                        or
                        institution_name
                    )

                    assertion_json = self._get_source_value(
                        row_dict,
                        badge_table,
                        "assertion_json"
                    )

                    badge_row = {
                        "uuid": badge_uuid,
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "user_id": destination_user_uuid,
                        "institution_id": institution_uuid,
                        "file_path": credential_path,
                        "file_name": file_name,
                        "file_type": file_type,
                        "badge_json": self._badge_json_value(
                            assertion_json
                        ),
                        "status": status,
                        "credential_path": credential_path,
                        "credential_type": self.CREDENTIAL_TYPE,
                        "created_by": created_by_uuid,
                        "updated_by": created_by_uuid,
                        "deleted_by": None,
                        "enrollment_code": enrollment_code,
                        "generated_on": None,
                    }

                    badge_insert_data.append(
                        self._filter_to_table_columns(
                            badge_row,
                            badges_table
                        )
                    )

                    badge_info_row = {
                        "uuid": self._badge_info_uuid(
                            source_badge_id
                        ),
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "badge_id": badge_uuid,
                        "badgeId": badge_uuid,
                        "badge_name": self._get_source_value(
                            row_dict,
                            badge_table,
                            "badge_name"
                        ),
                        "badge_description": self._get_source_value(
                            row_dict,
                            badge_table,
                            "description"
                        ),
                        "earning_criteria": self._get_source_value(
                            row_dict,
                            badge_table,
                            "criteria"
                        ),
                        "issuer_name": issuer_name,
                        "expires_on": self._get_source_value(
                            row_dict,
                            badge_table,
                            "expires"
                        ),
                        "badge_image_url": source_image,
                        "pdf_path": credential_path,
                        "generated_on": None,
                    }

                    badge_info_insert_data.append(
                        self._filter_to_table_columns(
                            badge_info_row,
                            badge_info_table
                        )
                    )

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
                                badge_table,
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
                        "student_email": (
                            source_username
                            or
                            self._get_source_value(
                                row_dict,
                                badge_table,
                                "email"
                            )
                        ),
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
                        "created_by": created_by_uuid,
                        "updated_by": created_by_uuid,
                        "deleted_by": None,
                        "digital_badges": badge_uuid,
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
                        "badge_image": source_image,
                    }

                    self._set_if_column(
                        credentials_row,
                        credentials_table,
                        "blockchain_hash",
                        self._get_source_value(
                            row_dict,
                            badge_table,
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
                        credential_path
                    )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing badge source id "
                        f"{source_badge_id}: {error}"
                    )

            if not badge_insert_data:

                logger.info(
                    "Digital badge chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting digital badge chunk: "
                f"prepared={len(badge_insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                badge_result = dest_conn.execute(
                    insert(badges_table),
                    badge_insert_data
                )

                badge_info_result = dest_conn.execute(
                    insert(badge_info_table),
                    badge_info_insert_data
                )

                credentials_result = dest_conn.execute(
                    insert(credentials_table),
                    credentials_insert_data
                )

            inserted_now = badge_result.rowcount or len(
                badge_insert_data
            )

            inserted_count += inserted_now
            prepared_count += len(
                badge_insert_data
            )

            logger.info(
                "Digital badge chunk inserted: "
                f"badges={inserted_now}, "
                f"badge_info="
                f"{badge_info_result.rowcount or len(badge_info_insert_data)}, "
                f"credentials_all="
                f"{credentials_result.rowcount or len(credentials_insert_data)}, "
                f"inserted_total={inserted_count}"
            )

        if not prepared_count:

            logger.warning(
                "No valid digital badge records available for insertion"
            )

        logger.info(
            "Digital Badges Migration summary: "
            f"inserted={inserted_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped={skipped_count}, "
            f"skipped_existing={skipped_existing}, "
            f"skipped_inactive={skipped_inactive}, "
            f"skipped_duplicate_badge={skipped_duplicate_badge}, "
            f"dynamic_user={dynamic_user_count}, "
            f"dynamic_issuer={dynamic_issuer_count}, "
            f"dynamic_institution={dynamic_institution_count}, "
            f"row_errors={row_error_count}"
        )

        return inserted_count

    def _get_batch_size(self):

        batch_size = int(
            self.config.get(
                "digital_badges_migration_batch_size",
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
        badge_table,
        source_student_table,
        source_user_table
    ):

        student_ids = set()
        user_ids = set()

        for row in rows:

            row_dict = row._mapping

            student_id = self._get_source_value(
                row_dict,
                badge_table,
                "student_id"
            )
            user_id = self._get_source_value(
                row_dict,
                badge_table,
                "user_id"
            )
            issuer_id = self._get_source_value(
                row_dict,
                badge_table,
                "issuer_id"
            )

            if student_id:

                student_ids.add(
                    student_id
                )

            if user_id:

                user_ids.add(
                    user_id
                )

            if issuer_id:

                user_ids.add(
                    issuer_id
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

        return {
            "students": students,
            "users": users,
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
            f"Built {len(lookup)} digital badge user lookups"
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
        badges_table,
        credentials_table
    ):

        existing_paths = set()

        with self.dest_engine.connect() as conn:

            if "credential_path" in badges_table.c:

                rows = conn.execute(
                    select(
                        badges_table.c.credential_path
                    )
                ).fetchall()

                for row in rows:

                    credential_path = row._mapping.get(
                        badges_table.c.credential_path
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
        row_dict,
        badge_table,
        source_badge_id,
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

        badge_id = self._get_source_value(
            row_dict,
            badge_table,
            "badge_id",
            "id"
        )

        return (
            str(badge_id)
            if badge_id is not None
            else
            str(source_badge_id)
        )

    def _should_migrate_badge(
        self,
        active,
        revoked
    ):

        return (
            self._is_truthy(active)
            and
            not self._is_truthy(revoked)
        )

    def _source_badge_key(
        self,
        row_dict,
        badge_table,
        source_user_id,
        source_student_id,
        source_badge_id
    ):

        badge_id = self._get_source_value(
            row_dict,
            badge_table,
            "badge_id"
        )

        if badge_id:

            owner_id = (
                source_user_id
                or
                source_student_id
                or
                ""
            )

            return (
                str(owner_id),
                self._normalize(badge_id)
            )

        return (
            "source",
            str(source_badge_id)
        )

    def _badge_uuid(
        self,
        source_badge_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:badge:{source_badge_id}"
            )
        )

    def _badge_info_uuid(
        self,
        source_badge_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:badge-info:{source_badge_id}"
            )
        )

    def _badge_pdf_path(
        self,
        source_badge_id
    ):

        return (
            f"{self.UPLOADS_PREFIX}/badges/{source_badge_id}/pdf_badge"
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

    def _fallback_student_user_name(
        self,
        row_dict,
        badge_table,
        source_student_id
    ):

        if source_student_id:

            return str(
                source_student_id
            )

        email = self._get_source_value(
            row_dict,
            badge_table,
            "email"
        )

        if email:

            return str(
                email
            )

        source_badge_id = self._get_source_value(
            row_dict,
            badge_table,
            "id"
        )

        return f"badge-{source_badge_id}"

    def _issuer_name_from_details(
        self,
        value
    ):

        parsed = self._json_value(
            value
        )

        if isinstance(parsed, dict):

            return (
                parsed.get("name")
                or
                parsed.get("issuer_name")
            )

        return None

    def _json_value(
        self,
        value
    ):

        if value is None:

            return None

        if isinstance(value, dict):

            return value

        text = str(value).strip()

        if not text:

            return None

        try:

            return json.loads(
                text
            )

        except Exception:

            return None

    def _badge_json_value(
        self,
        assertion_json
    ):

        return self._json_value(
            assertion_json
        )

    def _map_status(
        self,
        active,
        revoked
    ):

        if self._is_truthy(revoked):

            return 2

        if active is None:

            return 2

        return 2

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
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".json": "application/json",
            ".pdf": "application/pdf",
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
