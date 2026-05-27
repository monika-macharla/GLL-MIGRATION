import logging
import os
import uuid

from datetime import datetime
from sqlalchemy import (
    insert,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class FerpaMigrator(BaseMigrator):

    SOURCE_TABLE = "ferpa"
    DESTINATION_TABLE = "ferpa"
    INSERT_CHUNK_SIZE = 10000
    DESTINATION_TEXT_LIMIT = 65000

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
        self._destination_user_lookup = {}
        self._signature_upload_count = 0
        self._signature_upload_failed_count = 0
        self._oversized_signature_without_storage_count = 0

    def migrate(self) -> int:

        logger.info(
            "Starting FERPA Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        logger.info(
            f"ferpa source columns: "
            f"{source_table.columns.keys()}"
        )

        logger.info(
            f"ferpa destination columns: "
            f"{destination_table.columns.keys()}"
        )

        users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        source_users_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        self._destination_user_lookup = (
            self._build_destination_user_lookup(
                users_table
            )
        )

        existing_student_uuids = (
            self._load_existing_student_uuids(
                destination_table
            )
        )

        logger.info(
            "Using FERPA insert chunk size: "
            f"{self.INSERT_CHUNK_SIZE}"
        )

        inserted_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        skipped_missing_destination_user = 0
        skipped_existing_student = 0
        row_error_count = 0
        self._signature_upload_count = 0
        self._signature_upload_failed_count = 0
        self._oversized_signature_without_storage_count = 0

        last_source_id = 0
        remaining_limit = self.config.get("limit")

        if remaining_limit:

            remaining_limit = int(
                remaining_limit
            )

        while True:

            fetch_size = self.INSERT_CHUNK_SIZE

            if remaining_limit is not None:

                if remaining_limit <= 0:

                    break

                fetch_size = min(
                    fetch_size,
                    remaining_limit
                )

            query = (
                select(
                    source_table,
                    source_users_table.c.username,
                    source_users_table.c.user_type
                )
                .select_from(
                    source_table.join(
                        source_users_table,
                        source_table.c.user_id
                        == source_users_table.c.id
                    )
                )
                .where(
                    source_table.c.id > last_source_id
                )
                .where(
                    source_users_table.c.username.is_not(None)
                )
                .where(
                    source_users_table.c.username != ""
                )
                .order_by(
                    source_table.c.id
                )
                .limit(
                    fetch_size
                )
            )

            with self.source_engine.connect() as source_conn:

                rows = source_conn.execute(
                    query
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

            insert_data = []

            for row in rows:

                try:

                    row_dict = row._mapping

                    last_source_id = self._get_source_value(
                        row_dict,
                        source_table,
                        "id"
                    )

                    source_username = self._get_source_value(
                        row_dict,
                        source_users_table,
                        "username"
                    )

                    student_uuid = self._destination_user_lookup.get(
                        self._normalize(source_username)
                    )

                    if not student_uuid:

                        skipped_count += 1
                        skipped_missing_destination_user += 1

                        continue

                    if student_uuid in existing_student_uuids:

                        skipped_count += 1
                        skipped_existing_student += 1

                        continue

                    signed_date = self._get_source_value(
                        row_dict,
                        source_table,
                        "ferpa_terms_agreed_date"
                    )

                    created_at = signed_date or datetime.utcnow()

                    ferpa_uuid = str(uuid.uuid4())

                    mapped_row = {
                        "uuid": ferpa_uuid,
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "student_uuid": student_uuid,
                        "is_signed": self._is_truthy(
                            self._get_source_value(
                                row_dict,
                                source_table,
                                "authorized"
                            )
                        ),
                        "ferpa_evidence_signature": (
                            self._map_signature(
                                row_dict,
                                source_table,
                                ferpa_uuid
                            )
                        ),
                        "signed_by": self._get_source_value(
                            row_dict,
                            source_table,
                            "signed_name"
                        ),
                        "signed_date": signed_date,
                        "created_by": student_uuid,
                        "updated_by": student_uuid,
                    }

                    insert_data.append(
                        self._filter_to_table_columns(
                            mapped_row,
                            destination_table
                        )
                    )

                except Exception as error:

                    row_error_count += 1

                    logger.exception(
                        "Failed processing FERPA source id "
                        f"{last_source_id}: {error}"
                    )

            if not insert_data:

                logger.info(
                    "FERPA chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting FERPA chunk: "
                f"prepared={len(insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                result = dest_conn.execute(
                    insert(destination_table),
                    insert_data
                )

            inserted_now = result.rowcount or len(
                insert_data
            )

            inserted_count += inserted_now
            prepared_count += len(
                insert_data
            )

            logger.info(
                "FERPA chunk inserted: "
                f"inserted_now={inserted_now}, "
                f"inserted_total={inserted_count}"
            )

        if not prepared_count:

            logger.warning(
                "No valid FERPA records available for insertion"
            )

        logger.info(
            "FERPA Migration summary: "
            f"inserted={inserted_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped={skipped_count}, "
            f"skipped_missing_destination_user="
            f"{skipped_missing_destination_user}, "
            f"skipped_existing_student={skipped_existing_student}, "
            f"row_errors={row_error_count}, "
            f"signature_uploads={self._signature_upload_count}, "
            f"signature_upload_failed="
            f"{self._signature_upload_failed_count}, "
            f"oversized_signature_without_storage="
            f"{self._oversized_signature_without_storage_count}"
        )

        return inserted_count

    def _map_signature(
        self,
        row,
        source_table,
        ferpa_uuid
    ):

        signature = self._get_source_value(
            row,
            source_table,
            "ferpa_signature"
        )

        if not signature:

            return None

        signature_text = str(
            signature
        )

        if len(signature_text) <= self.DESTINATION_TEXT_LIMIT:

            return signature_text

        if (
            not getattr(self.storage, "bucket_name", None)
            or not os.getenv("AWS_ACCESS_KEY_ID")
        ):

            self._oversized_signature_without_storage_count += 1

            return None

        uploaded_path = self.storage.upload_base64(
            signature_text,
            f"ferpa/{ferpa_uuid}.png"
        )

        if (
            uploaded_path
            and len(uploaded_path) <= self.DESTINATION_TEXT_LIMIT
        ):

            self._signature_upload_count += 1

            return uploaded_path

        self._signature_upload_failed_count += 1

        return None

    def _load_existing_student_uuids(
        self,
        destination_table
    ):

        existing_student_uuids = set()

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    destination_table.c.student_uuid
                ).where(
                    destination_table.c.deleted_at.is_(None)
                )
            ).fetchall()

        for row in rows:

            student_uuid = row._mapping.get(
                destination_table.c.student_uuid
            )

            if student_uuid:

                existing_student_uuids.add(
                    student_uuid
                )

        logger.info(
            "Loaded "
            f"{len(existing_student_uuids)} existing FERPA "
            "student_uuid values for idempotent reruns."
        )

        return existing_student_uuids

    def _build_destination_user_lookup(
        self,
        users_table
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

        with self.dest_engine.connect() as conn:

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
            f"Built {len(lookup)} destination FERPA user lookups"
        )

        return lookup

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

    def _is_truthy(
        self,
        value
    ):

        if isinstance(value, bool):

            return value

        if value is None:

            return False

        return str(value).strip().lower() in [
            "1",
            "true",
            "yes",
            "y",
            "signed",
            "authorized",
        ]

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
