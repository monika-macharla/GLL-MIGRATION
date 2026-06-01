import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class ScholarshipActivityMigrator(BaseMigrator):

    SOURCE_TABLE = "scholarship_activity"
    DESTINATION_TABLE = "scholarship_user_activity"
    INSERT_CHUNK_SIZE = 10000
    UUID_NAMESPACE = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "gll-migration:scholarship_activity"
    )

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
        self._local_scholarship_lookup = {}
        self._nspa_scholarship_lookup = {}

    def migrate(self) -> int:

        logger.info(
            "Starting Scholarship Activity Migration..."
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

        source_users_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        destination_users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        local_scholarships_table = self._manual_reflect(
            "local_scholarships",
            self.dest_engine,
            self.metadata_dest
        )

        nspa_scholarships_table = self._manual_reflect(
            "nspa_scholarships",
            self.dest_engine,
            self.metadata_dest
        )

        logger.info(
            f"scholarship_activity source columns: "
            f"{source_table.columns.keys()}"
        )

        logger.info(
            f"scholarship_user_activity destination columns: "
            f"{destination_table.columns.keys()}"
        )

        self._destination_user_lookup = (
            self._build_destination_user_lookup(
                destination_users_table
            )
        )

        self._local_scholarship_lookup = (
            self._build_scholarship_lookup(
                local_scholarships_table
            )
        )

        self._nspa_scholarship_lookup = (
            self._build_scholarship_lookup(
                nspa_scholarships_table
            )
        )

        existing_uuids = self._load_existing_uuids(
            destination_table
        )

        inserted_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        skipped_existing = 0
        skipped_missing_destination_user = 0
        row_error_count = 0

        last_source_id = 0
        remaining_limit = self.config.get(
            "limit"
        )

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
                    source_users_table.c.email
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

                    source_id = self._get_source_value(
                        row_dict,
                        source_table,
                        "id"
                    )

                    last_source_id = source_id

                    destination_uuid = self._make_destination_uuid(
                        source_id
                    )

                    if destination_uuid in existing_uuids:

                        skipped_count += 1
                        skipped_existing += 1

                        continue

                    source_username = self._get_source_value(
                        row_dict,
                        source_users_table,
                        "username"
                    )

                    source_email = self._get_source_value(
                        row_dict,
                        source_users_table,
                        "email"
                    )

                    user_uuid = self._lookup_destination_user_uuid(
                        source_username,
                        source_email
                    )

                    if not user_uuid:

                        skipped_count += 1
                        skipped_missing_destination_user += 1

                        continue

                    program_reference_id = self._build_program_reference_id(
                        self._get_source_value(
                            row_dict,
                            source_table,
                            "scholarship_name"
                        ),
                        self._get_source_value(
                            row_dict,
                            source_table,
                            "sponsor_name"
                        )
                    )

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            source_table,
                            "date_visited"
                        )
                        or
                        datetime.utcnow()
                    )

                    updated_at = created_at

                    local_scholarship_uuid = (
                        self._local_scholarship_lookup.get(
                            self._normalize_reference(
                                program_reference_id
                            )
                        )
                    )

                    nspa_scholarship_uuid = (
                        self._nspa_scholarship_lookup.get(
                            self._normalize_reference(
                                program_reference_id
                            )
                        )
                    )

                    mapped_row = {
                        "uuid": destination_uuid,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "deleted_at": None,
                        "program_reference_id": program_reference_id,
                        "application_type": self._application_type(
                            self._get_source_value(
                                row_dict,
                                source_table,
                                "scholarship_applied"
                            ),
                            self._get_source_value(
                                row_dict,
                                source_table,
                                "prompt"
                            )
                        ),
                        "user_uuid": user_uuid,
                        "institution_uuid": None,
                        "local_scholarship_uuid": local_scholarship_uuid,
                        "nspa_scholarship_uuid": nspa_scholarship_uuid,
                    }

                    insert_data.append(
                        self._filter_to_table_columns(
                            mapped_row,
                            destination_table
                        )
                    )

                    existing_uuids.add(
                        destination_uuid
                    )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing scholarship activity source id "
                        f"{last_source_id}: {error}"
                    )

            if not insert_data:

                logger.info(
                    "Scholarship activity chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting scholarship activity chunk: "
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
                "Scholarship activity chunk inserted: "
                f"inserted_now={inserted_now}, "
                f"inserted_total={inserted_count}"
            )

        logger.info(
            "Scholarship Activity Migration summary: "
            f"inserted={inserted_count}, "
            f"skipped={skipped_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped_existing={skipped_existing}, "
            f"skipped_missing_destination_user="
            f"{skipped_missing_destination_user}, "
            f"row_errors={row_error_count}"
        )

        return inserted_count

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
                        self._normalize_user(value)
                    ] = user_uuid

        logger.info(
            f"Built {len(lookup)} destination scholarship activity "
            "user lookups"
        )

        return lookup

    def _build_scholarship_lookup(
        self,
        scholarship_table
    ):

        lookup = {}

        if "program_reference_id" not in scholarship_table.c:

            return lookup

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    scholarship_table.c.uuid,
                    scholarship_table.c.program_reference_id
                ).where(
                    scholarship_table.c.deleted_at.is_(None)
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping

            program_reference_id = row_map.get(
                scholarship_table.c.program_reference_id
            )

            scholarship_uuid = row_map.get(
                scholarship_table.c.uuid
            )

            if program_reference_id and scholarship_uuid:

                lookup[
                    self._normalize_reference(
                        program_reference_id
                    )
                ] = scholarship_uuid

        logger.info(
            "Built "
            f"{len(lookup)} scholarship lookups from "
            f"{scholarship_table.name}"
        )

        return lookup

    def _load_existing_uuids(
        self,
        destination_table
    ):

        existing_uuids = set()

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    destination_table.c.uuid
                )
            ).fetchall()

        for row in rows:

            activity_uuid = row._mapping.get(
                destination_table.c.uuid
            )

            if activity_uuid:

                existing_uuids.add(
                    activity_uuid
                )

        logger.info(
            "Loaded "
            f"{len(existing_uuids)} existing scholarship_user_activity "
            "uuid values for idempotent reruns."
        )

        return existing_uuids

    def _lookup_destination_user_uuid(
        self,
        username,
        email
    ):

        for value in [
            username,
            email
        ]:

            normalized = self._normalize_user(
                value
            )

            if normalized in self._destination_user_lookup:

                return self._destination_user_lookup[
                    normalized
                ]

        return None

    def _make_destination_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                self.UUID_NAMESPACE,
                str(source_id)
            )
        )

    def _build_program_reference_id(
        self,
        scholarship_name,
        sponsor_name
    ):

        scholarship_name = self._clean_value(
            scholarship_name
        )

        sponsor_name = self._clean_value(
            sponsor_name
        )

        if scholarship_name and sponsor_name:

            return (
                f"{scholarship_name} | {sponsor_name}"
            )[:500]

        if scholarship_name:

            return str(
                scholarship_name
            )[:500]

        if sponsor_name:

            return str(
                sponsor_name
            )[:500]

        return "UNKNOWN"

    def _application_type(
        self,
        scholarship_applied,
        prompt
    ):

        if self._to_bool(
            scholarship_applied
        ):

            return "APPLIED"

        if self._to_bool(
            prompt
        ):

            return "PROMPT"

        return "VISITED"

    def _to_bool(
        self,
        value
    ):

        if isinstance(value, bytes):

            return value not in [
                b"",
                b"\x00"
            ]

        return bool(
            value
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

    def _clean_value(
        self,
        value
    ):

        if isinstance(value, str) and value.strip().lower() in [
            "null",
            "none",
            ""
        ]:

            return None

        return value

    def _normalize_user(
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

    def _normalize_reference(
        self,
        value
    ):

        if value is None:

            return ""

        return (
            str(value)
            .strip()
            .lower()
        )
