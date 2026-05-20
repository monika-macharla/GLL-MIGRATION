import logging
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
    INSERT_CHUNK_SIZE = 10

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
        self._source_user_uuid_cache = {}

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

        self._destination_user_lookup = (
            self._build_destination_user_lookup(
                users_table
            )
        )

        query = select(
            source_table
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
            f"Found {len(rows)} FERPA records"
        )

        insert_data = []

        for index, row in enumerate(
            rows,
            start=1
        ):

            try:

                row_dict = row._mapping

                source_user_id = self._get_source_value(
                    row_dict,
                    source_table,
                    "user_id"
                )

                student_uuid = self._resolve_user_uuid(
                    source_user_id
                )

                if not student_uuid:

                    logger.warning(
                        f"Skipping FERPA row {index}: "
                        f"could not resolve user_id={source_user_id}"
                    )

                    continue

                signed_date = self._get_source_value(
                    row_dict,
                    source_table,
                    "ferpa_terms_agreed_date"
                )

                created_at = signed_date or datetime.utcnow()

                mapped_row = {
                    "uuid": str(uuid.uuid4()),
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
                    "ferpa_evidence_signature": self._get_source_value(
                        row_dict,
                        source_table,
                        "ferpa_signature"
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

                logger.exception(
                    f"Failed processing FERPA row "
                    f"{index}: {error}"
                )

        if not insert_data:

            logger.warning(
                "No valid FERPA records available for insertion"
            )

            return 0

        inserted_count = 0

        for chunk_start in range(
            0,
            len(insert_data),
            self.INSERT_CHUNK_SIZE
        ):

            chunk = insert_data[
                chunk_start:chunk_start + self.INSERT_CHUNK_SIZE
            ]

            chunk_end = chunk_start + len(
                chunk
            )

            logger.info(
                "Inserting FERPA rows "
                f"{chunk_start + 1}-{chunk_end} "
                f"of {len(insert_data)}"
            )

            with self.dest_engine.begin() as dest_conn:

                result = dest_conn.execute(
                    insert(destination_table),
                    chunk
                )

            inserted_count += result.rowcount or len(
                chunk
            )

        logger.info(
            "FERPA Migration summary: "
            f"inserted={inserted_count}, "
            f"prepared={len(insert_data)}"
        )

        return inserted_count

    def _resolve_user_uuid(
        self,
        source_user_id
    ):

        if not source_user_id:

            return None

        if source_user_id in self._source_user_uuid_cache:

            return self._source_user_uuid_cache[
                source_user_id
            ]

        source_user = self.fetch_one_by_column(
            self.source_engine,
            "gl_user",
            "id",
            source_user_id
        )

        if not source_user:

            source_user = self.fetch_one_by_column(
                self.source_engine,
                "jhi_user",
                "id",
                source_user_id
            )

        source_username = self._get_row_value(
            source_user,
            "username",
            "login",
            "email"
        )

        if not source_username:

            self._source_user_uuid_cache[
                source_user_id
            ] = None

            return None

        destination_user_uuid = self._destination_user_lookup.get(
            self._normalize(source_username)
        )

        self._source_user_uuid_cache[
            source_user_id
        ] = destination_user_uuid

        return destination_user_uuid

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

    def _get_row_value(
        self,
        row,
        *column_names
    ):

        if not row:

            return None

        for column_name in column_names:

            value = row.get(
                column_name
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

        return str(value).strip().lower()
