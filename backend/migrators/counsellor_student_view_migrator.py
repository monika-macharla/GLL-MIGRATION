import json
import logging
import re
import uuid

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CounsellorStudentViewMigrator(BaseMigrator):

    SOURCE_STUDENT_TABLE = "gl_student"
    SOURCE_USER_TABLE = "gl_user"
    DESTINATION_TABLE = "counsellor_student_view"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 10000
    USER_UUID_NAMESPACE = uuid.UUID(
        "55fa6c2d-84d5-5bd8-b0f3-6a1d8b6c91f4"
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

    def migrate(self) -> int:

        logger.info(
            "Starting Counsellor Student View Migration..."
        )

        source_student_table = self._manual_reflect(
            self.SOURCE_STUDENT_TABLE,
            self.source_engine,
            self.metadata_source
        )

        source_user_table = self._manual_reflect(
            self.SOURCE_USER_TABLE,
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        batch_size = self._get_batch_size()
        remaining_limit = self.config.get("limit")
        last_source_id = self._get_start_after_id()

        if remaining_limit is not None:

            remaining_limit = int(
                remaining_limit
            )

        fetched_count = 0
        prepared_count = 0
        inserted_count = 0
        ignored_existing = 0
        skipped_no_counsellors = 0
        skipped_no_student_id = 0
        skipped_no_institution = 0
        skipped_missing_counsellor = 0
        batch_number = 0

        while True:

            fetch_size = batch_size

            if remaining_limit is not None:

                if remaining_limit <= 0:

                    break

                fetch_size = min(
                    fetch_size,
                    remaining_limit
                )

            query = (
                select(source_student_table)
                .where(
                    source_student_table.c.id > last_source_id
                )
                .where(
                    source_student_table.c.counsellor_user_ids.isnot(None)
                )
                .where(
                    source_student_table.c.counsellor_user_ids != ""
                )
                .order_by(
                    source_student_table.c.id
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

            batch_number += 1
            counsellor_ids = set()

            for row in rows:

                row_dict = row._mapping
                last_source_id = row_dict.get(
                    source_student_table.c.id
                )

                counsellor_ids.update(
                    self._parse_counsellor_ids(
                        row_dict.get(
                            source_student_table.c.counsellor_user_ids
                        )
                    )
                )

            counsellors_by_id = self._fetch_counsellors_by_id(
                source_user_table,
                counsellor_ids
            )

            insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_student_id = row_dict.get(
                    source_student_table.c.id
                )
                student_number = self._clean_string(
                    row_dict.get(
                        source_student_table.c.school_student_id
                    )
                )

                if not student_number:

                    skipped_no_student_id += 1
                    continue

                source_institution_id = row_dict.get(
                    source_student_table.c.institution_id
                )

                if not source_institution_id:

                    skipped_no_institution += 1
                    continue

                parsed_counsellor_ids = self._parse_counsellor_ids(
                    row_dict.get(
                        source_student_table.c.counsellor_user_ids
                    )
                )

                if not parsed_counsellor_ids:

                    skipped_no_counsellors += 1
                    continue

                for counsellor_source_id in parsed_counsellor_ids:

                    counsellor = counsellors_by_id.get(
                        counsellor_source_id
                    )

                    if not counsellor:

                        skipped_missing_counsellor += 1
                        continue

                    counsellor_uuid = self._user_uuid(
                        counsellor
                    )
                    now = datetime.utcnow()
                    mapped_row = {
                        "uuid": self._row_uuid(
                            source_student_id,
                            counsellor_source_id
                        ),
                        "created_at": now,
                        "updated_at": now,
                        "deleted_at": None,
                        "student_id": student_number,
                        "institution_id": self._institution_uuid(
                            source_institution_id
                        ),
                        "campus_id": None,
                        "counsellor_name": self._counsellor_name(
                            counsellor
                        ),
                        "status": "1",
                        "counsellor_id": counsellor_uuid,
                        "import_uuid": None,
                    }

                    insert_data.append(
                        self._filter_to_table_columns(
                            mapped_row,
                            destination_table
                        )
                    )

            if not insert_data:

                logger.info(
                    "Counsellor student chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )
                continue

            prepared_count += len(
                insert_data
            )

            logger.info(
                "Inserting counsellor_student_view chunk "
                f"{batch_number}: prepared={len(insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            statement = mysql_insert(
                destination_table
            ).prefix_with(
                "IGNORE"
            )

            with self.dest_engine.begin() as dest_conn:

                result = dest_conn.execute(
                    statement,
                    insert_data
                )

            inserted_in_chunk = result.rowcount or 0
            inserted_count += inserted_in_chunk
            ignored_existing += (
                len(insert_data)
                - inserted_in_chunk
            )

        logger.info(
            "Counsellor Student View Migration Summary: "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"skipped_no_counsellors={skipped_no_counsellors}, "
            f"skipped_no_student_id={skipped_no_student_id}, "
            f"skipped_no_institution={skipped_no_institution}, "
            f"skipped_missing_counsellor={skipped_missing_counsellor}"
        )

        return inserted_count

    def _fetch_counsellors_by_id(
        self,
        source_user_table,
        counsellor_ids
    ):

        if not counsellor_ids:

            return {}

        lookup = {}

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                select(source_user_table).where(
                    source_user_table.c.id.in_(
                        list(counsellor_ids)
                    )
                )
            ).fetchall()

        for row in rows:

            row_dict = dict(
                row._mapping
            )
            lookup[
                int(row_dict["id"])
            ] = row_dict

        return lookup

    def _parse_counsellor_ids(
        self,
        value
    ):

        if value is None:

            return []

        if isinstance(value, bytes):

            value = value.decode(
                "utf-8",
                errors="ignore"
            )

        text = str(value).strip()

        if not text:

            return []

        parsed = None

        try:

            parsed = json.loads(
                text
            )

        except (TypeError, ValueError):

            parsed = re.split(
                r"[,\\s]+",
                text.strip("[]")
            )

        if not isinstance(parsed, list):

            parsed = [parsed]

        ids = []

        for item in parsed:

            try:

                ids.append(
                    int(str(item).strip().strip('"').strip("'"))
                )

            except (TypeError, ValueError):

                continue

        return ids

    def _user_uuid(
        self,
        source_user
    ):

        source_jhi_user_id = source_user.get(
            "user_id"
        )

        if source_jhi_user_id is not None:

            return self._stable_user_uuid(
                "gl_user.user_id",
                source_jhi_user_id
            )

        return self._stable_user_uuid(
            "gl_user.id",
            source_user.get("id")
        )

    def _stable_user_uuid(
        self,
        key_type,
        key_value
    ):

        return str(
            uuid.uuid5(
                self.USER_UUID_NAMESPACE,
                f"{key_type}:{key_value}"
            )
        )

    def _institution_uuid(
        self,
        source_institution_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:institution:{source_institution_id}"
            )
        )

    def _row_uuid(
        self,
        source_student_id,
        source_counsellor_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    "gll:counsellor-student-view:"
                    f"{source_student_id}:{source_counsellor_id}"
                )
            )
        )

    def _counsellor_name(
        self,
        source_user
    ):

        name = " ".join(
            str(value).strip()
            for value in [
                source_user.get("first_name"),
                source_user.get("last_name")
            ]
            if value
            and
            str(value).strip()
        )

        return (
            name
            or
            self._clean_string(source_user.get("username"))
            or
            self._clean_string(source_user.get("email"))
        )

    def _clean_string(
        self,
        value
    ):

        if value is None:

            return None

        value = str(
            value
        ).strip()

        return value or None

    def _filter_to_table_columns(
        self,
        row,
        table
    ):

        return {
            key: value
            for key, value in row.items()
            if key in table.c
        }

    def _get_batch_size(self):

        configured = (
            self.config.get("counsellor_student_view_batch_size")
            or
            self.config.get("batch_size")
            or
            self.DEFAULT_BATCH_SIZE
        )

        try:

            configured = int(
                configured
            )

        except (TypeError, ValueError):

            configured = self.DEFAULT_BATCH_SIZE

        return max(
            1,
            min(configured, self.MAX_BATCH_SIZE)
        )

    def _get_start_after_id(self):

        configured = (
            self.config.get("counsellor_student_view_start_after_id")
            or
            self.config.get("start_after_id")
            or
            0
        )

        try:

            return max(
                0,
                int(configured)
            )

        except (TypeError, ValueError):

            return 0
