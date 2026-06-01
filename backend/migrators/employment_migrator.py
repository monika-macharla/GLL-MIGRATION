import logging
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class EmploymentMigrator(BaseMigrator):

    SOURCE_TABLE = "employment_history"
    DESTINATION_TABLE = "employment"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 20000
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
            "Starting Employment Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        gl_user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        source_count = self._count_rows(
            self.source_engine,
            source_table
        )
        destination_count = self._count_rows(
            self.dest_engine,
            destination_table
        )

        logger.info(
            f"Source employment_history count: {source_count}"
        )
        logger.info(
            f"Destination employment current count: "
            f"{destination_count}"
        )

        batch_size = self._get_batch_size()
        remaining_limit = self.config.get("limit")

        if remaining_limit is not None:

            remaining_limit = int(
                remaining_limit
            )

        last_source_id = 0
        fetched_count = 0
        prepared_count = 0
        inserted_count = 0
        ignored_existing = 0
        missing_user = 0
        fallback_company = 0
        fallback_role = 0
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
                select(
                    source_table,
                    gl_user_table
                )
                .select_from(
                    source_table.outerjoin(
                        gl_user_table,
                        source_table.c.user_id
                        == gl_user_table.c.id
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
            batch_number += 1
            now = datetime.utcnow()

            for row in rows:

                row_dict = row._mapping
                source_id = row_dict.get(
                    source_table.c.id
                )
                last_source_id = source_id

                user_uuid = self._user_uuid(
                    row_dict,
                    source_table,
                    gl_user_table
                )

                if not user_uuid:

                    missing_user += 1
                    user_uuid = self._fallback_user_id(
                        row_dict,
                        source_table
                    )

                company_name = self._clean_string(
                    row_dict.get(
                        source_table.c.employer_name
                    )
                )

                if not company_name:

                    fallback_company += 1
                    company_name = "Unknown Employer"

                role = self._clean_string(
                    row_dict.get(
                        source_table.c.title
                    )
                )

                if not role:

                    fallback_role += 1
                    role = (
                        self._clean_string(
                            row_dict.get(
                                source_table.c.industry
                            )
                        )
                        or
                        "Unknown Role"
                    )

                mapped_row = {
                    "uuid": self._stable_uuid(
                        source_id
                    ),
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                    "user_id": self._truncate(
                        user_uuid,
                        255
                    ),
                    "company_name": self._truncate(
                        company_name,
                        255
                    ),
                    "role": self._truncate(
                        role,
                        255
                    ),
                    "from_date": row_dict.get(
                        source_table.c.from_date
                    ),
                    "to_date": row_dict.get(
                        source_table.c.to_date
                    ),
                    "is_current_company": (
                        1
                        if self._truthy(
                            row_dict.get(
                                source_table.c.current_working
                            )
                        )
                        else
                        0
                    ),
                    "created_by": self._truncate(
                        user_uuid,
                        255
                    ),
                    "updated_by": self._truncate(
                        user_uuid,
                        255
                    ),
                    "deleted_by": None,
                }

                insert_data.append(
                    self._filter_to_table_columns(
                        mapped_row,
                        destination_table
                    )
                )

            if insert_data:

                prepared_count += len(
                    insert_data
                )

                logger.info(
                    "Inserting employment chunk "
                    f"{batch_number}: prepared="
                    f"{len(insert_data)}, "
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

            if len(rows) < fetch_size:

                break

        logger.info(
            "Employment Migration Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"missing_user={missing_user}, "
            f"fallback_company={fallback_company}, "
            f"fallback_role={fallback_role}"
        )

        return inserted_count

    def _count_rows(
        self,
        engine,
        table
    ):

        with engine.connect() as conn:

            return conn.execute(
                select(func.count()).select_from(
                    table
                )
            ).scalar() or 0

    def _get_batch_size(self):

        configured = (
            self.config.get("employment_migration_batch_size")
            or
            self.config.get("batch_size")
            or
            self.config.get("chunk_size")
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
            min(
                configured,
                self.MAX_BATCH_SIZE
            )
        )

    def _user_uuid(
        self,
        row_dict,
        source_table,
        gl_user_table
    ):

        source_jhi_user_id = row_dict.get(
            gl_user_table.c.user_id
        )

        if source_jhi_user_id is not None:

            return str(
                uuid.uuid5(
                    self.USER_UUID_NAMESPACE,
                    f"gl_user.user_id:{source_jhi_user_id}"
                )
            )

        source_gl_user_id = (
            row_dict.get(
                gl_user_table.c.id
            )
            or
            row_dict.get(
                source_table.c.user_id
            )
        )

        if source_gl_user_id is None:

            return None

        return str(
            uuid.uuid5(
                self.USER_UUID_NAMESPACE,
                f"gl_user.id:{source_gl_user_id}"
            )
        )

    def _fallback_user_id(
        self,
        row_dict,
        source_table
    ):

        return (
            self._clean_string(
                row_dict.get(
                    source_table.c.user_id
                )
            )
            or
            "UNKNOWN-USER"
        )

    def _stable_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:employment:{source_id}"
            )
        )

    def _truthy(
        self,
        value
    ):

        if value is None:

            return False

        if isinstance(value, (bytes, bytearray)):

            return value != b"\x00"

        if isinstance(value, bool):

            return value

        return str(
            value
        ).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
        }

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

    def _truncate(
        self,
        value,
        max_length
    ):

        value = self._clean_string(
            value
        )

        if value is None:

            return None

        return value[:max_length]

    def _filter_to_table_columns(
        self,
        row,
        table
    ):

        return {
            column_name: row.get(
                column_name
            )
            for column_name in table.c.keys()
        }
