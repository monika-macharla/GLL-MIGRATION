import logging
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UserCampusMigrator(BaseMigrator):

    SOURCE_TABLE = "institution_user"
    SOURCE_INSTITUTION_TABLE = "institution"
    SOURCE_USER_TABLE = "gl_user"
    DESTINATION_TABLE = "user_campus"
    DESTINATION_CAMPUS_TABLE = "institution_campuses"
    COUNSELLOR_ROLE_ID = 6
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
            "Starting User Campus Migration..."
        )

        institution_user_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        institution_table = self._manual_reflect(
            self.SOURCE_INSTITUTION_TABLE,
            self.source_engine,
            self.metadata_source
        )

        gl_user_table = self._manual_reflect(
            self.SOURCE_USER_TABLE,
            self.source_engine,
            self.metadata_source
        )

        user_campus_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        campus_table = self._manual_reflect(
            self.DESTINATION_CAMPUS_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        source_count = self._count_source_rows(
            institution_user_table,
            institution_table
        )
        destination_count = self._count_rows(
            self.dest_engine,
            user_campus_table
        )

        logger.info(
            f"Source counsellor campus rows: {source_count}"
        )
        logger.info(
            f"Destination user_campus current count: "
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
        missing_campus = 0
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
                    institution_user_table,
                    institution_table,
                    gl_user_table
                )
                .select_from(
                    institution_user_table
                    .join(
                        institution_table,
                        institution_user_table.c.institution_id
                        == institution_table.c.id
                    )
                    .join(
                        gl_user_table,
                        institution_user_table.c.user_id
                        == gl_user_table.c.id,
                        isouter=True
                    )
                )
                .where(
                    institution_user_table.c.id > last_source_id,
                    institution_user_table.c.role_id
                    == self.COUNSELLOR_ROLE_ID,
                    institution_table.c.parent_Institution_id.isnot(
                        None
                    )
                )
                .order_by(
                    institution_user_table.c.id
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

            campus_ids = {
                self._campus_id(
                    row._mapping,
                    institution_table
                )
                for row in rows
            }
            campus_ids.discard(
                None
            )

            campuses_by_campus_id = self._load_campuses_by_campus_id(
                campus_table,
                campus_ids
            )

            insert_data = []
            batch_number += 1
            now = datetime.utcnow()

            for row in rows:

                row_dict = row._mapping
                source_institution_user_id = row_dict.get(
                    institution_user_table.c.id
                )
                last_source_id = source_institution_user_id

                user_uuid = self._user_uuid(
                    row_dict,
                    institution_user_table,
                    gl_user_table
                )

                if not user_uuid:

                    missing_user += 1
                    continue

                campus_id = self._campus_id(
                    row_dict,
                    institution_table
                )
                campus = campuses_by_campus_id.get(
                    campus_id
                )

                if not campus:

                    missing_campus += 1
                    logger.warning(
                        "Skipping institution_user.id="
                        f"{source_institution_user_id}: "
                        f"destination campus not found for "
                        f"campus_id={campus_id}"
                    )
                    continue

                institution_campus_uuid = campus.get(
                    "uuid"
                )

                mapped_row = {
                    "uuid": self._stable_uuid(
                        user_uuid,
                        institution_campus_uuid
                    ),
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                    "user_uuid": user_uuid,
                    "institution_campus_uuid": (
                        institution_campus_uuid
                    ),
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                }

                insert_data.append(
                    self._filter_to_table_columns(
                        mapped_row,
                        user_campus_table
                    )
                )

            if insert_data:

                prepared_count += len(
                    insert_data
                )

                logger.info(
                    "Inserting user_campus chunk "
                    f"{batch_number}: prepared="
                    f"{len(insert_data)}, "
                    f"total_fetched={fetched_count}"
                )

                statement = mysql_insert(
                    user_campus_table
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
            "User Campus Migration Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"missing_user={missing_user}, "
            f"missing_campus={missing_campus}"
        )

        return inserted_count

    def _load_campuses_by_campus_id(
        self,
        campus_table,
        campus_ids
    ):

        if not campus_ids:

            return {}

        query = select(
            campus_table
        ).where(
            campus_table.c.campus_id.in_(
                campus_ids
            )
        )

        campuses_by_campus_id = {}

        with self.dest_engine.connect() as dest_conn:

            for row in dest_conn.execute(
                query
            ):

                campus = dict(
                    row._mapping
                )
                campus_id = self._clean_string(
                    campus.get(
                        "campus_id"
                    )
                )

                if campus_id not in campuses_by_campus_id:

                    campuses_by_campus_id[
                        campus_id
                    ] = campus

        return campuses_by_campus_id

    def _count_source_rows(
        self,
        institution_user_table,
        institution_table
    ):

        query = (
            select(
                func.count()
            )
            .select_from(
                institution_user_table.join(
                    institution_table,
                    institution_user_table.c.institution_id
                    == institution_table.c.id
                )
            )
            .where(
                institution_user_table.c.role_id
                == self.COUNSELLOR_ROLE_ID,
                institution_table.c.parent_Institution_id.isnot(
                    None
                )
            )
        )

        with self.source_engine.connect() as conn:

            return conn.execute(
                query
            ).scalar() or 0

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
            self.config.get("user_campus_migration_batch_size")
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
        institution_user_table,
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
                institution_user_table.c.user_id
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

    def _campus_id(
        self,
        row_dict,
        institution_table
    ):

        return (
            self._clean_string(
                row_dict.get(
                    institution_table.c.school_code
                )
            )
            or
            self._clean_string(
                row_dict.get(
                    institution_table.c.id
                )
            )
        )

    def _stable_uuid(
        self,
        user_uuid,
        institution_campus_uuid
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                "gll:user-campus:"
                f"{user_uuid}:{institution_campus_uuid}"
            )
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
            column_name: row.get(
                column_name
            )
            for column_name in table.c.keys()
        }
