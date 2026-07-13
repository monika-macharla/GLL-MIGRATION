import logging
import re
import uuid

from datetime import datetime

from sqlalchemy import func, inspect, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CCTranscriptExtGpaMigrator(BaseMigrator):

    SOURCE_TABLE = "transcript"
    SOURCE_TABLE_ALIAS = "transcript"
    DESTINATION_TABLE = "import_edi_gpa"
    DEFAULT_AUTH_DATABASE = "gllauthserviceuatmigration"
    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
    DEFAULT_BATCH_SIZE = 5000
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
            "Starting CC Transcript Extended Info GPA Import Migration..."
        )

        source_table_name = self._resolve_source_table_name()

        source_table = self._manual_reflect(
            source_table_name,
            self.source_engine,
            self.metadata_source
        )

        institution_table = self._manual_reflect(
            "institution",
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        destination_institution_lookup = (
            self._load_destination_institution_lookup()
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
            f"Source {source_table_name} count: {source_count}"
        )
        logger.info(
            f"Destination {self.DESTINATION_TABLE} current count: "
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
        missing_student_number = 0
        missing_institution_id = 0
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

            t2 = source_table.alias('t2')
            subquery = (
                select(func.max(t2.c.id))
                .where(t2.c.stu_identification == source_table.c.stu_identification)
                .where(t2.c.institution_id == source_table.c.institution_id)
                .correlate(source_table)
            )

            query = (
                select(
                    source_table.c.id,
                    source_table.c.stu_identification,
                    source_table.c.institution_id,
                    source_table.c.cgpa_gpa1,
                    source_table.c.cgpa_gpa2,
                    source_table.c.hrs_attempted_gpa1,
                    source_table.c.hrs_attempted_gpa2,
                    source_table.c.hrs_earned_gpa1,
                    source_table.c.hrs_earned_gpa2,
                    source_table.c.hrs_points_gpa1,
                    source_table.c.hrs_points_gpa2,
                    source_table.c.overall_hrs_earned,
                    source_table.c.status_info,
                    institution_table.c.name
                )
                .select_from(
                    source_table
                    .join(
                        institution_table,
                        source_table.c.institution_id
                        == institution_table.c.id,
                        isouter=True
                    )
                )
                .where(source_table.c.id == subquery)
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

                student_number = self._clean_string(
                    row_dict.get(
                        source_table.c.stu_identification
                    )
                )

                if not student_number:

                    missing_student_number += 1
                    student_number = f"TRANSCRIPT-{source_id}"

                source_institution_id = row_dict.get(
                    source_table.c.institution_id
                )

                if not source_institution_id:

                    missing_institution_id += 1

                institution_name = self._clean_string(
                    row_dict.get(
                        institution_table.c.name
                    )
                )
                destination_institution_uuid = (
                    destination_institution_lookup.get(
                        self._normalize(
                            institution_name
                        )
                    )
                    if institution_name
                    else
                    None
                )

                mapped_row = {
                    "uuid": self._stable_uuid(
                        source_id
                    ),
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                    "student_number": self._truncate(
                        student_number,
                        255
                    ),
                    "institution_id": (
                        destination_institution_uuid
                        or
                        (
                            self._institution_uuid(
                                source_institution_id
                            )
                            if source_institution_id
                            else
                            None
                        )
                    ),
                    "cgpa_gpa1": self._float_value(
                        row_dict.get(
                            source_table.c.cgpa_gpa1
                        )
                    ),
                    "cgpa_gpa2": self._float_value(
                        row_dict.get(
                            source_table.c.cgpa_gpa2
                        )
                    ),
                    "hrs_attempted_gpa1": self._truncate(
                        row_dict.get(
                            source_table.c.hrs_attempted_gpa1
                        ),
                        45
                    ),
                    "hrs_attempted_gpa2": self._truncate(
                        row_dict.get(
                            source_table.c.hrs_attempted_gpa2
                        ),
                        45
                    ),
                    "hrs_earned_gpa1": self._truncate(
                        row_dict.get(
                            source_table.c.hrs_earned_gpa1
                        ),
                        45
                    ),
                    "hrs_earned_gpa2": self._truncate(
                        row_dict.get(
                            source_table.c.hrs_earned_gpa2
                        ),
                        45
                    ),
                    "hrs_points_gpa1": self._truncate(
                        row_dict.get(
                            source_table.c.hrs_points_gpa1
                        ),
                        45
                    ),
                    "hrs_points_gpa2": self._truncate(
                        row_dict.get(
                            source_table.c.hrs_points_gpa2
                        ),
                        45
                    ),
                    "over_all_hrs_earned": self._truncate(
                        row_dict.get(
                            source_table.c.overall_hrs_earned
                        ),
                        45
                    ),
                    "status_info": self._truncate(
                        row_dict.get(
                            source_table.c.status_info
                        ),
                        255
                    ),
                    "import_file_uuid": None,
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
                    f"Inserting {self.DESTINATION_TABLE} chunk "
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
            "CC Transcript Extended Info GPA Import Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"missing_student_number={missing_student_number}, "
            f"missing_institution_id={missing_institution_id}"
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

    def _resolve_source_table_name(self):

        inspector = inspect(
            self.source_engine
        )

        table_names = set(
            inspector.get_table_names()
        )

        if self.SOURCE_TABLE in table_names:

            return self.SOURCE_TABLE

        if self.SOURCE_TABLE_ALIAS in table_names:

            return self.SOURCE_TABLE_ALIAS

        return self.SOURCE_TABLE

    def _load_destination_institution_lookup(self):

        auth_rows = self._load_auth_institution_rows()

        if not auth_rows:

            logger.warning(
                "No auth institution rows loaded; falling back to "
                "generated institution UUIDs."
            )

            return {}

        lookup = {}

        for row in auth_rows:

            institution_name = self._normalize(
                row.get("name")
            )
            institution_uuid = row.get(
                "uuid"
            )

            if institution_name and institution_uuid:

                lookup[
                    institution_name
                ] = str(institution_uuid)

        logger.info(
            "Loaded CCTranscriptExtGpa destination institution lookup: "
            f"{len(lookup)} names"
        )

        return lookup

    def _load_auth_institution_rows(
        self
    ):

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if auth_db_engine:

            institutions_table = self._manual_reflect(
                "institutions",
                auth_db_engine,
                self.metadata_dest
            )

            with auth_db_engine.connect() as auth_conn:

                rows = auth_conn.execute(
                    select(
                        institutions_table.c.uuid,
                        institutions_table.c.name
                    )
                    .where(
                        institutions_table.c.deleted_at.is_(None)
                    )
                    .where(
                        institutions_table.c.name.isnot(None)
                    )
                ).fetchall()

            return [
                dict(row._mapping)
                for row in rows
            ]

        auth_database = self._auth_database()

        logger.info(
            "auth_db lookup engine not configured; loading "
            f"institutions from schema {auth_database}."
        )

        try:

            with self.dest_engine.connect() as auth_conn:

                return auth_conn.execute(
                    text(
                        f"""
                        SELECT uuid, name
                        FROM `{auth_database}`.`institutions`
                        WHERE deleted_at IS NULL
                          AND name IS NOT NULL
                          AND TRIM(name) <> ''
                        """
                    )
                ).mappings().all()

        except Exception as exc:

            logger.warning(
                "Failed loading auth institutions from schema "
                f"{auth_database}: {exc}"
            )

            return []

    def _auth_database(
        self
    ):

        settings = self.config.get(
            "institution_uuid_fix",
            {}
        )
        auth_database = settings.get(
            "auth_database"
        ) or self.config.get(
            "auth_database"
        ) or self.DEFAULT_AUTH_DATABASE

        if not self.IDENTIFIER_PATTERN.match(
            str(auth_database)
        ):

            raise ValueError(
                f"Invalid auth database identifier: {auth_database}"
            )

        return auth_database

    def _get_batch_size(self):

        configured = (
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

    def _stable_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:import-edi-gpa:{source_id}"
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

    def _float_value(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if value is None:

            return None

        try:

            return float(
                value
            )

        except ValueError:

            return None

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

    def _normalize(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if value is None:

            return None

        return " ".join(
            value.lower().split()
        )

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
