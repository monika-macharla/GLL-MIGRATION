import logging
import re
import uuid

from collections import defaultdict
from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class HSClassRankGPAMigrator(BaseMigrator):

    SOURCE_TABLE = "hs_class_rank_gpa"
    DESTINATION_TABLE = "import_class_rank_gpa"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 20000
    DEFAULT_AUTH_DATABASE = "gllauthserviceuatmigration"
    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")

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
            "Starting HS Class Rank GPA Import Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        hs_transcript_table = self._manual_reflect(
            "hs_transcript",
            self.source_engine,
            self.metadata_source
        )

        credential_table = self._manual_reflect(
            "credential",
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

        source_count = self._count_rows(
            self.source_engine,
            source_table
        )
        destination_count = self._count_rows(
            self.dest_engine,
            destination_table
        )

        logger.info(
            f"Source hs_class_rank_gpa count: {source_count}"
        )
        logger.info(
            f"Destination import_class_rank_gpa current count: "
            f"{destination_count}"
        )

        batch_size = self._get_batch_size()
        remaining_limit = self.config.get("limit")
        institution_uuid_lookup = (
            self._build_institution_uuid_lookup(
                institution_table
            )
        )

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

            query = (
                select(
                    source_table,
                    hs_transcript_table,
                    credential_table
                )
                .select_from(
                    source_table
                    .join(
                        hs_transcript_table,
                        source_table.c.transcript_id
                        == hs_transcript_table.c.id
                    )
                    .join(
                        credential_table,
                        hs_transcript_table.c.credential_id
                        == credential_table.c.id
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

            for row in rows:

                row_dict = row._mapping
                source_id = row_dict.get(
                    source_table.c.id
                )
                last_source_id = source_id

                student_number = self._clean_string(
                    row_dict.get(
                        hs_transcript_table.c.stu_identification
                    )
                )

                if not student_number:

                    missing_student_number += 1
                    student_number = f"HS-TRANSCRIPT-{row_dict.get(source_table.c.transcript_id)}"

                source_institution_id = row_dict.get(
                    credential_table.c.institution_id
                )

                if not source_institution_id:

                    missing_institution_id += 1

                mapped_row = {
                    "uuid": self._stable_uuid(
                        source_id
                    ),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "deleted_at": None,
                    "student_number": self._truncate(
                        student_number,
                        255
                    ),
                    "date_of_class_rank": self._date_as_string(
                        row_dict.get(
                            source_table.c.class_rank_date
                        )
                    ),
                    "class_rank_number": row_dict.get(
                        source_table.c.class_rank_number
                    ),
                    "class_size": row_dict.get(
                        source_table.c.class_size
                    ),
                    "quartile": self._truncate(
                        row_dict.get(
                            source_table.c.quartile
                        ),
                        10
                    ),
                    "weighted_gpa": self._round_decimal(
                        row_dict.get(
                            source_table.c.weighted_gpa
                        )
                    ),
                    "unweighted_gpa": self._round_decimal(
                        row_dict.get(
                            source_table.c.unweighted_gpa
                        )
                    ),
                    "cumulative_gpa": self._round_decimal(
                        row_dict.get(
                            source_table.c.cumulative_gpa
                        )
                    ),
                    "institution_id": (
                        institution_uuid_lookup.get(
                            source_institution_id
                        )
                        or
                        self._institution_uuid(
                            source_institution_id
                        )
                        if source_institution_id
                        else
                        None
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
                    f"Inserting import_class_rank_gpa chunk "
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
            "HS Class Rank GPA Import Summary: "
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

    def _build_institution_uuid_lookup(
        self,
        institution_table
    ):

        with self.source_engine.connect() as source_conn:

            source_rows = source_conn.execute(
                select(
                    institution_table.c.id,
                    institution_table.c.name
                )
                .where(
                    institution_table.c.name.isnot(None)
                )
            ).fetchall()

        auth_rows = self._load_auth_institution_rows()

        if not auth_rows:

            logger.warning(
                "No auth institution rows loaded; falling back to "
                "generated institution UUIDs."
            )

            return {}

        auth_by_name = defaultdict(list)

        for row in auth_rows:

            institution_name = self._normalize(
                row.get("name")
            )
            institution_uuid = row.get(
                "uuid"
            )

            if institution_name and institution_uuid:

                auth_by_name[
                    institution_name
                ].append(
                    institution_uuid
                )

        lookup = {}
        ambiguous = 0
        missing = 0

        for row in source_rows:

            row_dict = row._mapping
            source_institution_id = row_dict.get(
                institution_table.c.id
            )
            institution_name = self._normalize(
                row_dict.get(
                    institution_table.c.name
                )
            )

            if not source_institution_id or not institution_name:

                continue

            matches = auth_by_name.get(
                institution_name,
                []
            )

            if len(matches) == 1:

                lookup[
                    source_institution_id
                ] = str(matches[0])

            elif len(matches) > 1:

                ambiguous += 1

            else:

                missing += 1

        logger.info(
            "Loaded hs_class_rank_gpa institution UUID lookup: "
            f"mapped={len(lookup)}, "
            f"ambiguous={ambiguous}, "
            f"missing={missing}"
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
                f"gll:import-class-rank-gpa:{source_id}"
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

    def _date_as_string(
        self,
        value
    ):

        if value is None:

            return None

        if hasattr(
            value,
            "isoformat"
        ):

            return value.isoformat()

        return self._clean_string(
            value
        )

    def _round_decimal(
        self,
        value
    ):

        if value is None:

            return None

        return round(
            float(value),
            2
        )

    def _normalize(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if not value:

            return None

        return " ".join(
            value.lower().split()
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
