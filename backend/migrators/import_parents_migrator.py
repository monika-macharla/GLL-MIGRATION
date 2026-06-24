import logging
import re
import uuid

from collections import defaultdict
from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class ImportParentsMigrator(BaseMigrator):

    SOURCE_TABLE = "gl_parent"
    DESTINATION_TABLE = "import_parents"
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
            "Starting Import Parents Migration..."
        )

        parent_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        address_table = self._manual_reflect(
            "address",
            self.source_engine,
            self.metadata_source
        )

        state_table = self._manual_reflect(
            "state",
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
            parent_table
        )
        destination_count = self._count_rows(
            self.dest_engine,
            destination_table
        )

        logger.info(
            f"Source gl_parent count: {source_count}"
        )
        logger.info(
            f"Destination import_parents current count: "
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
        student_number_fallbacks = 0
        first_name_fallbacks = 0
        last_name_fallbacks = 0
        missing_student_links = 0
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
                    parent_table,
                    student_table,
                    address_table,
                    state_table
                )
                .select_from(
                    parent_table
                    .outerjoin(
                        student_table,
                        parent_table.c.student_id
                        == student_table.c.id
                    )
                    .outerjoin(
                        address_table,
                        parent_table.c.address_id
                        == address_table.c.id
                    )
                    .outerjoin(
                        state_table,
                        address_table.c.state
                        == state_table.c.id
                    )
                )
                .where(
                    parent_table.c.id > last_source_id
                )
                .order_by(
                    parent_table.c.id
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

                row_dict = row._mapping
                source_parent_id = row_dict.get(
                    parent_table.c.id
                )
                source_student_id = row_dict.get(
                    parent_table.c.student_id
                )
                last_source_id = source_parent_id

                if not source_student_id:

                    missing_student_links += 1

                student_number = (
                    self._clean_string(
                        row_dict.get(
                            parent_table.c.student_number
                        )
                    )
                    or
                    self._clean_string(
                        row_dict.get(
                            student_table.c.school_student_id
                        )
                    )
                    or
                    self._clean_string(
                        row_dict.get(
                            student_table.c.email
                        )
                    )
                )

                if not student_number:

                    student_number_fallbacks += 1
                    student_number = (
                        f"GL-STUDENT-{source_student_id}"
                        if source_student_id
                        else
                        f"GL-PARENT-{source_parent_id}"
                    )

                contact_first_name = self._clean_string(
                    row_dict.get(
                        parent_table.c.first_name
                    )
                )

                if not contact_first_name:

                    first_name_fallbacks += 1
                    contact_first_name = "UNKNOWN"

                contact_last_name = self._clean_string(
                    row_dict.get(
                        parent_table.c.last_name
                    )
                )

                if not contact_last_name:

                    last_name_fallbacks += 1
                    contact_last_name = "UNKNOWN"

                source_institution_id = row_dict.get(
                    student_table.c.institution_id
                )

                if not source_institution_id:

                    missing_institution_id += 1

                mapped_row = {
                    "uuid": self._import_parent_uuid(
                        source_parent_id
                    ),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "deleted_at": None,
                    "student_number": self._truncate(
                        student_number,
                        255
                    ),
                    "contact_last_name": self._truncate(
                        contact_last_name,
                        255
                    ),
                    "contact_first_name": self._truncate(
                        contact_first_name,
                        255
                    ),
                    "contact_middle_name": self._truncate(
                        row_dict.get(
                            parent_table.c.middle_name
                        ),
                        255
                    ),
                    "relationship_type": self._truncate(
                        row_dict.get(
                            parent_table.c.relationship
                        ),
                        100
                    ),
                    "contact_phone_number": self._truncate(
                        row_dict.get(
                            parent_table.c.phone_number
                        ),
                        20
                    ),
                    "contact_email": self._truncate(
                        row_dict.get(
                            parent_table.c.email_address
                        ),
                        255
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
                    "is_registered": 1 if row_dict.get(
                        parent_table.c.user_id
                    ) else 0,
                    "address_line1": self._truncate(
                        row_dict.get(
                            address_table.c.address_line_1
                        ),
                        255
                    ),
                    "city": self._truncate(
                        row_dict.get(
                            address_table.c.city
                        ),
                        255
                    ),
                    "state": self._truncate(
                        row_dict.get(
                            state_table.c.state_code
                        ),
                        255
                    ),
                    "zip5": self._truncate(
                        row_dict.get(
                            address_table.c.zip_code
                        ),
                        255
                    ),
                }

                insert_data.append(
                    self._filter_to_table_columns(
                        mapped_row,
                        destination_table
                    )
                )

            if insert_data:

                batch_number += 1
                prepared_count += len(
                    insert_data
                )

                logger.info(
                    f"Inserting import_parents chunk "
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
            "Import Parents Migration Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"student_number_fallbacks="
            f"{student_number_fallbacks}, "
            f"first_name_fallbacks={first_name_fallbacks}, "
            f"last_name_fallbacks={last_name_fallbacks}, "
            f"missing_student_links={missing_student_links}, "
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
            "Loaded import_parents institution UUID lookup: "
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

    def _import_parent_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:import-parents:{source_id}"
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

        table_columns = set(
            table.c.keys()
        )

        return {
            key: value
            for key, value in row.items()
            if key in table_columns
        }
