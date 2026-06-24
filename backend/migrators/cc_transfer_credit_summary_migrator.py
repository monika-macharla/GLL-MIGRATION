import logging
import uuid

from datetime import datetime

from sqlalchemy import func, inspect, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CCTransferCreditSummaryMigrator(BaseMigrator):

    SOURCE_TABLE = "cc_transfer_credit_summary"
    DESTINATION_TABLE = "import_edi_institutions_attended"
    DESTINATION_TABLE_ALIAS = "import_edi_inst_attended"
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
            "Starting CC Transfer Credit Summary Import Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        cc_transcript_table = self._manual_reflect(
            "cc_transcript",
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

        destination_table_name = self._resolve_table_name(
            self.dest_engine,
            self.DESTINATION_TABLE,
            self.DESTINATION_TABLE_ALIAS
        )

        destination_table = self._manual_reflect(
            destination_table_name,
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
            f"Source {self.SOURCE_TABLE} count: {source_count}"
        )
        logger.info(
            f"Destination {destination_table_name} current count: "
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
        invalid_start_date = 0
        invalid_exit_date = 0
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
                    source_table.c.id,
                    source_table.c.transcript_id,
                    source_table.c.institution_name,
                    source_table.c.credits,
                    source_table.c.start_date,
                    source_table.c.end_date,
                    cc_transcript_table.c.stu_identification,
                    credential_table.c.institution_id,
                    institution_table.c.name
                )
                .select_from(
                    source_table
                    .join(
                        cc_transcript_table,
                        source_table.c.transcript_id
                        == cc_transcript_table.c.id
                    )
                    .join(
                        credential_table,
                        cc_transcript_table.c.credential_id
                        == credential_table.c.id
                    )
                    .join(
                        institution_table,
                        credential_table.c.institution_id
                        == institution_table.c.id,
                        isouter=True
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

                student_number = self._clean_string(
                    row_dict.get(
                        cc_transcript_table.c.stu_identification
                    )
                )

                if not student_number:

                    missing_student_number += 1
                    student_number = f"CC-TRANSCRIPT-{row_dict.get(source_table.c.transcript_id)}"

                source_institution_id = row_dict.get(
                    credential_table.c.institution_id
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

                start_date_val = row_dict.get(
                    source_table.c.start_date
                )
                start_date_parsed = self._parse_date(start_date_val)
                if start_date_val and start_date_parsed is None:
                    invalid_start_date += 1
                start_date_str = (
                    start_date_parsed.strftime("%Y-%m-%d")
                    if start_date_parsed
                    else None
                )

                end_date_val = row_dict.get(
                    source_table.c.end_date
                )
                exit_date_parsed = self._parse_date(end_date_val)
                if end_date_val and exit_date_parsed is None:
                    invalid_exit_date += 1
                exit_date_str = (
                    exit_date_parsed.strftime("%Y-%m-%d")
                    if exit_date_parsed
                    else None
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
                    "institution_name": self._truncate(
                        row_dict.get(
                            source_table.c.institution_name
                        ),
                        255
                    ),
                    "start_date": self._truncate(
                        start_date_str,
                        45
                    ),
                    "exit_date": self._truncate(
                        exit_date_str,
                        45
                    ),
                    "credits_awarded": self._truncate(
                        row_dict.get(
                            source_table.c.credits
                        ),
                        45
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
                    f"Inserting {destination_table_name} chunk "
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
            "CC Transfer Credit Summary Import Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"missing_student_number={missing_student_number}, "
            f"missing_institution_id={missing_institution_id}, "
            f"invalid_start_date={invalid_start_date}, "
            f"invalid_exit_date={invalid_exit_date}"
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

    def _load_destination_institution_lookup(self):

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            logger.warning(
                "auth_db lookup engine not configured; "
                "falling back to generated institution UUIDs."
            )

            return {}

        institutions_table = self._manual_reflect(
            "institutions",
            auth_db_engine,
            self.metadata_dest
        )

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    institutions_table.c.uuid,
                    institutions_table.c.name
                )
            ).fetchall()

        for row in rows:

            row_dict = row._mapping
            institution_name = self._normalize(
                row_dict.get(
                    institutions_table.c.name
                )
            )
            institution_uuid = row_dict.get(
                institutions_table.c.uuid
            )

            if institution_name and institution_uuid:

                lookup[
                    institution_name
                ] = institution_uuid

        logger.info(
            "Loaded destination institution lookup: "
            f"{len(lookup)} names"
        )

        return lookup

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
                f"gll:import-edi-institutions-attended:{source_id}"
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

    def _resolve_table_name(
        self,
        engine,
        primary_name,
        alias_name
    ):

        inspector = inspect(
            engine
        )

        table_names = set(
            inspector.get_table_names()
        )

        if primary_name in table_names:

            return primary_name

        if alias_name in table_names:

            return alias_name

        return primary_name

    def _parse_date(
        self,
        value
    ):

        if value is None:

            return None

        import datetime as dt

        if isinstance(value, dt.datetime):

            return value.date()

        if isinstance(value, dt.date):

            return value

        value_str = self._clean_string(
            value
        )

        if not value_str:

            return None

        # Clean off time portion if it's there
        if " " in value_str:

            value_str = value_str.split(" ")[0]

        for date_format in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y/%m/%d",
            "%Y%m%d",
        ):

            try:

                return datetime.strptime(
                    value_str,
                    date_format
                ).date()

            except ValueError:

                continue

        return None
