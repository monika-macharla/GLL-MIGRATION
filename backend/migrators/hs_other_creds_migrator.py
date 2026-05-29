import logging
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class HSOtherCredsMigrator(BaseMigrator):

    SOURCE_TABLE_ALIASES = [
        "hs_other_creds",
        "hs_othser_creds",
    ]
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 20000

    CATEGORY_CONFIG = {
        "APIBAcknowledgements": {
            "table": "import_apibs",
            "value_column": "apib",
            "date_column": "apib_met_date",
        },
        "BiLiteracy": {
            "table": "import_biliteracies",
            "value_column": "bi_literacy_code",
            "date_column": "bi_literacy_met_date",
        },
        "DualCredit": {
            "table": "import_dual_credits",
            "value_column": "dual_credit",
            "date_column": "dual_credit_met_date",
        },
        "PerfAckCollegeAssess": {
            "table": "import_college_assessments",
            "value_column": "college_assessment",
            "date_column": "college_assessment_met_date",
        },
        "Certifications/Licensures": {
            "table": "import_cert_lics",
            "value_column": "certification_licensure",
            "date_column": "cert_lic_date",
        },
        "CPRMetDate": {
            "table": "import_other_requirements",
            "date_column": "cpr_met_date",
        },
        "SpeechMetDate": {
            "table": "import_other_requirements",
            "date_column": "speech_met_date",
        },
        "POIIMetDate": {
            "table": "import_other_requirements",
            "date_column": "poii_met_date",
        },
        "FinancialAidMetDate": {
            "table": "import_other_requirements",
            "date_column": "financial_aid_met_date",
        },
    }

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
            "Starting HS Other Credentials Import Migration..."
        )

        source_table_name = self._resolve_source_table_name()

        source_table = self._manual_reflect(
            source_table_name,
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

        destination_tables = {
            table_name: self._manual_reflect(
                table_name,
                self.dest_engine,
                self.metadata_dest
            )
            for table_name in sorted({
                config["table"]
                for config in self.CATEGORY_CONFIG.values()
            })
        }

        source_count = self._count_rows(
            self.source_engine,
            source_table
        )
        destination_counts = {
            table_name: self._count_rows(
                self.dest_engine,
                destination_table
            )
            for table_name, destination_table in destination_tables.items()
        }
        category_counts = self._source_category_counts(
            source_table
        )

        logger.info(
            f"Source {source_table_name} count: {source_count}"
        )
        logger.info(
            f"Destination starting counts: {destination_counts}"
        )
        logger.info(
            f"Source category counts: {category_counts}"
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
        skipped_unknown_category = 0
        missing_student_number = 0
        missing_institution_id = 0
        batch_number = 0
        inserted_by_table = {
            table_name: 0
            for table_name in destination_tables
        }
        prepared_by_table = {
            table_name: 0
            for table_name in destination_tables
        }

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

            insert_data_by_table = {
                table_name: []
                for table_name in destination_tables
            }
            batch_number += 1

            for row in rows:

                row_dict = row._mapping
                source_id = row_dict.get(
                    source_table.c.id
                )
                last_source_id = source_id

                category = self._clean_string(
                    row_dict.get(
                        source_table.c.category
                    )
                )
                category_config = self.CATEGORY_CONFIG.get(
                    category
                )

                if not category_config:

                    skipped_unknown_category += 1
                    continue

                table_name = category_config["table"]
                destination_table = destination_tables[
                    table_name
                ]

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
                        table_name,
                        source_id
                    ),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "deleted_at": None,
                    "student_number": self._truncate(
                        student_number,
                        255
                    ),
                    "institution_id": (
                        self._institution_uuid(
                            source_institution_id
                        )
                        if source_institution_id
                        else
                        None
                    ),
                    "import_file_uuid": None,
                }

                value_column = category_config.get(
                    "value_column"
                )

                if value_column:

                    mapped_row[value_column] = self._truncate(
                        (
                            row_dict.get(
                                source_table.c.print_value
                            )
                            or
                            row_dict.get(
                                source_table.c.code
                            )
                        ),
                        self._column_length(
                            destination_table,
                            value_column,
                            255
                        )
                    )

                date_column = category_config.get(
                    "date_column"
                )

                if date_column:

                    mapped_row[date_column] = self._truncate(
                        row_dict.get(
                            source_table.c.met_date
                        ),
                        self._column_length(
                            destination_table,
                            date_column,
                            255
                        )
                    )

                insert_data_by_table[table_name].append(
                    self._filter_to_table_columns(
                        mapped_row,
                        destination_table
                    )
                )

            for table_name, insert_data in insert_data_by_table.items():

                if not insert_data:

                    continue

                prepared_count += len(
                    insert_data
                )
                prepared_by_table[table_name] += len(
                    insert_data
                )

                logger.info(
                    f"Inserting {table_name} chunk "
                    f"{batch_number}: prepared="
                    f"{len(insert_data)}, "
                    f"source_id_through={last_source_id}, "
                    f"total_fetched={fetched_count}"
                )

                statement = mysql_insert(
                    destination_tables[table_name]
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
                inserted_by_table[table_name] += inserted_in_chunk
                ignored_existing += (
                    len(insert_data)
                    - inserted_in_chunk
                )

            if len(rows) < fetch_size:

                break

        logger.info(
            "HS Other Credentials Import Summary: "
            f"source_count={source_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"prepared_by_table={prepared_by_table}, "
            f"inserted_by_table={inserted_by_table}, "
            f"skipped_unknown_category={skipped_unknown_category}, "
            f"missing_student_number={missing_student_number}, "
            f"missing_institution_id={missing_institution_id}"
        )

        return inserted_count

    def _resolve_source_table_name(self):

        with self.source_engine.connect() as conn:

            existing_tables = set(
                conn.dialect.get_table_names(
                    conn
                )
            )

        for table_name in self.SOURCE_TABLE_ALIASES:

            if table_name in existing_tables:

                return table_name

        raise ValueError(
            "hs_other_creds source table not found"
        )

    def _source_category_counts(
        self,
        source_table
    ):

        with self.source_engine.connect() as conn:

            rows = conn.execute(
                select(
                    source_table.c.category,
                    func.count().label(
                        "row_count"
                    )
                )
                .group_by(
                    source_table.c.category
                )
            ).fetchall()

        return {
            row._mapping.get(
                source_table.c.category
            ): row._mapping.get(
                "row_count"
            )
            for row in rows
        }

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
        table_name,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:{table_name}:hs-other-creds:{source_id}"
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

    def _column_length(
        self,
        table,
        column_name,
        default
    ):

        if column_name not in table.c:

            return default

        length = getattr(
            table.c[column_name].type,
            "length",
            None
        )

        return length or default

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
