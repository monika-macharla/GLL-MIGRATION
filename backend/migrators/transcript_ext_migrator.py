import json
import logging
import uuid

from datetime import datetime

from sqlalchemy import func, insert, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class TranscriptExtMigrator(BaseMigrator):

    SOURCE_TABLE = "transcript_ext"
    DESTINATION_TABLE = "import_edi_transcript_ext"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 20000

    JSON_COLUMNS = {
        "core_complete",
        "trc_fresh_start",
        "tsi_met",
        "core_curriculum",
        "inst_attend",
        "tec_drops",
        "term_comments",
        "term_disciplinary",
        "terms_crses_coms",
        "terms_crses_oth_desc",
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
            "Starting EDI Transcript Ext Import Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        transcript_table = self._manual_reflect(
            "transcript",
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
            f"Source transcript_ext count: {source_count}"
        )
        logger.info(
            f"Destination import_edi_transcript_ext current count: "
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

            query = (
                select(
                    source_table,
                    transcript_table
                )
                .select_from(
                    source_table.join(
                        transcript_table,
                        source_table.c.transcript_id
                        == transcript_table.c.id
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
                        transcript_table.c.stu_identification
                    )
                )

                if not student_number:

                    missing_student_number += 1
                    student_number = f"TRANSCRIPT-{row_dict.get(source_table.c.transcript_id)}"

                source_institution_id = row_dict.get(
                    transcript_table.c.institution_id
                )

                if not source_institution_id:

                    missing_institution_id += 1

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
                        self._institution_uuid(
                            source_institution_id
                        )
                        if source_institution_id
                        else
                        None
                    ),
                    "core_complete": self._json_value(
                        row_dict.get(
                            source_table.c.core_complete
                        )
                    ),
                    "trc_fresh_start": self._json_value(
                        row_dict.get(
                            source_table.c.trc_fresh_start
                        )
                    ),
                    "tsi_met": self._json_value(
                        row_dict.get(
                            source_table.c.tsi_met
                        )
                    ),
                    "core_curriculum": self._json_value(
                        row_dict.get(
                            source_table.c.core_curriculum
                        )
                    ),
                    "inst_attend": self._json_value(
                        row_dict.get(
                            source_table.c.inst_attend
                        )
                    ),
                    "tec_drops": self._json_value(
                        row_dict.get(
                            source_table.c.tec_drops
                        )
                    ),
                    "term_comments": self._json_value(
                        row_dict.get(
                            source_table.c.terms_coms
                        )
                    ),
                    "term_disciplinary": self._json_value(
                        row_dict.get(
                            source_table.c.terms_disc
                        )
                    ),
                    "terms_crses_coms": self._json_value(
                        row_dict.get(
                            source_table.c.terms_crses_coms
                        )
                    ),
                    "terms_crses_oth_desc": self._json_value(
                        row_dict.get(
                            source_table.c.terms_crses_oth_desc
                        )
                    ),
                    "terms_crses_othdts": self._clean_string(
                        row_dict.get(
                            source_table.c.terms_crses_oth_dts
                        )
                    ),
                    "terms_crses_wthdrw": self._clean_string(
                        row_dict.get(
                            source_table.c.terms_crses_wthdrw
                        )
                    ),
                    "term_stands": self._clean_string(
                        row_dict.get(
                            source_table.c.terms_stands
                        )
                    ),
                    "trc_lines": self._clean_string(
                        row_dict.get(
                            source_table.c.trc_lines
                        )
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
                    f"Inserting import_edi_transcript_ext chunk "
                    f"{batch_number}: prepared="
                    f"{len(insert_data)}, "
                    f"source_id_through={last_source_id}, "
                    f"total_fetched={fetched_count}"
                )

                statement = self._insert_ignore_statement(
                    destination_table
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
            "EDI Transcript Ext Import Summary: "
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

    def _insert_ignore_statement(
        self,
        table
    ):

        if self.dest_engine.dialect.name == "mysql":

            return mysql_insert(
                table
            ).prefix_with(
                "IGNORE"
            )

        return insert(
            table
        )

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
                f"gll:import-edi-transcript-ext:{source_id}"
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

    def _json_value(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if not value:

            return None

        if value.lower() == "null":

            return None

        parsed = value

        for _ in range(3):

            if not isinstance(
                parsed,
                str
            ):

                break

            candidate = parsed.strip()

            if not candidate:

                return None

            if candidate.lower() == "null":

                return None

            try:

                parsed = json.loads(
                    candidate
                )

            except (TypeError, ValueError):

                return candidate

        return parsed

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
