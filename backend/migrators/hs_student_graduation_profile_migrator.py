import logging
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class HSStudentGraduationProfileMigrator(BaseMigrator):

    SOURCE_TABLE = "hs_student_graduation_profile"
    DESTINATION_TABLE = "import_student_graduation_profile"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 20000

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
            "Starting HS Student Graduation Profile Import Migration..."
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
            "Source hs_student_graduation_profile count: "
            f"{source_count}"
        )
        logger.info(
            "Destination import_student_graduation_profile current count: "
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
            now = datetime.utcnow()

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
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                    "student_number": self._truncate(
                        student_number,
                        255
                    ),
                    "graduation_program_type": self._truncate(
                        row_dict.get(
                            source_table.c.graduation_program_type
                        ),
                        100
                    ),
                    "graduation_date": self._truncate(
                        row_dict.get(
                            source_table.c.graduation_date
                        ),
                        10
                    ),
                    "fhsp_participant": self._truncate(
                        row_dict.get(
                            source_table.c.fhsp_participant
                        ),
                        10
                    ),
                    "fhsp_distinguished_level_achieve": self._truncate(
                        row_dict.get(
                            source_table.c.fhsp_distinguished_level_achieve
                        ),
                        10
                    ),
                    "ah_endorsement_indicator": self._truncate(
                        row_dict.get(
                            source_table.c.ah_endorsement_indicator
                        ),
                        10
                    ),
                    "bi_endorsement_indicator": self._truncate(
                        row_dict.get(
                            source_table.c.bi_endorsement_indicator
                        ),
                        10
                    ),
                    "stem_endorsement_indicator": self._truncate(
                        row_dict.get(
                            source_table.c["stem_endorsement_Indicator"]
                        ),
                        10
                    ),
                    "public_services_endorsement_indicator": self._truncate(
                        row_dict.get(
                            source_table.c["ps_endorsement_Indicator"]
                        ),
                        10
                    ),
                    "multi_studies_endorsement_indicator": self._truncate(
                        row_dict.get(
                            source_table.c["ms_endorsement_Indicator"]
                        ),
                        10
                    ),
                    "certificate_completion_date": self._truncate(
                        row_dict.get(
                            source_table.c.certificate_completion_date
                        ),
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
                    "Inserting import_student_graduation_profile chunk "
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
            "HS Student Graduation Profile Import Summary: "
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
                f"gll:import-student-graduation-profile:{source_id}"
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
