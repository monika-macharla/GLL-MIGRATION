import logging
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class HSCourseInformationMigrator(BaseMigrator):

    SOURCE_TABLE = "hs_course_information"
    DESTINATION_TABLE = "import_course_information"
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
            "Starting HS Course Information Import Migration..."
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
            f"Source hs_course_information count: "
            f"{source_count}"
        )
        logger.info(
            f"Destination import_course_information current count: "
            f"{destination_count}"
        )

        batch_size = self._get_batch_size()
        remaining_limit = self.config.get("limit")

        if remaining_limit is not None:

            remaining_limit = int(
                remaining_limit
            )

        last_source_id = self._get_start_after_id()
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
                    "school_year": self._truncate(
                        row_dict.get(
                            source_table.c.school_year
                        ),
                        10
                    ),
                    "subject_area": self._truncate(
                        row_dict.get(
                            source_table.c.department
                        ),
                        50
                    ),
                    "course_id": self._truncate(
                        row_dict.get(
                            source_table.c.course_number
                        ),
                        50
                    ),
                    "course_name": self._truncate(
                        row_dict.get(
                            source_table.c.course_name
                        ),
                        255
                    ),
                    "course_short_name": self._truncate(
                        row_dict.get(
                            source_table.c.course_short_name
                        ),
                        100
                    ),
                    "term_code": self._truncate(
                        row_dict.get(
                            source_table.c.store_code
                        ),
                        10
                    ),
                    "explanation": self._clean_string(
                        row_dict.get(
                            source_table.c.code
                        )
                    ),
                    "grade_level": self._truncate(
                        row_dict.get(
                            source_table.c.grade_level
                        ),
                        10
                    ),
                    "semester_seq": self._truncate(
                        row_dict.get(
                            source_table.c.semester_seq
                        ),
                        10
                    ),
                    "credit_campus": self._truncate(
                        row_dict.get(
                            source_table.c.credit_campus
                        ),
                        50
                    ),
                    "pass_fail_credit": self._truncate(
                        row_dict.get(
                            source_table.c.pass_fail_credit_indicator
                        ),
                        10
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
                    "grade_average": self._truncate(
                        row_dict.get(
                            source_table.c.average
                        ),
                        20
                    ),
                    "final_grade_average": self._truncate(
                        row_dict.get(
                            source_table.c.course_grade
                        ),
                        20
                    ),
                    "credit_earned": self._truncate(
                        row_dict.get(
                            source_table.c.credit_earned
                        ),
                        20
                    ),
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
                    f"Inserting import_course_information chunk "
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
            "HS Course Information Import Summary: "
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

    def _get_start_after_id(self):

        configured = (
            self.config.get("hs_course_information_start_after_id")
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

    def _stable_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:import-course-information:{source_id}"
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
