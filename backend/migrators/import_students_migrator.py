import json
import logging
import re
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class ImportStudentsMigrator(BaseMigrator):

    SOURCE_TABLE = "gl_student"
    DESTINATION_TABLE = "import_students"
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
            "Starting Import Students Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
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
            f"Source gl_student count: {source_count}"
        )
        logger.info(
            f"Destination import_students current count: "
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
        missing_student_number_fallback = 0
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
                    address_table,
                    state_table
                )
                .select_from(
                    source_table
                    .outerjoin(
                        address_table,
                        source_table.c.address_id
                        == address_table.c.id
                    )
                    .outerjoin(
                        state_table,
                        address_table.c.state
                        == state_table.c.id
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

            transcript_lookup = self._build_transcript_lookup(
                rows,
                source_table,
                transcript_table
            )

            insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_id = row_dict.get(
                    source_table.c.id
                )
                last_source_id = source_id

                student_number = self._clean_string(
                    row_dict.get(
                        source_table.c.school_student_id
                    )
                )

                if not student_number:

                    missing_student_number_fallback += 1
                    student_number = (
                        self._clean_string(
                            row_dict.get(
                                source_table.c.email
                            )
                        )
                        or
                        f"GL-STUDENT-{source_id}"
                    )

                source_institution_id = row_dict.get(
                    source_table.c.institution_id
                )

                if not source_institution_id:

                    missing_institution_id += 1

                zip5, zip4 = self._split_zip(
                    row_dict.get(
                        address_table.c.zip_code
                    )
                )

                created_at = (
                    row_dict.get(
                        source_table.c.created_date
                    )
                    or
                    datetime.utcnow()
                )
                updated_at = (
                    row_dict.get(
                        source_table.c.last_modified_date
                    )
                    or
                    created_at
                )
                transcript_data = transcript_lookup.get(
                    source_id,
                    {}
                )

                mapped_row = {
                    "uuid": self._import_student_uuid(
                        source_id
                    ),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": None,
                    "student_number": self._truncate(
                        student_number,
                        255
                    ),
                    "first_name": self._truncate(
                        row_dict.get(
                            source_table.c.first_name
                        ),
                        255
                    ),
                    "middle_name": self._truncate(
                        row_dict.get(
                            source_table.c.middle_name
                        ),
                        255
                    ),
                    "last_name": self._truncate(
                        row_dict.get(
                            source_table.c.last_name
                        ),
                        255
                    ),
                    "address_line1": self._truncate(
                        row_dict.get(
                            address_table.c.address_line_1
                        ),
                        255
                    ),
                    "address_line2": self._truncate(
                        row_dict.get(
                            address_table.c.address_line_2
                        ),
                        255
                    ),
                    "city": self._truncate(
                        row_dict.get(
                            address_table.c.city
                        ),
                        100
                    ),
                    "state": self._truncate(
                        row_dict.get(
                            state_table.c.state_code
                        ),
                        50
                    ),
                    "zip5": zip5,
                    "zip4": zip4,
                    "ethnicity": self._truncate(
                        row_dict.get(
                            source_table.c.person_ethnics
                        ),
                        100
                    ),
                    "race": self._truncate(
                        row_dict.get(
                            source_table.c.person_race
                        ),
                        100
                    ),
                    "ssn_last4": self._last_four_digits(
                        row_dict.get(
                            source_table.c.last4_ssn
                        )
                    ),
                    "student_state_number": self._truncate(
                        row_dict.get(
                            source_table.c.school_student_id
                        ),
                        50
                    ),
                    "date_of_birth": self._date_as_string(
                        row_dict.get(
                            source_table.c.date_of_birth
                        )
                    ),
                    "grade_level": None,
                    "gender": self._truncate(
                        row_dict.get(
                            source_table.c.gender
                        ),
                        50
                    ),
                    "language": self._truncate(
                        row_dict.get(
                            source_table.c.person_primary_language
                        ),
                        100
                    ),
                    "cdcn": self._truncate(
                        row_dict.get(
                            source_table.c.school_address_id
                        ),
                        50
                    ),
                    "currently_enrolled": self._bit_to_int(
                        row_dict.get(
                            source_table.c.currently_enrolled
                        )
                    ),
                    "enrollment_id": self._enrollment_uuid(
                        source_id
                    ),
                    "is_registered": 1 if row_dict.get(
                        source_table.c.user_id
                    ) else 0,
                    "institution_id": (
                        self._institution_uuid(
                            source_institution_id
                        )
                        if source_institution_id
                        else
                        None
                    ),
                    "import_file_uuid": None,
                    "email": self._truncate(
                        row_dict.get(
                            source_table.c.email
                        ),
                        255
                    ),
                    "phone_number": self._truncate(
                        row_dict.get(
                            source_table.c.phone_no
                        ),
                        255
                    ),
                    "phone_type": self._truncate(
                        row_dict.get(
                            source_table.c.phone_type
                        ),
                        255
                    ),
                    "is_demographic": 1 if (
                        row_dict.get(
                            source_table.c.person_ethnics
                        )
                        or
                        row_dict.get(
                            source_table.c.person_race
                        )
                    ) else 0,
                    "prefix": None,
                    "suffix": None,
                    "fieldOfStudy": transcript_data.get(
                        "field_of_study"
                    ),
                    "degreeAwarded": transcript_data.get(
                        "degrees_awarded"
                    ),
                    "gpa": None,
                    "creditHours": None,
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
                    f"Inserting import_students chunk "
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
            "Import Students Migration Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"student_number_fallbacks="
            f"{missing_student_number_fallback}, "
            f"missing_institution_id={missing_institution_id}"
        )

        return inserted_count

    def _build_transcript_lookup(
        self,
        student_rows,
        source_table,
        transcript_table
    ):

        student_ids = [
            row._mapping.get(
                source_table.c.id
            )
            for row in student_rows
            if row._mapping.get(
                source_table.c.id
            ) is not None
        ]

        if not student_ids:

            return {}

        lookup = {}

        with self.source_engine.connect() as conn:

            rows = conn.execute(
                select(
                    transcript_table.c.student_id,
                    transcript_table.c.field_of_study,
                    transcript_table.c.degrees_awarded
                )
                .where(
                    transcript_table.c.student_id.in_(
                        student_ids
                    )
                )
            ).fetchall()

        for row in rows:

            row_dict = row._mapping
            student_id = row_dict.get(
                transcript_table.c.student_id
            )

            if student_id is None:

                continue

            student_data = lookup.setdefault(
                student_id,
                {
                    "field_of_study": [],
                    "degrees_awarded": [],
                    "_field_seen": set(),
                    "_degree_seen": set(),
                }
            )

            self._extend_json_values(
                student_data["field_of_study"],
                student_data["_field_seen"],
                row_dict.get(
                    transcript_table.c.field_of_study
                )
            )
            self._extend_json_values(
                student_data["degrees_awarded"],
                student_data["_degree_seen"],
                row_dict.get(
                    transcript_table.c.degrees_awarded
                )
            )

        cleaned_lookup = {}

        for student_id, student_data in lookup.items():

            cleaned_lookup[student_id] = {
                "field_of_study": (
                    student_data["field_of_study"]
                    or
                    None
                ),
                "degrees_awarded": (
                    student_data["degrees_awarded"]
                    or
                    None
                ),
            }

        return cleaned_lookup

    def _extend_json_values(
        self,
        target,
        seen,
        raw_value
    ):

        values = self._json_array_values(
            raw_value
        )

        for value in values:

            cleaned = self._clean_string(
                value
            )

            if not cleaned:

                continue

            if cleaned in seen:

                continue

            seen.add(
                cleaned
            )
            target.append(
                cleaned
            )

    def _json_array_values(
        self,
        raw_value
    ):

        raw_value = self._clean_string(
            raw_value
        )

        if not raw_value:

            return []

        try:

            parsed = json.loads(
                raw_value
            )

        except (TypeError, ValueError):

            return [
                raw_value
            ]

        if parsed is None:

            return []

        if isinstance(
            parsed,
            list
        ):

            return parsed

        return [
            parsed
        ]

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

    def _import_student_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:import-students:{source_id}"
            )
        )

    def _enrollment_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:import-students:enrollment:{source_id}"
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

    def _split_zip(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if not value:

            return None, None

        parts = re.split(
            r"[-\s]+",
            value,
            maxsplit=1
        )

        zip5 = self._truncate(
            parts[0],
            255
        )
        zip4 = (
            self._truncate(
                parts[1],
                10
            )
            if len(parts) > 1
            else
            None
        )

        return zip5, zip4

    def _last_four_digits(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if not value:

            return None

        digits = re.sub(
            r"\D",
            "",
            value
        )

        if not digits:

            return None

        return digits[-4:]

    def _date_as_string(
        self,
        value
    ):

        if not value:

            return None

        if hasattr(
            value,
            "isoformat"
        ):

            return value.isoformat()

        return self._clean_string(
            value
        )

    def _bit_to_int(
        self,
        value
    ):

        if value is None:

            return None

        if isinstance(
            value,
            bytes
        ):

            return 1 if value != b"\x00" else 0

        return 1 if bool(value) else 0

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
