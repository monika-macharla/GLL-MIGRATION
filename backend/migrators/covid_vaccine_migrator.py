import logging
import uuid

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CovidVaccineMigrator(BaseMigrator):

    SOURCE_TABLE = "covid_vaccine_meta_data"
    DESTINATION_TABLE = "vaccination_certificate_data"
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
            "Starting Covid Vaccine Import Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        gl_user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        gl_student_table = self._manual_reflect(
            "gl_student",
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
            f"Source covid_vaccine_meta_data count: {source_count}"
        )
        logger.info(
            f"Destination vaccination_certificate_data current count: "
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
        skipped_without_dose = 0
        missing_student = 0
        missing_institution = 0
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
                    gl_user_table
                )
                .select_from(
                    source_table.join(
                        gl_user_table,
                        source_table.c.user_id
                        == gl_user_table.c.id
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

                students_by_user_id = self._load_students_by_user_id(
                    source_conn,
                    gl_student_table,
                    rows,
                    source_table,
                    gl_user_table
                )

                institution_names = self._load_institution_names(
                    source_conn,
                    institution_table,
                    students_by_user_id
                )

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

                dose_rows = self._dose_rows(
                    row_dict,
                    source_table
                )

                if not dose_rows:

                    skipped_without_dose += 1
                    continue

                source_user_id = row_dict.get(
                    source_table.c.user_id
                )
                student_user_id = row_dict.get(
                    gl_user_table.c.user_id
                )
                student = (
                    students_by_user_id.get(
                        student_user_id
                    )
                    or
                    students_by_user_id.get(
                        source_user_id
                    )
                )

                if not student:

                    missing_student += 1

                source_institution_id = (
                    student.get("institution_id")
                    if student
                    else
                    None
                )

                if not source_institution_id:

                    missing_institution += 1

                student_number = self._student_number(
                    student,
                    row_dict,
                    gl_user_table,
                    source_user_id
                )

                for dose_number, vaccine_type, vaccine_date in dose_rows:

                    mapped_row = {
                        "uuid": self._stable_uuid(
                            source_id,
                            dose_number
                        ),
                        "created_at": now,
                        "updated_at": now,
                        "deleted_at": None,
                        "student_number": self._truncate(
                            student_number,
                            100
                        ),
                        "entity_name": self._truncate(
                            institution_names.get(
                                source_institution_id
                            ),
                            100
                        ),
                        "grade": None,
                        "student_first_name": self._truncate(
                            self._student_or_user_value(
                                student,
                                row_dict,
                                gl_user_table,
                                "first_name"
                            ),
                            100
                        ),
                        "student_middle_name": self._truncate(
                            self._student_or_user_value(
                                student,
                                row_dict,
                                gl_user_table,
                                "middle_name"
                            ),
                            100
                        ),
                        "student_last_name": self._truncate(
                            self._student_or_user_value(
                                student,
                                row_dict,
                                gl_user_table,
                                "last_name"
                            ),
                            100
                        ),
                        "dob": self._student_or_user_value(
                            student,
                            row_dict,
                            gl_user_table,
                            "date_of_birth"
                        ),
                        "phone": self._truncate(
                            self._student_or_user_value(
                                student,
                                row_dict,
                                gl_user_table,
                                "phone_no"
                            ),
                            20
                        ),
                        "vaccine_short_name": self._truncate(
                            vaccine_type,
                            100
                        ),
                        "vaccine_date": vaccine_date,
                        "vaccine_long_desc": self._truncate(
                            f"Dose {dose_number}",
                            255
                        ),
                        "institution_uuid": (
                            self._institution_uuid(
                                source_institution_id
                            )
                            if source_institution_id
                            else
                            self._institution_uuid(1)
                        ),
                        "import_file_uuid": None,
                        "status": 1,
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
                    "Inserting vaccination_certificate_data chunk "
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
            "Covid Vaccine Import Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"skipped_without_dose={skipped_without_dose}, "
            f"missing_student={missing_student}, "
            f"missing_institution={missing_institution}"
        )

        return inserted_count

    def _load_students_by_user_id(
        self,
        source_conn,
        gl_student_table,
        rows,
        source_table,
        gl_user_table
    ):

        user_ids = set()

        for row in rows:

            row_dict = row._mapping
            source_user_id = row_dict.get(
                source_table.c.user_id
            )
            student_user_id = row_dict.get(
                gl_user_table.c.user_id
            )

            if source_user_id:

                user_ids.add(
                    source_user_id
                )

            if student_user_id:

                user_ids.add(
                    student_user_id
                )

        if not user_ids:

            return {}

        query = (
            select(
                gl_student_table
            )
            .where(
                gl_student_table.c.user_id.in_(
                    user_ids
                )
            )
            .order_by(
                gl_student_table.c.user_id,
                gl_student_table.c.institution_id.is_(None),
                gl_student_table.c.id
            )
        )

        students_by_user_id = {}

        for student_row in source_conn.execute(
            query
        ):

            student = dict(
                student_row._mapping
            )
            user_id = student.get(
                "user_id"
            )

            if user_id not in students_by_user_id:

                students_by_user_id[
                    user_id
                ] = student

        return students_by_user_id

    def _load_institution_names(
        self,
        source_conn,
        institution_table,
        students_by_user_id
    ):

        institution_ids = {
            student.get("institution_id")
            for student in students_by_user_id.values()
            if student.get("institution_id")
        }

        if not institution_ids:

            return {}

        query = select(
            institution_table.c.id,
            institution_table.c.name
        ).where(
            institution_table.c.id.in_(
                institution_ids
            )
        )

        return {
            row._mapping.get(
                institution_table.c.id
            ): row._mapping.get(
                institution_table.c.name
            )
            for row in source_conn.execute(
                query
            )
        }

    def _dose_rows(
        self,
        row_dict,
        source_table
    ):

        dose_rows = []

        dose_1_type = self._clean_string(
            row_dict.get(
                source_table.c.dose1_type
            )
        )
        dose_1_date = row_dict.get(
            source_table.c.dose1_date
        )

        if dose_1_type or dose_1_date:

            dose_rows.append(
                (
                    1,
                    dose_1_type,
                    dose_1_date
                )
            )

        dose_2_type = self._clean_string(
            row_dict.get(
                source_table.c.dose2_type
            )
        )
        dose_2_date = row_dict.get(
            source_table.c.dose2_date
        )

        if dose_2_type or dose_2_date:

            dose_rows.append(
                (
                    2,
                    dose_2_type,
                    dose_2_date
                )
            )

        return dose_rows

    def _student_number(
        self,
        student,
        row_dict,
        gl_user_table,
        source_user_id
    ):

        if student:

            student_number = self._clean_string(
                student.get(
                    "school_student_id"
                )
            )

            if student_number:

                return student_number

        return (
            self._clean_string(
                row_dict.get(
                    gl_user_table.c.username
                )
            )
            or
            self._clean_string(
                row_dict.get(
                    gl_user_table.c.email
                )
            )
            or
            f"USER-{source_user_id}"
        )

    def _student_or_user_value(
        self,
        student,
        row_dict,
        gl_user_table,
        column_name
    ):

        if student:

            value = self._clean_string(
                student.get(
                    column_name
                )
            )

            if value is not None:

                return student.get(
                    column_name
                )

        return row_dict.get(
            gl_user_table.c[column_name]
        )

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
        source_id,
        dose_number
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:vaccination-certificate-data:{source_id}:{dose_number}"
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
