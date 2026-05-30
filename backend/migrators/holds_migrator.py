import logging
import uuid

from datetime import datetime

from sqlalchemy import func, inspect, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class HoldsMigrator(BaseMigrator):

    SOURCE_TABLE = "inst_holds_ext"
    DESTINATION_TABLE = "holds"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 20000
    USER_UUID_NAMESPACE = uuid.UUID(
        "55fa6c2d-84d5-5bd8-b0f3-6a1d8b6c91f4"
    )

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
            "Starting Holds Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        gl_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        gl_user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        destination_users_table = self._optional_reflect(
            "users",
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
            f"Source inst_holds_ext count: {source_count}"
        )
        logger.info(
            f"Destination holds current count: {destination_count}"
        )

        batch_size = self._get_batch_size()
        remaining_limit = self.config.get("limit")

        if remaining_limit is not None:

            remaining_limit = int(
                remaining_limit
            )

        offset = 0
        row_number = 0
        fetched_count = 0
        prepared_count = 0
        inserted_count = 0
        ignored_existing = 0
        missing_student = 0
        missing_user = 0
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
                    source_table
                )
                .order_by(
                    source_table.c.student_id,
                    source_table.c.hold_code,
                    source_table.c.hold_messages,
                    source_table.c.hold_descriptions,
                    source_table.c.add_date,
                    source_table.c.change_date
                )
                .limit(
                    fetch_size
                )
                .offset(
                    offset
                )
            )

            with self.source_engine.connect() as source_conn:

                rows = source_conn.execute(
                    query
                ).fetchall()

                if not rows:

                    break

                students_by_student_id = (
                    self._load_students_by_student_id(
                        source_conn,
                        gl_student_table,
                        rows,
                        source_table
                    )
                )

                users_by_id = self._load_users_by_id(
                    source_conn,
                    gl_user_table,
                    students_by_student_id
                )

                destination_users_by_email = (
                    self._load_destination_users_by_email(
                        destination_users_table,
                        students_by_student_id,
                        users_by_id
                    )
                )

            fetched_count += len(
                rows
            )
            offset += len(
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

                row_number += 1
                row_dict = row._mapping
                source_student_id = self._clean_string(
                    row_dict.get(
                        source_table.c.student_id
                    )
                )

                if not source_student_id:

                    source_student_id = f"UNKNOWN-{row_number}"

                student = students_by_student_id.get(
                    source_student_id
                )

                if not student:

                    missing_student += 1

                source_user_id = (
                    student.get("user_id")
                    if student
                    else
                    None
                )
                source_user = users_by_id.get(
                    source_user_id
                )

                if not source_user:

                    missing_user += 1

                source_institution_id = (
                    student.get("institution_id")
                    if student
                    else
                    None
                )

                if not source_institution_id:

                    missing_institution += 1

                created_at = (
                    self._parse_datetime(
                        row_dict.get(
                            source_table.c.add_date
                        )
                    )
                    or
                    now
                )
                updated_at = (
                    self._parse_datetime(
                        row_dict.get(
                            source_table.c.change_date
                        )
                    )
                    or
                    created_at
                )

                user_name = (
                    self._best_user_name(
                        source_user,
                        student,
                        source_student_id
                    )
                )
                user_email = (
                    self._source_user_value(
                        source_user,
                        "email"
                    )
                    or
                    self._student_value(
                        student,
                        "email"
                    )
                )
                destination_user = (
                    destination_users_by_email.get(
                        self._normalize(
                            user_email
                        )
                    )
                    if user_email
                    else
                    None
                )
                destination_user_uuid = (
                    destination_user.get("uuid")
                    if destination_user
                    else
                    (
                        self._user_uuid(
                            source_user
                        )
                        if source_user
                        else
                        None
                    )
                )

                if destination_user:

                    user_name = (
                        self._clean_string(
                            destination_user.get(
                                "user_name"
                            )
                        )
                        or
                        user_name
                    )

                mapped_row = {
                    "uuid": self._stable_uuid(
                        row_number,
                        row_dict,
                        source_table
                    ),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": None,
                    "user_name": self._truncate(
                        user_name,
                        255
                    ),
                    "user_email": self._truncate(
                        user_email,
                        255
                    ),
                    "student_id": self._truncate(
                        source_student_id,
                        100
                    ),
                    "student_number": self._truncate(
                        source_student_id,
                        255
                    ),
                    "is_hold": 1,
                    "hold_code": self._truncate(
                        row_dict.get(
                            source_table.c.hold_code
                        ),
                        100
                    ),
                    "hold_message": self._truncate(
                        row_dict.get(
                            source_table.c.hold_messages
                        ),
                        255
                    ),
                    "hold_messages": self._truncate(
                        row_dict.get(
                            source_table.c.hold_messages
                        ),
                        8000
                    ),
                    "hold_description": self._truncate(
                        row_dict.get(
                            source_table.c.hold_descriptions
                        ),
                        8000
                    ),
                    "hold_descriptions": self._truncate(
                        row_dict.get(
                            source_table.c.hold_descriptions
                        ),
                        8000
                    ),
                    "user_uuid": destination_user_uuid,
                    "institution_uuid": (
                        self._institution_uuid(
                            source_institution_id
                        )
                        if source_institution_id
                        else
                        None
                    ),
                    "institution_id": (
                        self._institution_uuid(
                            source_institution_id
                        )
                        if source_institution_id
                        else
                        self._institution_uuid(1)
                    ),
                    "import_file_uuid": None,
                    "created_by": destination_user_uuid,
                    "updated_by": destination_user_uuid,
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
                    "Inserting holds chunk "
                    f"{batch_number}: prepared="
                    f"{len(insert_data)}, "
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
            "Holds Migration Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"missing_student={missing_student}, "
            f"missing_user={missing_user}, "
            f"missing_institution={missing_institution}"
        )

        return inserted_count

    def _load_students_by_student_id(
        self,
        source_conn,
        gl_student_table,
        rows,
        source_table
    ):

        student_ids = {
            self._clean_string(
                row._mapping.get(
                    source_table.c.student_id
                )
            )
            for row in rows
        }
        student_ids.discard(
            None
        )

        if not student_ids:

            return {}

        query = (
            select(
                gl_student_table
            )
            .where(
                gl_student_table.c.school_student_id.in_(
                    student_ids
                )
            )
            .order_by(
                gl_student_table.c.school_student_id,
                gl_student_table.c.user_id.is_(None),
                gl_student_table.c.institution_id.is_(None),
                gl_student_table.c.id
            )
        )

        students_by_student_id = {}

        for student_row in source_conn.execute(
            query
        ):

            student = dict(
                student_row._mapping
            )
            school_student_id = self._clean_string(
                student.get(
                    "school_student_id"
                )
            )

            if school_student_id not in students_by_student_id:

                students_by_student_id[
                    school_student_id
                ] = student

        return students_by_student_id

    def _load_users_by_id(
        self,
        source_conn,
        gl_user_table,
        students_by_student_id
    ):

        user_ids = {
            student.get("user_id")
            for student in students_by_student_id.values()
            if student.get("user_id")
        }

        if not user_ids:

            return {}

        query = select(
            gl_user_table
        ).where(
            gl_user_table.c.id.in_(
                user_ids
            )
        )

        return {
            row._mapping.get(
                gl_user_table.c.id
            ): dict(
                row._mapping
            )
            for row in source_conn.execute(
                query
            )
        }

    def _load_destination_users_by_email(
        self,
        destination_users_table,
        students_by_student_id,
        users_by_id
    ):

        if destination_users_table is None:

            return {}

        emails = set()

        for student in students_by_student_id.values():

            email = self._normalize(
                student.get(
                    "email"
                )
            )

            if email:

                emails.add(
                    email
                )

        for source_user in users_by_id.values():

            email = self._normalize(
                source_user.get(
                    "email"
                )
            )

            if email:

                emails.add(
                    email
                )

        if not emails:

            return {}

        query = select(
            destination_users_table
        ).where(
            destination_users_table.c.email.in_(
                emails
            )
        )

        users_by_email = {}

        with self.dest_engine.connect() as dest_conn:

            for row in dest_conn.execute(
                query
            ):

                user = dict(
                    row._mapping
                )
                email = self._normalize(
                    user.get(
                        "email"
                    )
                )

                if email and email not in users_by_email:

                    users_by_email[
                        email
                    ] = user

        return users_by_email

    def _optional_reflect(
        self,
        table_name,
        engine,
        metadata
    ):

        inspector = inspect(
            engine
        )

        if not inspector.has_table(
            table_name
        ):

            logger.warning(
                f"Optional destination table "
                f"{table_name} not found; "
                "continuing without that lookup."
            )

            return None

        return self._manual_reflect(
            table_name,
            engine,
            metadata
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
            self.config.get("holds_migration_batch_size")
            or
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
        row_number,
        row_dict,
        source_table
    ):

        uuid_key = "|".join(
            [
                str(row_number),
                self._clean_string(
                    row_dict.get(
                        source_table.c.student_id
                    )
                ) or "",
                self._clean_string(
                    row_dict.get(
                        source_table.c.hold_code
                    )
                ) or "",
                self._clean_string(
                    row_dict.get(
                        source_table.c.hold_messages
                    )
                ) or "",
                self._clean_string(
                    row_dict.get(
                        source_table.c.hold_descriptions
                    )
                ) or "",
            ]
        )

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:holds:{uuid_key}"
            )
        )

    def _user_uuid(
        self,
        source_user
    ):

        source_jhi_user_id = source_user.get(
            "user_id"
        )

        if source_jhi_user_id is not None:

            return str(
                uuid.uuid5(
                    self.USER_UUID_NAMESPACE,
                    f"gl_user.user_id:{source_jhi_user_id}"
                )
            )

        return str(
            uuid.uuid5(
                self.USER_UUID_NAMESPACE,
                f"gl_user.id:{source_user.get('id')}"
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

    def _parse_datetime(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if not value:

            return None

        for date_format in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
        ):

            try:

                return datetime.strptime(
                    value,
                    date_format
                )

            except ValueError:

                continue

        try:

            return datetime.fromisoformat(
                value
            )

        except ValueError:

            return None

    def _source_user_value(
        self,
        source_user,
        column_name
    ):

        if not source_user:

            return None

        return self._clean_string(
            source_user.get(
                column_name
            )
        )

    def _best_user_name(
        self,
        source_user,
        student,
        source_student_id
    ):

        source_username = self._source_user_value(
            source_user,
            "username"
        )

        if source_username:

            return source_username

        first_name = self._student_value(
            student,
            "first_name"
        )
        last_name = self._student_value(
            student,
            "last_name"
        )
        full_name = self._clean_string(
            " ".join(
                value
                for value in (
                    first_name,
                    last_name
                )
                if value
            )
        )

        return (
            full_name
            or
            self._source_user_value(
                source_user,
                "email"
            )
            or
            self._student_value(
                student,
                "email"
            )
            or
            source_student_id
        )

    def _normalize(
        self,
        value
    ):

        return (
            str(value or "")
            .strip()
            .lower()
        )

    def _student_value(
        self,
        student,
        column_name
    ):

        if not student:

            return None

        return self._clean_string(
            student.get(
                column_name
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
