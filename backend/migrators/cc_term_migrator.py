import logging
import re
import uuid

from datetime import datetime

from sqlalchemy import func, inspect, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CCTermMigrator(BaseMigrator):

    SOURCE_TABLE = "semester"
    DESTINATION_TABLE = "import_edi_semester"
    DESTINATION_TABLE_ALIAS = "import_edi_semesters"
    DEFAULT_AUTH_DATABASE = "gllauthservicenew"
    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
    DEFAULT_BATCH_SIZE = 5000
    MAX_BATCH_SIZE = 10000
    # Regex to strip parenthesized date portions from term
    # e.g. "FALL 2019 (08/26/2019-12/13/2019)" -> "FALL 2019"
    PAREN_STRIP_RE = re.compile(r"\(.*?\)")

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
            "Starting Semester Import Migration "
            "(semester -> import_edi_semester)..."
        )

        destination_table_name = self._resolve_table_name(
            self.dest_engine,
            self.DESTINATION_TABLE,
            self.DESTINATION_TABLE_ALIAS
        )

        # -----------------------------------------
        # Reflect source tables:
        #   semester (gll_prod_new.semester)
        #   transcript (gll_prod_new.transcript)
        # -----------------------------------------

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
            destination_table_name,
            self.dest_engine,
            self.metadata_dest
        )

        # -----------------------------------------
        # Load institution ID -> UUID lookup
        # Maps Java integer institution_id to the
        # TypeScript UUID from auth service
        # -----------------------------------------

        institution_id_to_uuid = (
            self._load_institution_id_to_uuid_lookup()
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
            f"Source semester count: {source_count}"
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
        invalid_end_date = 0
        skipped_not_latest_transcript = 0
        batch_number = 0

        # -----------------------------------------
        # Pre-compute latest transcript ID per
        # (stu_identification, institution_id)
        # to avoid duplicate rows from multiple
        # transcript records per student.
        # Equivalent to:
        #   WHERE t.id = (
        #     SELECT MAX(t2.id)
        #     FROM transcript t2
        #     WHERE t2.stu_identification = t.stu_identification
        #       AND t2.institution_id = t.institution_id
        #   )
        # -----------------------------------------

        latest_transcript_ids = (
            self._load_latest_transcript_ids(transcript_table)
        )

        logger.info(
            f"Pre-computed latest transcript IDs for "
            f"{len(latest_transcript_ids)} "
            f"(student, institution) pairs"
        )

        while True:

            fetch_size = batch_size

            if remaining_limit is not None:

                if remaining_limit <= 0:

                    break

                fetch_size = min(
                    fetch_size,
                    remaining_limit
                )

            # -----------------------------------------
            # Query: semester JOIN transcript
            # ON transcript.id = semester.transcript_id
            # -----------------------------------------

            query = (
                select(
                    source_table.c.id,
                    source_table.c.transcript_id,
                    source_table.c.term,
                    source_table.c.start_date,
                    source_table.c.end_date,
                    source_table.c.credit_hrs,
                    source_table.c.gpa,
                    source_table.c.year,
                    transcript_table.c.stu_identification,
                    transcript_table.c.institution_id,
                )
                .select_from(
                    source_table
                    .join(
                        transcript_table,
                        transcript_table.c.id
                        == source_table.c.transcript_id
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

                transcript_id = row_dict.get(
                    source_table.c.transcript_id
                )

                student_number = self._clean_string(
                    row_dict.get(
                        transcript_table.c.stu_identification
                    )
                )

                source_institution_id = row_dict.get(
                    transcript_table.c.institution_id
                )

                # -----------------------------------------
                # Deduplication: skip if this transcript
                # is NOT the latest one for this student
                # at this institution.
                # -----------------------------------------

                dedup_key = (
                    str(student_number or ""),
                    source_institution_id
                )

                latest_tid = latest_transcript_ids.get(
                    dedup_key
                )

                if (
                    latest_tid is not None
                    and transcript_id != latest_tid
                ):

                    skipped_not_latest_transcript += 1
                    continue

                if not student_number:

                    missing_student_number += 1
                    student_number = (
                        f"SEMESTER-TRANSCRIPT-"
                        f"{transcript_id}"
                    )

                # -----------------------------------------
                # Institution ID resolution:
                # Look up the TypeScript UUID from the
                # auth service using the Java integer ID.
                # -----------------------------------------

                destination_institution_uuid = None

                if source_institution_id is not None:

                    destination_institution_uuid = (
                        institution_id_to_uuid.get(
                            int(source_institution_id)
                        )
                    )

                if not destination_institution_uuid:

                    missing_institution_id += 1
                    logger.debug(
                        f"No institution UUID found for "
                        f"source institution_id="
                        f"{source_institution_id}, "
                        f"semester source_id={source_id}"
                    )

                start_date = self._parse_date(
                    row_dict.get(
                        source_table.c.start_date
                    )
                )
                end_date = self._parse_date(
                    row_dict.get(
                        source_table.c.end_date
                    )
                )

                if (
                    start_date is None
                    and
                    self._clean_string(
                        row_dict.get(
                            source_table.c.start_date
                        )
                    )
                ):

                    invalid_start_date += 1

                if (
                    end_date is None
                    and
                    self._clean_string(
                        row_dict.get(
                            source_table.c.end_date
                        )
                    )
                ):

                    invalid_end_date += 1

                # -----------------------------------------
                # Derive session_name by stripping
                # parenthesized date portions from term.
                # e.g. "FALL 2019 (08/26/2019-12/13/2019)"
                # becomes "FALL 2019"
                # -----------------------------------------

                term_value = self._clean_string(
                    row_dict.get(
                        source_table.c.term
                    )
                )

                session_name = None
                if term_value:
                    session_name = self.PAREN_STRIP_RE.sub(
                        "", term_value
                    ).strip()

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
                    ),
                    "semester_id": self._stable_semester_uuid(
                        source_id
                    ),
                    "gpa": self._float_value(
                        row_dict.get(
                            source_table.c.gpa
                        )
                    ),
                    "credit_hrs": self._integer_value(
                        row_dict.get(
                            source_table.c.credit_hrs
                        )
                    ),
                    "term": self._truncate(
                        term_value,
                        255
                    ),
                    "year": self._truncate(
                        row_dict.get(
                            source_table.c.year
                        ),
                        45
                    ),
                    "start_date": start_date,
                    "end_date": end_date,
                    "session_name": self._truncate(
                        session_name,
                        255
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
            "Semester Import Summary: "
            f"source_count={source_count}, "
            f"destination_start_count={destination_count}, "
            f"fetched={fetched_count}, "
            f"prepared={prepared_count}, "
            f"inserted={inserted_count}, "
            f"ignored_existing={ignored_existing}, "
            f"missing_student_number={missing_student_number}, "
            f"missing_institution_id={missing_institution_id}, "
            f"invalid_start_date={invalid_start_date}, "
            f"invalid_end_date={invalid_end_date}, "
            f"skipped_not_latest_transcript="
            f"{skipped_not_latest_transcript}"
        )

        return inserted_count

    # -----------------------------------------
    # Load latest transcript ID per
    # (stu_identification, institution_id)
    # -----------------------------------------

    def _load_latest_transcript_ids(
        self,
        transcript_table
    ):
        """
        Returns a dict mapping
        (stu_identification, institution_id) -> max(id)
        so we only pick semesters from the latest
        transcript per student per institution.
        """

        query = (
            select(
                transcript_table.c.stu_identification,
                transcript_table.c.institution_id,
                func.max(
                    transcript_table.c.id
                ).label("max_id")
            )
            .where(
                transcript_table.c.stu_identification.isnot(
                    None
                )
            )
            .group_by(
                transcript_table.c.stu_identification,
                transcript_table.c.institution_id
            )
        )

        with self.source_engine.connect() as conn:

            rows = conn.execute(query).fetchall()

        lookup = {}

        for row in rows:

            row_dict = row._mapping
            stu_id = self._clean_string(
                row_dict.get("stu_identification")
            )
            inst_id = row_dict.get("institution_id")
            max_id = row_dict.get("max_id")

            if stu_id is not None:

                lookup[
                    (str(stu_id), inst_id)
                ] = max_id

        return lookup

    # -----------------------------------------
    # Load institution integer ID -> UUID lookup
    # from auth service database
    # -----------------------------------------

    def _load_institution_id_to_uuid_lookup(self):
        """
        Build a mapping from the Java integer
        institution.id (source) to the TypeScript
        UUID in the auth service institutions table.

        Strategy:
        1. Load all institutions from source DB
           (gll_prod_new.institution) -> {id: name}
        2. Load all institutions from auth DB
           (gllauthservicenew.institutions) -> {name: uuid}
        3. Join by normalized name to produce
           {source_int_id: auth_uuid}
        """

        # Step 1: Load source institution id -> name
        source_id_to_name = {}

        try:

            source_institution_table = self._manual_reflect(
                "institution",
                self.source_engine,
                self.metadata_source
            )

            with self.source_engine.connect() as conn:

                rows = conn.execute(
                    select(
                        source_institution_table.c.id,
                        source_institution_table.c.name
                    )
                ).fetchall()

            for row in rows:

                row_dict = row._mapping
                inst_id = row_dict.get("id")
                inst_name = self._clean_string(
                    row_dict.get("name")
                )

                if inst_id is not None and inst_name:

                    source_id_to_name[
                        int(inst_id)
                    ] = inst_name

            logger.info(
                f"Loaded {len(source_id_to_name)} "
                f"source institutions (id -> name)"
            )

        except Exception as exc:

            logger.warning(
                f"Failed loading source institutions: {exc}"
            )

        # Step 2: Load auth institution name -> uuid
        auth_name_to_uuid = {}

        auth_rows = self._load_auth_institution_rows()

        for row in auth_rows:

            name = self._normalize(
                row.get("name")
            )
            inst_uuid = row.get("uuid")

            if name and inst_uuid:

                auth_name_to_uuid[name] = str(inst_uuid)

        logger.info(
            f"Loaded {len(auth_name_to_uuid)} "
            f"auth institutions (name -> uuid)"
        )

        # Step 3: Join by normalized name
        id_to_uuid = {}

        for source_id, source_name in source_id_to_name.items():

            normalized = self._normalize(source_name)

            if normalized in auth_name_to_uuid:

                id_to_uuid[source_id] = (
                    auth_name_to_uuid[normalized]
                )

        logger.info(
            f"Resolved {len(id_to_uuid)} "
            f"institution ID -> UUID mappings"
        )

        # Log any unmatched source institutions
        unmatched = set(source_id_to_name.keys()) - set(
            id_to_uuid.keys()
        )

        if unmatched:

            for uid in sorted(unmatched):

                logger.warning(
                    f"No auth UUID match for source "
                    f"institution id={uid}, "
                    f"name='{source_id_to_name[uid]}'"
                )

        return id_to_uuid

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
                f"gll:import-edi-semester:{source_id}"
            )
        )

    def _stable_semester_uuid(
        self,
        source_id
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:semester-id:{source_id}"
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

    def _parse_date(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if value is None:

            return None

        for date_format in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y/%m/%d",
        ):

            try:

                return datetime.strptime(
                    value,
                    date_format
                ).date()

            except ValueError:

                continue

        return None

    def _float_value(
        self,
        value
    ):

        value = self._clean_string(
            value
        )

        if value is None:

            return None

        try:

            return float(
                value
            )

        except ValueError:

            return None

    def _integer_value(
        self,
        value
    ):

        float_value = self._float_value(
            value
        )

        if float_value is None:

            return None

        return int(
            round(
                float_value
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
