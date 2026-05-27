import json
import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    inspect,
    insert,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class NsapiPreferencesMigrator(BaseMigrator):

    SOURCE_TABLE = "nsapi_criteria"
    DESTINATION_TABLE = "scholarship_prefernces"
    INSERT_CHUNK_SIZE = 10000

    GENDER_MAPPING = {
        "male": 1,
        "female": 2,
        "other": 3,
        "prefer_not_to_say": 4,
        "prefer not to say": 4,
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
        self._destination_user_lookup = {}

    def migrate(self) -> int:

        logger.info(
            "Starting NSAPI Preferences Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        destination_table_name = self._resolve_destination_table_name()

        destination_table = self._manual_reflect(
            destination_table_name,
            self.dest_engine,
            self.metadata_dest
        )

        users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        source_users_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        logger.info(
            f"nsapi_criteria source columns: "
            f"{source_table.columns.keys()}"
        )

        logger.info(
            f"scholarship_prefernces destination columns: "
            f"{destination_table.columns.keys()}"
        )

        self._destination_user_lookup = (
            self._build_destination_user_lookup(
                users_table
            )
        )

        existing_student_uuids = (
            self._load_existing_student_uuids(
                destination_table
            )
        )

        logger.info(
            "Using NSAPI preferences insert chunk size: "
            f"{self.INSERT_CHUNK_SIZE}"
        )

        inserted_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        skipped_missing_destination_user = 0
        skipped_existing_student = 0
        row_error_count = 0

        last_source_id = 0
        remaining_limit = self.config.get("limit")

        if remaining_limit:

            remaining_limit = int(
                remaining_limit
            )

        while True:

            fetch_size = self.INSERT_CHUNK_SIZE

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
                    source_users_table.c.username
                )
                .select_from(
                    source_table.join(
                        source_users_table,
                        source_table.c.user_id
                        == source_users_table.c.id
                    )
                )
                .where(
                    source_table.c.id > last_source_id
                )
                .where(
                    source_users_table.c.username.is_not(None)
                )
                .where(
                    source_users_table.c.username != ""
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

            for row in rows:

                try:

                    row_dict = row._mapping

                    last_source_id = self._get_source_value(
                        row_dict,
                        source_table,
                        "id"
                    )

                    criteria = self._parse_criteria(
                        self._get_source_value(
                            row_dict,
                            source_table,
                            "criteria"
                        )
                    )

                    source_username = self._get_source_value(
                        row_dict,
                        source_users_table,
                        "username"
                    )

                    student_uuid = self._destination_user_lookup.get(
                        self._normalize(source_username)
                    )

                    if not student_uuid:

                        skipped_count += 1
                        skipped_missing_destination_user += 1

                        continue

                    if student_uuid in existing_student_uuids:

                        skipped_count += 1
                        skipped_existing_student += 1

                        continue

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            source_table,
                            "created_date"
                        )
                        or
                        datetime.utcnow()
                    )

                    updated_at = (
                        self._get_source_value(
                            row_dict,
                            source_table,
                            "modified_date"
                        )
                        or
                        created_at
                    )

                    mapped_row = {
                        "uuid": str(uuid.uuid4()),
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "deleted_at": None,
                        "student_uuid": student_uuid,
                        "age": self._to_int(
                            criteria.get("age"),
                            default=0
                        ),
                        "state": self._clean_value(criteria.get("state")),
                        "country": self._clean_value(criteria.get("country")),
                        "ethinicity": self._clean_value(
                            criteria.get("ethnicity")
                        ),
                        "citizenship_status": self._clean_value(
                            criteria.get("citizenship")
                        ),
                        "race": self._json_column_value(
                            destination_table,
                            "race",
                            criteria.get("race")
                        ),
                        "sat_total": self._to_int(
                            criteria.get("satTotal")
                        ),
                        "act_composite": self._to_int(
                            criteria.get("actTotal")
                        ),
                        "gpa": self._clean_value(criteria.get("gpa")),
                        "class_rank_percentile": self._clean_value(
                            criteria.get("rankPercentile")
                        ),
                        "grade_level": self._clean_value(
                            criteria.get("graddutionLevel")
                        ),
                        "graduation_status": self._clean_value(
                            criteria.get("graduationStatus")
                        ),
                        "scholarship_for": self._clean_value(
                            criteria.get("scholarshipFor")
                        ),
                        "working_as": self._json_column_value(
                            destination_table,
                            "working_as",
                            criteria.get("workingAs")
                        ),
                        "armed_service_background": self._clean_value(
                            criteria.get("armedServiceBackground")
                        ),
                        "service_status": self._clean_value(
                            criteria.get("serviceStatus")
                        ),
                        "interests": self._json_column_value(
                            destination_table,
                            "interests",
                            criteria.get("interests")
                        ),
                        "activities": self._json_column_value(
                            destination_table,
                            "activities",
                            criteria.get("activities")
                        ),
                        "created_by": student_uuid,
                        "updated_by": student_uuid,
                        "college_preferences": self._json_column_value(
                            destination_table,
                            "college_preferences",
                            criteria.get("collegeChoice")
                        ),
                        "intended_major": self._json_column_value(
                            destination_table,
                            "intended_major",
                            criteria.get("intendedMajor")
                        ),
                        "city": self._clean_value(criteria.get("city")),
                        "situation": self._json_column_value(
                            destination_table,
                            "situation",
                            criteria.get("situation")
                        ),
                        "gender": self._map_gender(
                            criteria.get("gender")
                            or
                            criteria.get("genderIdentity")
                        ),
                    }

                    insert_data.append(
                        self._filter_to_table_columns(
                            mapped_row,
                            destination_table
                        )
                    )

                    existing_student_uuids.add(
                        student_uuid
                    )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing NSAPI preference source id "
                        f"{last_source_id}: {error}"
                    )

            if not insert_data:

                logger.info(
                    "NSAPI preference chunk fetched "
                    f"{len(rows)} rows through source id "
                    f"{last_source_id}; nothing to insert."
                )

                continue

            logger.info(
                "Inserting NSAPI preference chunk: "
                f"prepared={len(insert_data)}, "
                f"source_id_through={last_source_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                result = dest_conn.execute(
                    insert(destination_table),
                    insert_data
                )

            inserted_now = result.rowcount or len(
                insert_data
            )

            inserted_count += inserted_now
            prepared_count += len(
                insert_data
            )

            logger.info(
                "NSAPI preference chunk inserted: "
                f"inserted_now={inserted_now}, "
                f"inserted_total={inserted_count}"
            )

        if not prepared_count:

            logger.warning(
                "No valid NSAPI preference records available for insertion"
            )

        logger.info(
            "NSAPI Preferences Migration summary: "
            f"inserted={inserted_count}, "
            f"skipped={skipped_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped_missing_destination_user="
            f"{skipped_missing_destination_user}, "
            f"skipped_existing_student={skipped_existing_student}, "
            f"row_errors={row_error_count}"
        )

        return inserted_count

    def _resolve_destination_table_name(self):

        inspector = inspect(
            self.dest_engine
        )

        table_names = inspector.get_table_names()

        for table_name in table_names:

            if (
                table_name.strip().lower()
                ==
                self.DESTINATION_TABLE.lower()
            ):

                return table_name

        close_matches = [
            table_name
            for table_name in table_names
            if any(
                token in table_name.strip().lower()
                for token in [
                    "scholarship",
                    "preference",
                    "nsapi"
                ]
            )
        ]

        raise ValueError(
            "Destination table "
            f"'{self.DESTINATION_TABLE}' was not found. "
            f"Close destination table matches: {close_matches}"
        )

    def _load_existing_student_uuids(
        self,
        destination_table
    ):

        existing_student_uuids = set()

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    destination_table.c.student_uuid
                ).where(
                    destination_table.c.deleted_at.is_(None)
                )
            ).fetchall()

        for row in rows:

            student_uuid = row._mapping.get(
                destination_table.c.student_uuid
            )

            if student_uuid:

                existing_student_uuids.add(
                    student_uuid
                )

        logger.info(
            "Loaded "
            f"{len(existing_student_uuids)} existing "
            "scholarship_prefernces student_uuid values "
            "for idempotent reruns."
        )

        return existing_student_uuids

    def _parse_criteria(
        self,
        value
    ):

        if value is None:

            return {}

        if isinstance(value, dict):

            return value

        if isinstance(value, bytes):

            value = value.decode(
                "utf-8"
            )

        text = str(value).strip()

        if not text:

            return {}

        parsed = json.loads(
            text
        )

        if isinstance(parsed, dict):

            return parsed

        return {}

    def _build_destination_user_lookup(
        self,
        users_table
    ):

        lookup = {}

        selected_columns = [
            users_table.c.uuid
        ]

        for column_name in [
            "user_name",
            "email"
        ]:

            if column_name in users_table.c:

                selected_columns.append(
                    users_table.c[column_name]
                )

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    *selected_columns
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping

            user_uuid = row_map.get(
                users_table.c.uuid
            )

            for column_name in [
                "user_name",
                "email"
            ]:

                if column_name not in users_table.c:

                    continue

                value = row_map.get(
                    users_table.c[column_name]
                )

                if value:

                    lookup[
                        self._normalize(value)
                    ] = user_uuid

        logger.info(
            f"Built {len(lookup)} destination NSAPI user lookups"
        )

        return lookup

    def _json_column_value(
        self,
        table,
        column_name,
        value
    ):

        value = self._clean_value(
            value
        )

        if value is None:

            return None

        column = table.c.get(
            column_name
        )

        if column is not None and "json" in column.type.__class__.__name__.lower():

            return value

        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":")
        )

    def _get_source_value(
        self,
        row,
        table,
        *column_names
    ):

        for column_name in column_names:

            if column_name in table.c:

                value = row.get(
                    table.c[column_name]
                )

                if value is not None:

                    return value

        return None

    def _filter_to_table_columns(
        self,
        row,
        table
    ):

        return {
            column_name: value
            for column_name, value in row.items()
            if column_name in table.c
        }

    def _map_gender(
        self,
        value
    ):

        if value is None:

            return 0

        normalized = self._normalize(
            value
        )

        return self.GENDER_MAPPING.get(
            normalized,
            0
        )

    def _to_int(
        self,
        value,
        default=None
    ):

        value = self._clean_value(
            value
        )

        if value in [
            None,
            ""
        ]:

            return default

        try:

            return int(
                value
            )

        except (
            TypeError,
            ValueError
        ):

            return default

    def _clean_value(
        self,
        value
    ):

        if isinstance(value, str) and value.strip().lower() in [
            "null",
            "none",
            ""
        ]:

            return None

        return value

    def _normalize(
        self,
        value
    ):

        if value is None:

            return ""

        return (
            str(value)
            .strip()
            .lower()
            .replace(" ", "")
        )
