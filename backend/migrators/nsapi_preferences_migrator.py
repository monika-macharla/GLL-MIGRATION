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

    STATE_MAPPING = {
        "AL": 1, "ALABAMA": 1,
        "AK": 2, "ALASKA": 2,
        "AS": 3, "AMERICAN_SAMOA": 3, "AMERICAN SAMOA": 3,
        "AZ": 4, "ARIZONA": 4,
        "AR": 5, "ARKANSAS": 5,
        "CA": 6, "CALIFORNIA": 6,
        "CO": 7, "COLORADO": 7,
        "CT": 8, "CONNECTICUT": 8,
        "DE": 9, "DELAWARE": 9,
        "DC": 10, "DISTRICT_OF_COLUMBIA": 10, "DISTRICT OF COLUMBIA": 10,
        "FM": 11, "FEDERATED_STATES_OF_MICRONESIA": 11, "FEDERATED STATES OF MICRONESIA": 11,
        "FL": 12, "FLORIDA": 12,
        "GA": 13, "GEORGIA": 13,
        "GU": 14, "GUAM": 14,
        "HI": 15, "HAWAII": 15,
        "ID": 16, "IDAHO": 16,
        "IL": 17, "ILLINOIS": 17,
        "IN": 18, "INDIANA": 18,
        "IA": 19, "IOWA": 19,
        "KS": 20, "KANSAS": 20,
        "KY": 21, "KENTUCKY": 21,
        "LA": 22, "LOUISIANA": 22,
        "ME": 23, "MAINE": 23,
        "MH": 24, "MARSHALL_ISLANDS": 24, "MARSHALL ISLANDS": 24,
        "MD": 25, "MARYLAND": 25,
        "MA": 26, "MASSACHUSETTS": 26,
        "MI": 27, "MICHIGAN": 27,
        "MN": 28, "MINNESOTA": 28,
        "MS": 29, "MISSISSIPPI": 29,
        "MO": 30, "MISSOURI": 30,
        "MT": 31, "MONTANA": 31,
        "NE": 32, "NEBRASKA": 32,
        "NV": 33, "NEVADA": 33,
        "NH": 34, "NEW_HAMPSHIRE": 34, "NEW HAMPSHIRE": 34,
        "NJ": 35, "NEW_JERSEY": 35, "NEW JERSEY": 35,
        "NM": 36, "NEW_MEXICO": 36, "NEW MEXICO": 36,
        "NY": 37, "NEW_YORK": 37, "NEW YORK": 37,
        "NC": 38, "NORTH_CAROLINA": 38, "NORTH CAROLINA": 38,
        "ND": 39, "NORTH_DAKOTA": 39, "NORTH DAKOTA": 39,
        "MP": 40, "NORTHERN_MARIANA_ISLANDS": 40, "NORTHERN MARIANA ISLANDS": 40,
        "OH": 41, "OHIO": 41,
        "OK": 42, "OKLAHOMA": 42,
        "OR": 43, "OREGON": 43,
        "PW": 44, "PALAU": 44,
        "PA": 45, "PENNSYLVANIA": 45,
        "PR": 46, "PUERTO_RICO": 46, "PUERTO RICO": 46,
        "RI": 47, "RHODE_ISLAND": 47, "RHODE ISLAND": 47,
        "SC": 48, "SOUTH_CAROLINA": 48, "SOUTH CAROLINA": 48,
        "SD": 49, "SOUTH_DAKOTA": 49, "SOUTH DAKOTA": 49,
        "TN": 50, "TENNESSEE": 50,
        "TX": 51, "TEXAS": 51,
        "UT": 52, "UTAH": 52,
        "VT": 53, "VERMONT": 53,
        "VI": 54, "VIRGIN_ISLANDS": 54, "VIRGIN ISLANDS": 54,
        "VA": 55, "VIRGINIA": 55,
        "WA": 56, "WASHINGTON": 56,
        "WV": 57, "WEST_VIRGINIA": 57, "WEST VIRGINIA": 57,
        "WI": 58, "WISCONSIN": 58,
        "WY": 59, "WYOMING": 59,
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
        self._college_code_lookup = {}

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

        self._college_code_lookup = self._build_college_code_lookup()

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
                        "state": self._map_state(
                            criteria.get("state")
                        ),
                        "country": 1,
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
                            self._build_value_subvalue_preferences(
                                criteria.get("collegeChoice"),
                                value_keys=[
                                    "value",
                                    "name",
                                    "college",
                                    "collegeName",
                                    "institution",
                                    "institutionName",
                                    "schoolName",
                                    "school",
                                    "label",
                                    "title"
                                ],
                                sub_value_keys=[
                                    "subValue",
                                    "code",
                                    "collegeCode",
                                    "institutionCode",
                                    "schoolCode",
                                    "schoolId",
                                    "ceebCode",
                                    "ceeb",
                                    "ipeds",
                                    "opeId",
                                    "id"
                                ]
                            )
                        ),
                        "intended_major": self._json_column_value(
                            destination_table,
                            "intended_major",
                            self._build_value_subvalue_preferences(
                                criteria.get("intendedMajor"),
                                value_keys=[
                                    "value",
                                    "name",
                                    "major",
                                    "majorName",
                                    "cipTitle",
                                    "label",
                                    "title"
                                ],
                                sub_value_keys=[
                                    "subValue",
                                    "code",
                                    "majorCode",
                                    "cipCode",
                                    "cip",
                                    "id"
                                ]
                            )
                        ),
                        "city": self._string_value(
                            criteria.get("city")
                        ),
                        "situation": self._json_column_value(
                            destination_table,
                            "situation",
                            self._situation_value(
                                criteria.get("situation")
                            )
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

    def _build_college_code_lookup(self):

        lookup = {}

        self._add_college_code_lookup_rows(
            lookup,
            self.source_engine,
            self.metadata_source,
            "institution",
            name_columns=[
                "name",
                "alias_name",
                "edi_name"
            ],
            code_columns=[
                "qual_code",
                "school_code",
                "nsc_receiver_id"
            ]
        )

        self._add_college_code_lookup_rows(
            lookup,
            self.dest_engine,
            self.metadata_dest,
            "institutions",
            name_columns=[
                "name"
            ],
            code_columns=[
                "qual_code",
                "nsc_receiver_id"
            ]
        )

        logger.info(
            f"Built {len(lookup)} college preference code lookups"
        )

        return lookup

    def _add_college_code_lookup_rows(
        self,
        lookup,
        engine,
        metadata,
        table_name,
        name_columns,
        code_columns
    ):

        inspector = inspect(
            engine
        )

        if table_name not in inspector.get_table_names():

            return

        table = self._manual_reflect(
            table_name,
            engine,
            metadata
        )

        available_name_columns = [
            column_name
            for column_name in name_columns
            if column_name in table.c
        ]
        available_code_columns = [
            column_name
            for column_name in code_columns
            if column_name in table.c
        ]

        if not available_name_columns or not available_code_columns:

            return

        selected_columns = [
            table.c[column_name]
            for column_name in (
                available_name_columns
                + available_code_columns
            )
        ]

        with engine.connect() as conn:

            rows = conn.execute(
                select(
                    *selected_columns
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping
            code = None

            for column_name in available_code_columns:

                code = self._clean_value(
                    row_map.get(
                        table.c[column_name]
                    )
                )

                if code is not None:

                    break

            if code is None:

                continue

            for column_name in available_name_columns:

                name = self._clean_value(
                    row_map.get(
                        table.c[column_name]
                    )
                )

                if name is None:

                    continue

                lookup.setdefault(
                    self._normalize_college_name(
                        name
                    ),
                    code
                )

    def _normalize_college_name(
        self,
        value
    ):

        text = str(
            value or ""
        ).lower()

        text = text.replace(
            "&",
            "and"
        )

        return "".join(
            character
            for character in text
            if character.isalnum()
        )

    def _map_state(
        self,
        state
    ):

        state = self._clean_value(
            state
        )

        if state is None:

            return 0

        if isinstance(state, int):

            return state if 0 <= state <= 59 else 0

        state_key = str(
            state
        ).strip().upper()

        if state_key.isdigit():

            state_number = int(
                state_key
            )

            return state_number if 0 <= state_number <= 59 else 0

        return self.STATE_MAPPING.get(
            state_key,
            self.STATE_MAPPING.get(
                state_key.replace(
                    " ",
                    "_"
                ),
                0
            )
        )

    def _string_value(
        self,
        value
    ):

        value = self._clean_value(
            value
        )

        if value is None:

            return ""

        if isinstance(value, (list, dict)):

            return json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":")
            )

        return str(
            value
        )

    def _situation_value(
        self,
        value
    ):

        values = self._as_list(
            value
        )

        cleaned_values = [
            str(item)
            for item in values
            if self._clean_value(item) is not None
        ]

        return cleaned_values or [
            " "
        ]

    def _build_value_subvalue_preferences(
        self,
        value,
        value_keys,
        sub_value_keys
    ):

        preferences = []

        for item in self._as_list(value):

            if isinstance(item, dict):

                preference_value = self._first_present(
                    item,
                    value_keys
                )
                preference_sub_value = self._first_present(
                    item,
                    sub_value_keys
                )

                if preference_value is None:

                    preference_value, preference_sub_value = (
                        self._value_subvalue_from_mapping(
                            item
                        )
                    )

            else:

                preference_value, preference_sub_value = (
                    self._value_subvalue_from_text(
                        item
                    )
                )

            preference_value = self._clean_value(
                preference_value
            )
            preference_sub_value = self._clean_value(
                preference_sub_value
            )

            if preference_value is None:

                continue

            if preference_sub_value in [
                None,
                ""
            ]:

                preference_sub_value = (
                    self._college_code_lookup.get(
                        self._normalize_college_name(
                            preference_value
                        )
                    )
                )

            preferences.append({
                "value": str(
                    preference_value
                ),
                "subValue": (
                    ""
                    if preference_sub_value is None
                    else str(preference_sub_value)
                ),
            })

        return preferences

    def _value_subvalue_from_mapping(
        self,
        item
    ):

        values = [
            self._clean_value(value)
            for value in item.values()
            if self._clean_value(value) is not None
        ]

        if not values:

            return None, None

        display_values = [
            value
            for value in values
            if not self._looks_like_code(value)
        ]

        code_values = [
            value
            for value in values
            if self._looks_like_code(value)
        ]

        if display_values:

            return display_values[0], (
                code_values[0]
                if code_values
                else ""
            )

        return values[0], (
            values[1]
            if len(values) > 1
            else ""
        )

    def _value_subvalue_from_text(
        self,
        value
    ):

        value = self._clean_value(
            value
        )

        if value is None:

            return None, None

        text = str(
            value
        ).strip()

        for delimiter in [
            "|",
            "::",
            " - "
        ]:

            if delimiter not in text:

                continue

            left, right = [
                part.strip()
                for part in text.split(
                    delimiter,
                    1
                )
            ]

            if self._looks_like_code(left) and not self._looks_like_code(right):

                return right, left

            return left, right

        return text, ""

    def _looks_like_code(
        self,
        value
    ):

        text = str(
            value
        ).strip()

        if not text:

            return False

        return all(
            character.isdigit()
            or character == "."
            for character in text
        )

    def _as_list(
        self,
        value
    ):

        value = self._clean_value(
            value
        )

        if value is None:

            return []

        if isinstance(value, list):

            return value

        if isinstance(value, tuple):

            return list(
                value
            )

        if isinstance(value, dict):

            return [
                value
            ]

        if isinstance(value, bytes):

            value = value.decode(
                "utf-8"
            )

        text = str(
            value
        ).strip()

        if not text:

            return []

        try:

            parsed = json.loads(
                text
            )

            if isinstance(parsed, list):

                return parsed

            if parsed is None:

                return []

            return [
                parsed
            ]

        except json.JSONDecodeError:

            return [
                item.strip()
                for item in text.split(",")
                if item.strip()
            ]

    def _first_present(
        self,
        item,
        keys
    ):

        normalized_item = {
            self._normalize(key): value
            for key, value in item.items()
        }

        for key in keys:

            value = normalized_item.get(
                self._normalize(key)
            )

            if self._clean_value(value) is not None:

                return value

        return None

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
