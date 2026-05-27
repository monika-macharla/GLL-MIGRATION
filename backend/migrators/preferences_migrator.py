import hashlib
import json
import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    JSON,
    insert,
    inspect,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class PreferencesMigrator(BaseMigrator):

    SOURCE_TABLE = "student_preference"
    DESTINATION_TABLE = "my_preferences"
    INSERT_CHUNK_SIZE = 10000

    PREFERENCE_ITEM_TYPE = "auth.service.v1.PreferenceItem"
    EDUCATION_PREFERENCE_TYPE = "auth.service.v1.EducationPreference"

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
        self._reference_cache = None
        self._destination_user_lookup = {}
        self._institution_lookup = {}
        self._all_institution_names = []
        self._source_user_uuid_cache = {}

    def migrate(self) -> int:

        logger.info(
            "Starting Preferences Migration..."
        )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        preferences_table = self._manual_reflect(
            self.DESTINATION_TABLE,
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

        institutions_table = None

        if "institutions" in inspect(
            self.dest_engine
        ).get_table_names():

            institutions_table = self._manual_reflect(
                "institutions",
                self.dest_engine,
                self.metadata_dest
            )

        logger.info(
            f"student_preference columns: "
            f"{source_table.columns.keys()}"
        )

        logger.info(
            f"my_preferences columns: "
            f"{preferences_table.columns.keys()}"
        )

        logger.info(
            f"Using preferences insert chunk size: "
            f"{self.INSERT_CHUNK_SIZE}"
        )

        self._destination_user_lookup = (
            self._build_destination_user_lookup(
                users_table
            )
        )

        self._institution_lookup = (
            self._build_institution_lookup(
                institutions_table
            )
        )

        self._all_institution_names = [
            institution["name"]
            for institution in self._institution_lookup.values()
            if institution.get("name")
        ]

        existing_user_ids = set()

        with self.dest_engine.connect() as dest_conn:

            existing_rows = dest_conn.execute(
                select(
                    preferences_table.c.user_id
                ).where(
                    preferences_table.c.deleted_at.is_(None)
                )
            ).fetchall()

            for existing_row in existing_rows:

                existing_user_id = existing_row._mapping.get(
                    preferences_table.c.user_id
                )

                if existing_user_id:

                    existing_user_ids.add(
                        existing_user_id
                    )

        logger.info(
            f"Loaded {len(existing_user_ids)} existing "
            f"my_preferences user_id values for idempotent reruns."
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
                source_users_table.c.username.is_not(None)
            )
            .where(
                source_users_table.c.username != ""
            )
        )

        if self.config.get("limit"):

            query = query.limit(
                self.config["limit"]
            )

        with self.source_engine.connect() as source_conn:

            rows = source_conn.execute(
                query
            ).fetchall()

        logger.info(
            f"Fetched {len(rows)} source student_preference rows "
            f"with resolvable gl_user usernames."
        )

        insert_data = []

        skipped_count = 0
        skipped_missing_destination_user = 0
        skipped_existing_user = 0
        row_error_count = 0

        for index, row in enumerate(
            rows,
            start=1
        ):

            try:

                row_dict = row._mapping

                source_username = self._get_source_value(
                    row_dict,
                    source_users_table,
                    "username"
                )

                destination_user_uuid = (
                    self._destination_user_lookup.get(
                        self._normalize(source_username)
                    )
                )

                if not destination_user_uuid:

                    skipped_count += 1
                    skipped_missing_destination_user += 1

                    continue

                if destination_user_uuid in existing_user_ids:

                    skipped_count += 1
                    skipped_existing_user += 1

                    continue

                job_names = self._parse_list(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "job_preferences"
                    )
                )

                communication_names = self._parse_list(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "communication_preferences"
                    )
                )

                major_names = self._parse_list(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "major"
                    )
                )

                career_names = self._parse_list(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "industry_type"
                    )
                )

                education_names = self._parse_list(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "education_preferences",
                        "education",
                        "institution_preferences",
                        "institution"
                    )
                )

                education_all = self._is_truthy(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "education_all"
                    )
                )

                if education_all:

                    education_names = [
                        "All"
                    ]

                if self._is_truthy(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "employee_all"
                    )
                ) and not career_names:

                    career_names = []

                created_at = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "created_at",
                        "created_date"
                    )
                    or
                    datetime.utcnow()
                )

                updated_at = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "updated_at",
                        "last_modified_date"
                    )
                    or
                    created_at
                )

                if education_all:

                    education_preferences = self._build_preference_items(
                        education_names,
                        "education"
                    )

                else:

                    education_preferences = (
                        self._build_education_preferences(
                            education_names,
                            institutions_table
                        )
                    )

                mapped_row = {
                    "uuid": str(uuid.uuid4()),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": None,
                    "education_preferences": self._json_column_value(
                        preferences_table,
                        "education_preferences",
                        education_preferences
                    ),
                    "job_preferences": self._json_column_value(
                        preferences_table,
                        "job_preferences",
                        self._build_preference_items(
                            job_names,
                            "job"
                        )
                    ),
                    "communication_preferences": self._json_column_value(
                        preferences_table,
                        "communication_preferences",
                        self._build_preference_items(
                            communication_names,
                            "communication"
                        )
                    ),
                    "major_preferences": self._json_column_value(
                        preferences_table,
                        "major_preferences",
                        self._build_preference_items(
                            major_names,
                            "major"
                        )
                    ),
                    "career_preferences": self._json_column_value(
                        preferences_table,
                        "career_preferences",
                        self._build_preference_items(
                            career_names,
                            "career"
                        )
                    ),
                    "user_id": destination_user_uuid,
                    "created_by": None,
                }

                insert_data.append(
                    self._filter_to_table_columns(
                        mapped_row,
                        preferences_table
                    )
                )

                existing_user_ids.add(
                    destination_user_uuid
                )

            except Exception as error:

                skipped_count += 1
                row_error_count += 1

                logger.exception(
                    f"Failed processing preference row "
                    f"{index}: {error}"
                )

            if index % self.INSERT_CHUNK_SIZE == 0:

                logger.info(
                    f"Prepared preferences progress: "
                    f"processed={index}, "
                    f"prepared={len(insert_data)}, "
                    f"skipped={skipped_count}."
                )

        logger.info(
            f"Prepared preferences rows: "
            f"source_rows={len(rows)}, "
            f"prepared={len(insert_data)}, "
            f"skipped={skipped_count}, "
            f"missing_destination_user="
            f"{skipped_missing_destination_user}, "
            f"existing_user={skipped_existing_user}, "
            f"errors={row_error_count}."
        )

        if not insert_data:

            logger.warning(
                "No valid preference records available for insertion"
            )

            return 0

        inserted_count = 0

        total_chunks = (
            (len(insert_data) + self.INSERT_CHUNK_SIZE - 1)
            // self.INSERT_CHUNK_SIZE
        )

        for chunk_number, chunk_start in enumerate(
            range(
                0,
                len(insert_data),
                self.INSERT_CHUNK_SIZE
            ),
            start=1
        ):


            chunk = insert_data[
                chunk_start:chunk_start + self.INSERT_CHUNK_SIZE
            ]

            chunk_end = chunk_start + len(
                chunk
            )

            logger.info(
                f"Preferences insert chunk "
                f"{chunk_number}/{total_chunks}: rows "
                f"{chunk_start + 1}-{chunk_end} "
                f"of {len(insert_data)}"
            )

            with self.dest_engine.begin() as dest_conn:

                result = dest_conn.execute(
                    insert(preferences_table),
                    chunk
                )

            inserted_count += result.rowcount or len(
                chunk
            )

            logger.info(
                f"Preferences insert chunk "
                f"{chunk_number}/{total_chunks} complete; "
                f"inserted_total={inserted_count}."
            )

        logger.info(
            "Preferences Migration summary: "
            f"inserted={inserted_count}, "
            f"skipped={skipped_count}, "
            f"prepared={len(insert_data)}, "
            f"source_rows={len(rows)}, "
            f"missing_destination_user="
            f"{skipped_missing_destination_user}, "
            f"existing_user={skipped_existing_user}, "
            f"errors={row_error_count}"
        )

        return inserted_count

    def _resolve_user_uuid(
        self,
        source_user_id,
        users_table
    ):

        if not source_user_id:

            return None

        if source_user_id in self._source_user_uuid_cache:

            return self._source_user_uuid_cache[
                source_user_id
            ]

        source_user = self.fetch_one_by_column(
            self.source_engine,
            "gl_user",
            "id",
            source_user_id
        )

        if not source_user:

            source_student = self.fetch_one_by_column(
                self.source_engine,
                "gl_student",
                "id",
                source_user_id
            )

            if source_student:

                source_user = self.fetch_one_by_column(
                    self.source_engine,
                    "gl_user",
                    "id",
                    source_student.get("user_id")
                )

        if not source_user:

            source_user = self.fetch_one_by_column(
                self.source_engine,
                "jhi_user",
                "id",
                source_user_id
            )

        source_username = self._get_row_value(
            source_user,
            "username",
            "login",
            "email"
        )

        if not source_username:

            self._source_user_uuid_cache[
                source_user_id
            ] = None

            return None

        destination_user_uuid = self._destination_user_lookup.get(
            self._normalize(source_username)
        )

        self._source_user_uuid_cache[
            source_user_id
        ] = destination_user_uuid

        return destination_user_uuid

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
            f"Built {len(lookup)} destination user preference lookups"
        )

        return lookup

    def _build_preference_items(
        self,
        names,
        preference_kind
    ):

        items = []

        for name in names:

            clean_name = str(
                name or ""
            ).strip()

            if not clean_name:

                continue

            items.append(
                {
                    "name": clean_name,
                    "uuid": self._resolve_preference_uuid(
                        clean_name,
                        preference_kind
                    ),
                    "category": 0,
                    "$typeName": self.PREFERENCE_ITEM_TYPE,
                }
            )

        return items

    def _build_education_preferences(
        self,
        names,
        institutions_table
    ):

        items = []

        for name in names:

            clean_name = str(
                name or ""
            ).strip()

            if not clean_name:

                continue

            institution = self._find_institution(
                clean_name,
                institutions_table
            )

            items.append(
                {
                    "name": (
                        institution.get("name")
                        if institution
                        else clean_name
                    ),
                    "type": (
                        institution.get("type")
                        if institution
                        else "university"
                    ),
                    "uuid": (
                        institution.get("uuid")
                        if institution
                        else self._stable_uuid(
                            "education",
                            clean_name
                        )
                    ),
                    "$typeName": self.EDUCATION_PREFERENCE_TYPE,
                    "parentInstitutionId": (
                        institution.get("parentInstitutionId")
                        if institution
                        else 0
                    ),
                }
            )

        return items

    def _find_institution(
        self,
        name,
        institutions_table
    ):

        return self._institution_lookup.get(
            self._normalize(
                name
            )
        )

    def _build_institution_lookup(
        self,
        institutions_table
    ):

        lookup = {}

        if institutions_table is None or "name" not in institutions_table.c:

            return lookup

        selected_columns = [
            institutions_table.c.name
        ]

        for column_name in [
            "uuid",
            "type",
            "parent_id"
        ]:

            if column_name in institutions_table.c:

                selected_columns.append(
                    institutions_table.c[column_name]
                )

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    *selected_columns
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping

            institution_name = row_map.get(
                institutions_table.c.name
            )

            if not institution_name:

                continue

            lookup[
                self._normalize(institution_name)
            ] = {
                "name": institution_name,
                "type": self._map_education_type(
                    row_map.get(
                        institutions_table.c.type
                    )
                    if "type" in institutions_table.c
                    else None
                ),
                "uuid": str(
                    row_map.get(
                        institutions_table.c.uuid
                    )
                    if "uuid" in institutions_table.c
                    else self._stable_uuid(
                        "education",
                        institution_name
                    )
                ),
                "parentInstitutionId": (
                    row_map.get(
                        institutions_table.c.parent_id
                    )
                    if "parent_id" in institutions_table.c
                    else 0
                ) or 0,
            }

        logger.info(
            f"Built {len(lookup)} institution preference lookups"
        )

        return lookup

    def _old_find_institution_unused(
        self,
        name,
        institutions_table
    ):

        normalized_name = self._normalize(
            name
        )

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    institutions_table
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping

            if "name" not in institutions_table.c:

                continue

            if (
                self._normalize(
                    row_map.get(
                        institutions_table.c.name
                    )
                )
                == normalized_name
            ):

                return {
                    "name": row_map.get(
                        institutions_table.c.name
                    ),
                    "type": self._map_education_type(
                        row_map.get(
                            institutions_table.c.type
                        )
                        if "type" in institutions_table.c
                        else None
                    ),
                    "uuid": str(
                        row_map.get(
                            institutions_table.c.uuid
                        )
                    ),
                    "parentInstitutionId": (
                        row_map.get(
                            institutions_table.c.parent_id
                        )
                        if "parent_id" in institutions_table.c
                        else 0
                    ) or 0,
                }

        return None

    def _get_all_institution_names(
        self,
        institutions_table
    ):

        return self._all_institution_names

    def _resolve_preference_uuid(
        self,
        name,
        preference_kind
    ):

        if self._reference_cache is None:

            self._reference_cache = (
                self._build_reference_cache()
            )

        normalized_name = self._normalize(
            name
        )

        cached_uuid = self._reference_cache.get(
            normalized_name
        )

        if cached_uuid:

            return cached_uuid

        return self._stable_uuid(
            preference_kind,
            name
        )

    def _build_reference_cache(self):

        cache = {}

        inspector = inspect(
            self.dest_engine
        )

        table_names = [
            table_name
            for table_name in inspector.get_table_names()
            if any(
                token in table_name.lower()
                for token in [
                    "preference",
                    "career",
                    "major",
                    "industry",
                    "job",
                    "education"
                ]
            )
        ]

        for table_name in table_names:

            try:

                table = self._manual_reflect(
                    table_name,
                    self.dest_engine,
                    self.metadata_dest
                )

                if "uuid" not in table.c:

                    continue

                name_columns = [
                    column_name
                    for column_name in [
                        "name",
                        "title",
                        "label",
                        "code"
                    ]
                    if column_name in table.c
                ]

                if not name_columns:

                    continue

                with self.dest_engine.connect() as conn:

                    rows = conn.execute(
                        select(
                            table.c.uuid,
                            *[
                                table.c[column_name]
                                for column_name in name_columns
                            ]
                        )
                    ).fetchall()

                for row in rows:

                    row_map = row._mapping

                    for column_name in name_columns:

                        name = row_map.get(
                            table.c[column_name]
                        )

                        if not name:

                            continue

                        cache.setdefault(
                            self._normalize(name),
                            str(
                                row_map.get(
                                    table.c.uuid
                                )
                            )
                        )

            except Exception:

                logger.debug(
                    f"Skipping preference reference table "
                    f"{table_name}",
                    exc_info=True
                )

        return cache

    def _parse_list(
        self,
        value
    ):

        if value is None:

            return []

        if isinstance(value, list):

            return value

        if isinstance(value, tuple):

            return list(value)

        if isinstance(value, bytes):

            value = value.decode(
                "utf-8"
            )

        text = str(value).strip()

        if not text:

            return []

        try:

            parsed = json.loads(text)

            if isinstance(parsed, list):

                return parsed

            if parsed is None:

                return []

            return [parsed]

        except json.JSONDecodeError:

            return [
                item.strip()
                for item in text.split(",")
                if item.strip()
            ]

    def _dump_json(
        self,
        value
    ):

        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":")
        )

    def _json_column_value(
        self,
        table,
        column_name,
        value
    ):

        column = table.c.get(
            column_name
        )

        if column is not None and isinstance(
            column.type,
            JSON
        ):

            return value

        return self._dump_json(
            value
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

    def _get_row_value(
        self,
        row,
        *column_names
    ):

        if not row:

            return None

        for column_name in column_names:

            value = row.get(
                column_name
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

    def _stable_uuid(
        self,
        *parts
    ):

        digest = hashlib.md5(
            "::".join(
                str(part)
                for part in parts
            ).encode("utf-8")
        ).hexdigest()

        return str(
            uuid.UUID(digest)
        )

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
        )

    def _is_truthy(
        self,
        value
    ):

        return str(
            value
        ).strip().lower() in [
            "1",
            "true",
            "yes",
            "y"
        ]

    def _map_education_type(
        self,
        value
    ):

        mapping = {
            1: "university",
            2: "employer",
            3: "school",
            4: "service_provider",
            5: "regional_service_provider",
            "1": "university",
            "2": "employer",
            "3": "school",
            "4": "service_provider",
            "5": "regional_service_provider",
            "university": "university",
            "school": "school",
            "employer": "employer",
        }

        return mapping.get(
            value,
            "university"
        )
