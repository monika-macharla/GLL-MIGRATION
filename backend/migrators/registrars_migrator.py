import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    inspect,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class RegistrarsMigrator(BaseMigrator):

    DESTINATION_TABLE = "registrars"
    SOURCE_TABLE_CANDIDATES = [
        "institution_registrat",
        "institution_registrar",
        "institution_registrars"
    ]

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
            "Starting Registrars Migration..."
        )

        source_table_names = set(
            inspect(self.source_engine).get_table_names()
        )

        source_table_name = (
            self._get_configured_source_table(
                source_table_names
            )
            or
            self._first_existing_table(
                source_table_names,
                self.SOURCE_TABLE_CANDIDATES
            )
        )

        if not source_table_name:

            raise ValueError(
                "Registrar source table not found. "
                f"Tried: {self.SOURCE_TABLE_CANDIDATES}"
            )

        source_table = self._manual_reflect(
            source_table_name,
            self.source_engine,
            self.metadata_source
        )

        registrar_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        institutions_table = self._manual_reflect(
            "institutions",
            self.dest_engine,
            self.metadata_dest
        )

        logger.info(
            f"Registrar source columns: "
            f"{source_table.columns.keys()}"
        )

        logger.info(
            f"Registrar destination columns: "
            f"{registrar_table.columns.keys()}"
        )

        query = select(
            source_table
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
            f"Found {len(rows)} registrar records"
        )

        insert_data = []

        skipped_count = 0

        for index, row in enumerate(
            rows,
            start=1
        ):

            try:

                row_dict = row._mapping

                user_uuid, source_user = (
                    self._resolve_user_uuid(
                        row_dict,
                        source_table,
                        users_table
                    )
                )

                if not user_uuid:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping registrar row {index}: "
                        "could not resolve user UUID. "
                        f"source identifiers="
                        f"{self._get_user_debug_values(row_dict, source_table)}"
                    )

                    continue

                institution_uuid = (
                    self._resolve_institution_uuid(
                        row_dict,
                        source_table,
                        institutions_table
                    )
                )

                if not institution_uuid:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping registrar row {index}: "
                        "could not resolve institution UUID"
                    )

                    continue

                created_at = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "created_at",
                        "created_date",
                        "created_on"
                    )
                    or
                    datetime.utcnow()
                )

                updated_at = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "updated_at",
                        "last_modified_date",
                        "modified_at"
                    )
                    or
                    created_at
                )

                email = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "email",
                        "user_email",
                        "registrar_email"
                    )
                    or
                    self._get_row_value(
                        source_user,
                        "email",
                        "username",
                        "login"
                    )
                )

                first_name = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "first_name",
                        "firstname",
                        "registrar_first_name"
                    )
                    or
                    self._get_row_value(
                        source_user,
                        "first_name",
                        "firstname"
                    )
                )

                last_name = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "last_name",
                        "lastname",
                        "registrar_last_name"
                    )
                    or
                    self._get_row_value(
                        source_user,
                        "last_name",
                        "lastname"
                    )
                )

                full_name = (
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "name",
                        "registrar_name"
                    )
                    or
                    " ".join(
                        part
                        for part in [
                            str(first_name or "").strip(),
                            str(last_name or "").strip()
                        ]
                        if part
                    )
                    or
                    email
                    or
                    ""
                )

                status = self._map_status(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "status",
                        "active"
                    )
                )

                mapped_row = {
                    "uuid": str(uuid.uuid4()),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "deleted_at": None,
                    "user_id": user_uuid,
                    "user_uuid": user_uuid,
                    "registrar_id": user_uuid,
                    "institution_id": institution_uuid,
                    "institution_uuid": institution_uuid,
                    "email": email,
                    "registrar_email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "name": full_name,
                    "registrar_name": full_name,
                    "status": status,
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                    "deleted_by": None,
                }

                insert_data.append(
                    self._filter_to_table_columns(
                        mapped_row,
                        registrar_table
                    )
                )

            except Exception as error:

                skipped_count += 1

                logger.exception(
                    f"Failed processing registrar row "
                    f"{index}: {error}"
                )

        if not insert_data:

            logger.warning(
                "No valid registrar records available for insertion"
            )

            return 0

        with self.dest_engine.begin() as dest_conn:

            result = dest_conn.execute(
                insert(registrar_table),
                insert_data
            )

        logger.info(
            "Registrars Migration summary: "
            f"inserted={len(insert_data)}, "
            f"skipped={skipped_count}, "
            f"rowcount={result.rowcount}"
        )

        return len(
            insert_data
        )

    def _get_configured_source_table(
        self,
        source_table_names
    ):

        for mapping in self.config.get(
            "mappings",
            []
        ):

            destination_table = str(
                mapping.get("destination_table") or ""
            ).strip().lower()

            source_table = str(
                mapping.get("source_table") or ""
            ).strip()

            if (
                destination_table == self.DESTINATION_TABLE
                and
                source_table in source_table_names
            ):

                return source_table

        return None

    def _first_existing_table(
        self,
        table_names,
        candidates
    ):

        for candidate in candidates:

            if candidate in table_names:

                return candidate

        return None

    def _resolve_user_uuid(
        self,
        row,
        source_table,
        users_table
    ):

        source_user_id = self._get_source_value(
            row,
            source_table,
            "user_id",
            "gl_user_id",
            "jhi_user_id",
            "registrar_user_id",
            "registrar_gl_user_id",
            "registrar_jhi_user_id",
            "registrar_id",
            "institution_user_id",
            "institution_user",
            "admin_user_id",
            "id"
        )

        source_user = None

        if source_user_id:

            source_institution_user = self.fetch_one_by_column(
                self.source_engine,
                "institution_user",
                "id",
                source_user_id
            )

            if source_institution_user:

                source_user_id = source_institution_user.get(
                    "user_id"
                )

            source_user = (
                self.fetch_one_by_column(
                    self.source_engine,
                    "gl_user",
                    "id",
                    source_user_id
                )
                or
                self.fetch_one_by_column(
                    self.source_engine,
                    "jhi_user",
                    "id",
                    source_user_id
                )
            )

        source_username = (
            self._get_source_value(
                row,
                source_table,
                "username",
                "login",
                "user_name",
                "email",
                "user_email",
                "registrar_email",
                "registrar_username",
                "registrar_login"
            )
            or
            self._get_row_value(
                source_user,
                "username",
                "login",
                "email"
            )
        )

        if not source_username:

            return None, source_user

        dest_user = self._find_destination_user(
            users_table,
            source_username
        )

        if dest_user:

            return (
                dest_user.get("uuid"),
                source_user
            )

        return None, source_user

    def _find_destination_user(
        self,
        users_table,
        source_username
    ):

        candidate = str(
            source_username or ""
        ).strip()

        if not candidate:

            return None

        with self.dest_engine.connect() as conn:

            for column_name in [
                "user_name",
                "email"
            ]:

                if column_name not in users_table.c:

                    continue

                dest_user = conn.execute(
                    select(
                        users_table
                    ).where(
                        users_table.c[column_name] == candidate
                    )
                ).fetchone()

                if dest_user:

                    return dest_user._mapping

            rows = conn.execute(
                select(
                    users_table
                )
            ).fetchall()

        normalized_candidate = self._normalize(
            candidate
        )

        for row in rows:

            row_map = row._mapping

            for column_name in [
                "user_name",
                "email"
            ]:

                if column_name not in users_table.c:

                    continue

                if (
                    self._normalize(
                        row_map.get(
                            users_table.c[column_name]
                        )
                    )
                    == normalized_candidate
                ):

                    return row_map

        return None

    def _resolve_institution_uuid(
        self,
        row,
        source_table,
        institutions_table
    ):

        source_institution_id = self._get_source_value(
            row,
            source_table,
            "institution_id",
            "institution",
            "school_id"
        )

        if not source_institution_id:

            source_institution_user_id = self._get_source_value(
                row,
                source_table,
                "institution_user_id",
                "institution_user",
                "id"
            )

            source_institution_user = self.fetch_one_by_column(
                self.source_engine,
                "institution_user",
                "id",
                source_institution_user_id
            )

            if source_institution_user:

                source_institution_id = (
                    source_institution_user.get(
                        "institution_id"
                    )
                )

        institution_name = self._get_source_value(
            row,
            source_table,
            "institution_name",
            "school_name"
        )

        if source_institution_id and not institution_name:

            source_institution = self.fetch_one_by_column(
                self.source_engine,
                "institution",
                "id",
                source_institution_id
            )

            if source_institution:

                institution_name = source_institution.get(
                    "name"
                )

        if not institution_name:

            return None

        with self.dest_engine.connect() as conn:

            query = select(
                institutions_table
            ).where(
                institutions_table.c.name == institution_name
            )

            dest_institution = conn.execute(
                query
            ).fetchone()

        if dest_institution:

            return dest_institution._mapping.get(
                institutions_table.c.uuid
            )

        normalized_institution_name = self._normalize(
            institution_name
        )

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    institutions_table
                )
            ).fetchall()

        for dest_institution in rows:

            row_map = dest_institution._mapping

            if (
                self._normalize(
                    row_map.get(
                        institutions_table.c.name
                    )
                )
                == normalized_institution_name
            ):

                return row_map.get(
                    institutions_table.c.uuid
                )

        return None

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

    def _get_user_debug_values(
        self,
        row,
        table
    ):

        debug_values = {}

        for column_name in [
            "id",
            "user_id",
            "gl_user_id",
            "jhi_user_id",
            "registrar_user_id",
            "registrar_id",
            "institution_user_id",
            "username",
            "login",
            "user_name",
            "email",
            "user_email",
            "registrar_email"
        ]:

            if column_name in table.c:

                debug_values[column_name] = row.get(
                    table.c[column_name]
                )

        return debug_values

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

    def _map_status(
        self,
        status
    ):

        if isinstance(status, bool):

            return 1 if status else 2

        normalized_status = (
            str(status or "")
            .strip()
            .lower()
        )

        if normalized_status in [
            "active",
            "enabled",
            "1",
            "true"
        ]:

            return 1

        if normalized_status in [
            "inactive",
            "disabled",
            "deleted",
            "0",
            "false"
        ]:

            return 2

        return 1
