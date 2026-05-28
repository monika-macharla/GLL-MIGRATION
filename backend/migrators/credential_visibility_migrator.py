import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    inspect,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CredentialVisibilityMigrator(BaseMigrator):

    SOURCE_TABLE_ALIASES = [
        "student_credential_visibility",
        "student_crdential_visibility",
    ]
    DESTINATION_TABLE = "module_permissions"
    PERMISSIONS_TABLE = "permissions"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 10000
    DYNAMIC_VALUE = "DYNAMIC"

    MODULE_MAPPINGS = [
        ("transcripts", "transcripts"),
        ("recom_letters", "recommendationLetters"),
        ("badges", "badges"),
        ("certifications", "certifications"),
        ("resume", "resume"),
        ("enable_tea", "enableTea"),
        ("enable_sar", "enableSar"),
        ("enable_nsapi", "nsapi"),
        ("enable_scholarship", "scholarships"),
        ("enable_swagger_api", "apis"),
        ("group_membership", "groupMembership"),
        ("enable_selfuploaded_credential", "selfUploadedDocuments"),
    ]

    INSTITUTION_TYPE_MAPPING = {
        "community college": "communitycollege",
        "high school": "highschool",
        "higher education": "university",
        "employer": "employer",
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

    def migrate(self) -> int:

        logger.info(
            "Starting Credential Visibility Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        source_table_name = self._resolve_source_table_name()

        source_table = self._manual_reflect(
            source_table_name,
            self.source_engine,
            self.metadata_source
        )

        source_institution_table = self._manual_reflect(
            "institution",
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        permissions_table = None
        destination_table_names = set(
            inspect(self.dest_engine).get_table_names()
        )

        if self.PERMISSIONS_TABLE in destination_table_names:

            permissions_table = self._manual_reflect(
                self.PERMISSIONS_TABLE,
                self.dest_engine,
                self.metadata_dest
            )

        auth_institutions_table = self._manual_reflect(
            "institutions",
            auth_db_engine,
            self.metadata_dest
        )

        institution_lookup = self._build_institution_lookup(
            auth_institutions_table,
            auth_db_engine
        )

        existing_uuids = self._load_existing_uuids(
            destination_table
        )
        existing_module_keys = self._load_existing_module_keys(
            destination_table
        )

        existing_permission_names = (
            self._load_existing_permission_names(
                permissions_table
            )
            if permissions_table is not None
            else
            set()
        )

        self._ensure_permission_rows(
            permissions_table,
            existing_permission_names
        )

        batch_size = self._get_batch_size()
        inserted_count = 0
        fetched_count = 0
        prepared_count = 0
        skipped_existing = 0
        missing_institution_count = 0
        skipped_duplicate_source_institution = 0
        processed_institute_ids = set()
        last_source_id = 0
        remaining_limit = self.config.get("limit")

        if remaining_limit:

            remaining_limit = int(
                remaining_limit
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

            with self.source_engine.connect() as source_conn:

                rows = source_conn.execute(
                    select(
                        source_table
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

            source_institutions = self._build_source_institution_context(
                rows,
                source_table,
                source_institution_table
            )

            insert_data = []

            for row in rows:

                row_dict = row._mapping
                source_id = self._get_source_value(
                    row_dict,
                    source_table,
                    "id"
                )
                last_source_id = source_id

                source_institution_id = self._get_source_value(
                    row_dict,
                    source_table,
                    "institution_id"
                )

                if not source_institution_id:

                    missing_institution_count += 1

                    continue

                source_institution = source_institutions.get(
                    source_institution_id
                )
                destination_institution_uuid = None

                if source_institution:

                    destination_institution_uuid = institution_lookup.get(
                        self._normalize(
                            source_institution.get("name")
                        )
                    )

                if source_institution_id and not destination_institution_uuid:

                    missing_institution_count += 1

                    continue

                if destination_institution_uuid in processed_institute_ids:

                    skipped_duplicate_source_institution += 1

                    continue

                processed_institute_ids.add(
                    destination_institution_uuid
                )

                institution_type = self._map_institution_type(
                    self._get_source_value(
                        row_dict,
                        source_table,
                        "institution_type"
                    )
                )

                now = datetime.utcnow()

                for source_column, destination_module in self.MODULE_MAPPINGS:

                    module_uuid = self._module_permission_uuid(
                        source_id,
                        destination_module
                    )

                    if module_uuid in existing_uuids:

                        skipped_existing += 1

                        continue

                    module_key = (
                        destination_institution_uuid,
                        destination_module
                    )

                    if module_key in existing_module_keys:

                        skipped_existing += 1

                        continue

                    module_row = {
                        "uuid": module_uuid,
                        "created_at": now,
                        "updated_at": now,
                        "deleted_at": None,
                        "institute_id": destination_institution_uuid,
                        "module": destination_module,
                        "is_accessible": self._map_bool(
                            self._get_source_value(
                                row_dict,
                                source_table,
                                source_column
                            )
                        ),
                        "institution_type": institution_type,
                    }

                    insert_data.append(
                        self._filter_to_table_columns(
                            module_row,
                            destination_table
                        )
                    )
                    existing_uuids.add(
                        module_uuid
                    )
                    existing_module_keys.add(
                        module_key
                    )

            if not insert_data:

                logger.info(
                    "Credential visibility chunk through source id "
                    f"{last_source_id}: nothing to insert."
                )

                continue

            logger.info(
                "Inserting module permissions chunk: "
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
                "Module permissions chunk inserted: "
                f"inserted={inserted_now}, "
                f"inserted_total={inserted_count}"
            )

        logger.info(
            "Credential Visibility Migration summary: "
            f"inserted={inserted_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped_existing={skipped_existing}, "
            f"missing_institution={missing_institution_count}, "
            f"skipped_duplicate_source_institution="
            f"{skipped_duplicate_source_institution}"
        )

        return inserted_count

    def _resolve_source_table_name(self):

        source_table_names = set(
            inspect(self.source_engine).get_table_names()
        )

        for table_name in self.SOURCE_TABLE_ALIASES:

            if table_name in source_table_names:

                return table_name

        raise ValueError(
            "student_credential_visibility source table not found"
        )

    def _build_source_institution_context(
        self,
        rows,
        source_table,
        source_institution_table
    ):

        institution_ids = set()

        for row in rows:

            institution_id = self._get_source_value(
                row._mapping,
                source_table,
                "institution_id"
            )

            if institution_id:

                institution_ids.add(
                    institution_id
                )

        return self._fetch_lookup_by_ids(
            self.source_engine,
            source_institution_table,
            source_institution_table.c.id,
            institution_ids
        )

    def _fetch_lookup_by_ids(
        self,
        engine,
        table,
        id_column,
        ids
    ):

        if not ids:

            return {}

        with engine.connect() as conn:

            rows = conn.execute(
                select(
                    table
                ).where(
                    id_column.in_(
                        list(ids)
                    )
                )
            ).fetchall()

        return {
            row._mapping.get(id_column): dict(row._mapping)
            for row in rows
        }

    def _build_institution_lookup(
        self,
        institutions_table,
        auth_db_engine
    ):

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    institutions_table.c.uuid,
                    institutions_table.c.name
                )
            ).fetchall()

        for row in rows:

            row_dict = row._mapping
            name = row_dict.get(
                institutions_table.c.name
            )
            institution_uuid = row_dict.get(
                institutions_table.c.uuid
            )

            if name and institution_uuid:

                lookup[
                    self._normalize(name)
                ] = institution_uuid

        return lookup

    def _load_existing_uuids(
        self,
        destination_table
    ):

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    destination_table.c.uuid
                )
            ).fetchall()

        return {
            row._mapping.get(destination_table.c.uuid)
            for row in rows
            if row._mapping.get(destination_table.c.uuid)
        }

    def _load_existing_module_keys(
        self,
        destination_table
    ):

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    destination_table.c.institute_id,
                    destination_table.c.module
                )
            ).fetchall()

        return {
            (
                row._mapping.get(destination_table.c.institute_id),
                row._mapping.get(destination_table.c.module)
            )
            for row in rows
            if (
                row._mapping.get(destination_table.c.institute_id)
                and
                row._mapping.get(destination_table.c.module)
            )
        }

    def _load_existing_permission_names(
        self,
        permissions_table
    ):

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    permissions_table.c.name
                )
            ).fetchall()

        return {
            row._mapping.get(permissions_table.c.name)
            for row in rows
            if row._mapping.get(permissions_table.c.name)
        }

    def _ensure_permission_rows(
        self,
        permissions_table,
        existing_permission_names
    ):

        if permissions_table is None:

            return

        now = datetime.utcnow()
        insert_data = []

        for _, module_name in self.MODULE_MAPPINGS:

            permission_name = f"{module_name}:access"

            if permission_name in existing_permission_names:

                continue

            permission_row = {
                "uuid": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"gll:permission:{permission_name}"
                    )
                ),
                "created_at": now,
                "updated_at": now,
                "deleted_at": None,
                "name": permission_name,
                "description": f"Access permission for {module_name}",
                "action_type": "access",
                "created_by": None,
            }

            insert_data.append(
                self._filter_to_table_columns(
                    permission_row,
                    permissions_table
                )
            )
            existing_permission_names.add(
                permission_name
            )

        if not insert_data:

            return

        with self.dest_engine.begin() as conn:

            result = conn.execute(
                insert(permissions_table),
                insert_data
            )

        logger.info(
            "Inserted permissions rows for modules: "
            f"{result.rowcount or len(insert_data)}"
        )

    def _module_permission_uuid(
        self,
        source_id,
        module_name
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:module-permission:{source_id}:{module_name}"
            )
        )

    def _map_bool(
        self,
        value
    ):

        if value is None:

            return 0

        if isinstance(value, bool):

            return 1 if value else 0

        if isinstance(value, bytes):

            return 1 if value == b"\x01" else 0

        return 1 if str(value).strip().lower() in [
            "1",
            "true",
            "yes",
            "y",
            "\\x01",
        ] else 0

    def _map_institution_type(
        self,
        value
    ):

        normalized = (
            str(value or "")
            .strip()
            .lower()
        )

        return self.INSTITUTION_TYPE_MAPPING.get(
            normalized,
            normalized
        )

    def _get_batch_size(self):

        batch_size = int(
            self.config.get(
                "credential_visibility_migration_batch_size",
                self.config.get(
                    "batch_size",
                    self.DEFAULT_BATCH_SIZE
                )
            )
        )

        if batch_size < 1:

            batch_size = self.DEFAULT_BATCH_SIZE

        return min(
            batch_size,
            self.MAX_BATCH_SIZE
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
