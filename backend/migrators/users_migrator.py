import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UsersMigrator(BaseMigrator):

    DEFAULT_BATCH_SIZE = 10000

    MAX_BATCH_SIZE = 10000

    UUID_NAMESPACE = uuid.UUID(
        "55fa6c2d-84d5-5bd8-b0f3-6a1d8b6c91f4"
    )

    # -------------------------------------------------
    # SOURCE institution_user.role_id
    # -> destination role.code
    # -------------------------------------------------

    INSTITUTION_ROLE_MAPPING = {

        1: "receiver_admin",

        2: "institution_admin",

        3: "receiver",

        4: "recruiter",

        5: "developer",

        6: "counsellor",

        7: "recommender",

        8: "service_provider",

        9: "career_services",
    }

    USER_TYPE_ROLE_MAPPING = {

        "student": "student",

        "parent": "parent",

        "recommender": "recommender",

        "employer": "recruiter",

        "university": "receiver",

        "serviceprovider": "service_provider",

        "support": "support_admin",

        "superadmin": "super_admin",

        "tcbadmin": "institution_admin",
    }

    # -------------------------------------------------
    # SOURCE STATUS ENUM
    # -------------------------------------------------

    USER_STATUS_MAPPING = {

        "active": 1,
        "inactive": 2,
        "deleted": 3,
        "deactivated": 4,

        # numeric support
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
    }

    # -------------------------------------------------
    # Gender Mapping
    # -------------------------------------------------

    GENDER_MAPPING = {

        "male": 1,

        "female": 2,

        "other": 3,

        "prefer_not_to_say": 4,
    }

    ACTIVE_STATUS = 1

    # -------------------------------------------------
    # Constructor
    # -------------------------------------------------

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

    # -------------------------------------------------
    # Date Helpers
    # -------------------------------------------------

    def _clean_datetime(
        self,
        value,
        fallback=None
    ):

        if value is None:

            return fallback

        if isinstance(value, str):

            clean_value = value.strip()

            if (
                not clean_value
                or clean_value.startswith("0000-00-00")
            ):

                return fallback

            return value

        return value

    def _normalize(
        self,
        value
    ):

        return (
            str(value or "")
            .strip()
            .lower()
        )

    def _map_bool(
        self,
        value
    ):

        if value is None:

            return False

        if isinstance(value, (bytes, bytearray)):

            return value != b"\x00"

        if isinstance(value, bool):

            return value

        normalized_value = self._normalize(
            value
        )

        return normalized_value in {
            "1",
            "true",
            "yes",
            "y"
        }

    def _stable_uuid(
        self,
        key_type,
        key_value
    ):

        return str(
            uuid.uuid5(
                self.UUID_NAMESPACE,
                f"{key_type}:{key_value}"
            )
        )

    def _chunk_values(
        self,
        values,
        chunk_size=500
    ):

        for index in range(
            0,
            len(values),
            chunk_size
        ):

            yield values[
                index:index + chunk_size
            ]

    # -------------------------------------------------
    # Main Migration
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "USERS MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        # -------------------------------------------------
        # Detect Source Table
        # -------------------------------------------------

        mappings = self.config.get(
            "mappings",
            []
        )

        source_table_name = None

        for mapping in mappings:

            if mapping.get(
                "destination_table"
            ) == "users":

                source_table_name = mapping.get(
                    "source_table"
                )

                break

        if not source_table_name:

            raise ValueError(
                "Users source table "
                "not found in mappings."
            )

        logger.info(
            f"Using source table: "
            f"{source_table_name}"
        )

        # -------------------------------------------------
        # Reflect Tables
        # -------------------------------------------------

        source_table = self._manual_reflect(
            source_table_name,
            self.source_engine,
            self.metadata_source
        )

        institution_user_table = self._manual_reflect(
            "institution_user",
            self.source_engine,
            self.metadata_source
        )

        gl_parent_table = self._manual_reflect(
            "gl_parent",
            self.source_engine,
            self.metadata_source
        )

        users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        roles_table = self._manual_reflect(
            "role",
            self.dest_engine,
            self.metadata_dest
        )

        # -------------------------------------------------
        # Validate
        # -------------------------------------------------

        if not users_table.columns:

            raise ValueError(
                "Destination users table "
                "not found."
            )

        if not roles_table.columns:

            raise ValueError(
                "Destination role table "
                "not found."
            )

        # -------------------------------------------------
        # Build Role Lookup
        # role.code -> role.uuid
        # -------------------------------------------------

        role_lookup = {}

        with self.dest_engine.connect() as conn:

            role_results = conn.execute(
                select(
                    roles_table.c.uuid,
                    roles_table.c.code
                )
            )

            for role in role_results:

                role_lookup[
                    str(role.code)
                    .strip()
                    .lower()
                ] = role.uuid

        logger.info(
            f"Loaded {len(role_lookup)} roles."
        )

        if "user_id" not in source_table.c:

            raise ValueError(
                "Users migration expects source "
                "gl_user.user_id to map to jhi_user.id."
            )

        # -------------------------------------------------
        # Build audit actor lookup
        # gl_user.created_user / last_modified_user
        # -> destination users.uuid
        # -------------------------------------------------

        audit_actor_names = {}
        audit_actor_uuid_by_name = {}
        source_audit_user_uuid_by_name = {}
        audit_columns = [
            column_name
            for column_name in (
                "created_user",
                "last_modified_user"
            )
            if column_name in source_table.c
        ]

        if audit_columns:

            with self.source_engine.connect() as conn:

                for column_name in audit_columns:

                    audit_column = source_table.c[column_name]

                    audit_results = conn.execute(
                        select(
                            audit_column
                        )
                        .distinct()
                        .where(
                            audit_column.is_not(None)
                        )
                    )

                    for audit_row in audit_results:

                        audit_name = (
                            audit_row._mapping.get(
                                audit_column
                            )
                        )

                        audit_name = str(
                            audit_name or ""
                        ).strip()

                        if not audit_name:

                            continue

                        audit_actor_names[
                            self._normalize(
                                audit_name
                            )
                        ] = audit_name

                audit_name_values = list(
                    audit_actor_names.values()
                )

                for audit_name_chunk in self._chunk_values(
                    audit_name_values
                ):

                    username_results = conn.execute(
                        select(
                            source_table.c.username,
                            source_table.c.id,
                            source_table.c.user_id
                        )
                        .where(
                            source_table.c.username.in_(
                                audit_name_chunk
                            )
                        )
                    )

                    for username_row in username_results:

                        username_row_dict = (
                            username_row._mapping
                        )

                        source_username = (
                            username_row_dict.get(
                                source_table.c.username
                            )
                        )

                        source_jhi_user_id = (
                            username_row_dict.get(
                                source_table.c.user_id
                            )
                        )

                        source_gl_user_id = (
                            username_row_dict.get(
                                source_table.c.id
                            )
                        )

                        if (
                            not source_username
                            or source_gl_user_id is None
                        ):

                            continue

                        if source_jhi_user_id is not None:

                            audit_user_uuid = self._stable_uuid(
                                "gl_user.user_id",
                                source_jhi_user_id
                            )

                        else:

                            audit_user_uuid = self._stable_uuid(
                                "gl_user.id",
                                source_gl_user_id
                            )

                        source_audit_user_uuid_by_name[
                            self._normalize(
                                source_username
                            )
                        ] = audit_user_uuid

        for normalized_audit_name, audit_name in (
            audit_actor_names.items()
        ):

            source_audit_user_uuid = (
                source_audit_user_uuid_by_name.get(
                    normalized_audit_name
                )
            )

            if source_audit_user_uuid:

                audit_actor_uuid_by_name[
                    normalized_audit_name
                ] = source_audit_user_uuid

                continue

            logger.warning(
                f"Audit actor '{audit_name}' does not exist "
                f"in gl_user; rows created by this actor will "
                f"fallback to self-created audit values."
            )

        logger.info(
            f"Loaded {len(audit_actor_uuid_by_name)} "
            f"audit actor mappings; "
            f"{len(source_audit_user_uuid_by_name)} "
            f"matched gl_user rows."
        )

        # -------------------------------------------------
        # Source Query
        # -------------------------------------------------

        query = select(source_table)

        batch_size = int(
            self.config.get(
                "user_migration_batch_size",
                self.config.get(
                    "batch_size",
                    self.DEFAULT_BATCH_SIZE
                )
            )
        )

        if batch_size < 1:

            batch_size = self.DEFAULT_BATCH_SIZE

        if batch_size > self.MAX_BATCH_SIZE:

            logger.warning(
                f"Configured users batch size "
                f"{batch_size} is too high; "
                f"using {self.MAX_BATCH_SIZE}."
            )

            batch_size = self.MAX_BATCH_SIZE

        logger.info(
            f"Using users migration batch size: "
            f"{batch_size}"
        )

        # -------------------------------------------------
        # Prepare Insert Data
        # -------------------------------------------------

        users_insert_data = []

        migrated_count = 0
        batch_number = 0

        def flush_batches():

            nonlocal migrated_count
            nonlocal batch_number

            if not users_insert_data:

                return

            batch_number += 1

            logger.info(
                f"Users chunk {batch_number}: "
                f"inserting "
                f"{len(users_insert_data)} users..."
            )

            with self.dest_engine.begin() as dest_conn:

                try:

                    dest_conn.exec_driver_sql(
                        "SET FOREIGN_KEY_CHECKS=0"
                    )

                    dest_conn.execute(
                        insert(users_table),
                        users_insert_data
                    )

                finally:

                    dest_conn.exec_driver_sql(
                        "SET FOREIGN_KEY_CHECKS=1"
                    )

            migrated_count += len(users_insert_data)

            users_insert_data.clear()

        # -------------------------------------------------
        # SECOND PASS
        # -------------------------------------------------

        with self.source_engine.connect() as source_conn:

            last_source_id = None

            while True:

                batch_query = (
                    query
                    .order_by(
                        source_table.c.id
                    )
                    .limit(
                        batch_size
                    )
                )

                if last_source_id is not None:

                    batch_query = batch_query.where(
                        source_table.c.id > last_source_id
                    )

                results = source_conn.execute(
                    batch_query
                ).fetchall()

                if not results:

                    break

                chunk_start_id = results[0]._mapping.get(
                    source_table.c.id
                )

                chunk_end_id = results[-1]._mapping.get(
                    source_table.c.id
                )

                logger.info(
                    f"Processing users source chunk: "
                    f"id {chunk_start_id} to "
                    f"{chunk_end_id}, "
                    f"{len(results)} rows."
                )

                chunk_users_prepared = 0
                chunk_created_by_mapped = 0
                chunk_updated_by_mapped = 0
                chunk_errors = 0
                source_user_uuid_by_gl_id = {}
                source_user_type_by_gl_id = {}
                source_user_dates_by_gl_id = {}
                parent_lookup = {}

                for row in results:

                    row_dict = row._mapping

                    source_gl_user_id = row_dict.get(
                        source_table.c.id
                    )

                    source_jhi_user_id = row_dict.get(
                        source_table.c.user_id
                    )

                    if source_jhi_user_id is not None:

                        generated_uuid = self._stable_uuid(
                            "gl_user.user_id",
                            source_jhi_user_id
                        )

                    else:

                        generated_uuid = self._stable_uuid(
                            "gl_user.id",
                            source_gl_user_id
                        )

                    source_user_uuid_by_gl_id[
                        source_gl_user_id
                    ] = generated_uuid

                    source_user_type_by_gl_id[
                        source_gl_user_id
                    ] = self._normalize(
                        row_dict.get(
                            source_table.c.user_type
                        )
                    )

                    created_date = self._clean_datetime(
                        row_dict.get(
                            source_table.c.created_date
                        )
                    )

                    last_change_date = self._clean_datetime(
                        row_dict.get(
                            source_table.c.last_change_date
                        )
                    )

                    if not created_date:

                        created_date = (
                            last_change_date
                            or datetime.utcnow()
                        )

                    if not last_change_date:

                        last_change_date = created_date

                    source_user_dates_by_gl_id[
                        source_gl_user_id
                    ] = {

                        "created_at": created_date,

                        "updated_at": last_change_date,
                    }

                chunk_gl_user_ids = list(
                    source_user_uuid_by_gl_id.keys()
                )

                chunk_non_student_gl_ids = [
                    source_gl_user_id
                    for source_gl_user_id in chunk_gl_user_ids
                    if source_user_type_by_gl_id.get(
                        source_gl_user_id
                    ) != "student"
                ]

                institution_user_chunk_lookup = {}

                if chunk_non_student_gl_ids:

                    institution_results = source_conn.execute(
                        select(
                            institution_user_table.c.user_id,
                            institution_user_table.c.role_id,
                            institution_user_table.c.status,
                            institution_user_table.c.institution_id
                        ).where(
                            institution_user_table.c.user_id.in_(
                                chunk_non_student_gl_ids
                            )
                        )
                    )

                    for institution_row in institution_results:

                        institution_row_dict = (
                            institution_row._mapping
                        )

                        institution_user_chunk_lookup[
                            institution_row_dict.get(
                                institution_user_table.c.user_id
                            )
                        ] = institution_row_dict

                chunk_parent_gl_ids = [
                    source_gl_user_id
                    for source_gl_user_id in chunk_gl_user_ids
                    if source_user_type_by_gl_id.get(
                        source_gl_user_id
                    ) == "parent"
                ]

                if chunk_parent_gl_ids:

                    parent_results = source_conn.execute(
                        select(
                            gl_parent_table.c.user_id,
                            gl_parent_table.c.email_address
                        )
                        .where(
                            gl_parent_table.c.user_id.in_(
                                chunk_parent_gl_ids
                            )
                        )
                        .order_by(
                            gl_parent_table.c.id
                        )
                    )

                    for parent_row in parent_results:

                        parent_row_dict = parent_row._mapping
                        parent_user_id = parent_row_dict.get(
                            gl_parent_table.c.user_id
                        )

                        if parent_user_id in parent_lookup:

                            continue

                        parent_lookup[
                            parent_user_id
                        ] = parent_row_dict

                for row in results:

                    try:

                        row_dict = row._mapping

                        source_user_id = row_dict.get(
                            source_table.c.id
                        )

                        last_source_id = source_user_id

                        generated_uuid = (
                            source_user_uuid_by_gl_id.get(
                                source_user_id
                            )
                        )

                        if not generated_uuid:

                            chunk_errors += 1

                            logger.warning(
                                f"Skipping gl_user.id="
                                f"{source_user_id}: "
                                f"unable to build destination uuid"
                            )

                            continue

                        username = row_dict.get(
                            source_table.c.username
                        )

                        email = row_dict.get(
                            source_table.c.email
                        )

                        source_user_type = self._normalize(
                            row_dict.get(
                                source_table.c.user_type
                            )
                        )

                        parent_data = parent_lookup.get(
                            source_user_id,
                            {}
                        )

                        if (
                            source_user_type == "parent"
                            and parent_data.get(
                                gl_parent_table.c.email_address
                            )
                        ):

                            email = parent_data.get(
                                gl_parent_table.c.email_address
                            )

                        logger.debug(
                            f"Migrating user: "
                            f"{username}"
                        )

                        # -------------------------------------------------
                        # Dates
                        # -------------------------------------------------

                        source_dates = (
                            source_user_dates_by_gl_id.get(
                                source_user_id,
                                {}
                            )
                        )

                        created_date = source_dates.get(
                            "created_at"
                        )

                        last_change_date = source_dates.get(
                            "updated_at",
                            created_date
                        )

                        # -------------------------------------------------
                        # institution_user lookup
                        # -------------------------------------------------

                        institution_user_data = {}

                        institution_row = (
                            institution_user_chunk_lookup.get(
                                source_user_id
                            )
                        )

                        if institution_row:

                            source_role_id = institution_row.get(
                                institution_user_table.c.role_id
                            )

                            source_status = institution_row.get(
                                institution_user_table.c.status
                            )

                            role_code = (
                                self.INSTITUTION_ROLE_MAPPING.get(
                                    source_role_id
                                )
                            )

                            institution_user_data = {

                                "source_institution_id": (
                                    institution_row.get(
                                        institution_user_table.c.institution_id
                                    )
                                ),

                                "role_code": role_code,

                                "active_role_uuid": (
                                    role_lookup.get(role_code)
                                    if role_code
                                    else None
                                ),

                                "status": (
                                    self.USER_STATUS_MAPPING.get(
                                        str(source_status)
                                        .strip()
                                        .lower(),
                                        self.ACTIVE_STATUS
                                    )
                                )
                            }

                        active_role_uuid = (
                            institution_user_data.get(
                                "active_role_uuid"
                            )
                        )

                        if not active_role_uuid:

                            fallback_role_code = (
                                self.USER_TYPE_ROLE_MAPPING.get(
                                    source_user_type
                                )
                            )

                            if fallback_role_code:

                                active_role_uuid = (
                                    role_lookup.get(
                                        fallback_role_code
                                    )
                                )

                        user_status = (
                            institution_user_data.get(
                                "status",
                                self.ACTIVE_STATUS
                            )
                        )

                        created_user_name = (
                            row_dict.get(
                                source_table.c.created_user
                            )
                            if "created_user" in source_table.c
                            else None
                        )

                        last_modified_user_name = (
                            row_dict.get(
                                source_table.c.last_modified_user
                            )
                            if "last_modified_user" in source_table.c
                            else None
                        )

                        created_by_uuid = (
                            audit_actor_uuid_by_name.get(
                                self._normalize(
                                    created_user_name
                                )
                            )
                        )

                        if created_by_uuid:

                            chunk_created_by_mapped += 1

                        else:

                            created_by_uuid = generated_uuid

                        updated_by_uuid = (
                            audit_actor_uuid_by_name.get(
                                self._normalize(
                                    last_modified_user_name
                                )
                            )
                        )

                        if updated_by_uuid:

                            chunk_updated_by_mapped += 1

                        else:

                            updated_by_uuid = created_by_uuid

                        # -------------------------------------------------
                        # Users Row
                        # -------------------------------------------------

                        mapped_user_row = {

                            "uuid": generated_uuid,

                            "created_at": created_date,

                            "updated_at": last_change_date,

                            "deleted_at": None,

                            "user_name": username,

                            "email": email,

                            # status from institution_user
                            "status": user_status,

                            # active_role_uuid from institution_user.role_id
                            "active_role_uuid": active_role_uuid,

                            "created_by": created_by_uuid,

                            "updated_by": updated_by_uuid,

                            "is_parent": (
                                source_user_type
                                == "parent"
                            ),

                            "is_student": (
                                source_user_type
                                == "student"
                            ),

                            "is_demographic": 0,
                        }

                        users_insert_data.append(
                            {
                                column_name: value
                                for column_name, value
                                in mapped_user_row.items()
                                if column_name in users_table.c
                            }
                        )

                        chunk_users_prepared += 1

                    except Exception as row_error:

                        chunk_errors += 1

                        logger.exception(
                            f"Failed processing row: "
                            f"{str(row_error)}"
                        )

                logger.info(
                    f"Prepared users source chunk: "
                    f"id {chunk_start_id} to "
                    f"{chunk_end_id}; "
                    f"users={chunk_users_prepared}, "
                    f"created_by_mapped="
                    f"{chunk_created_by_mapped}, "
                    f"updated_by_mapped="
                    f"{chunk_updated_by_mapped}, "
                    f"errors={chunk_errors}."
                )

                flush_batches()

        flush_batches()

        # -------------------------------------------------
        # No Data
        # -------------------------------------------------

        if not migrated_count:

            logger.warning(
                "No users found to migrate."
            )

            return 0

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} users."
        )

        logger.info(
            "======================================="
        )

        return migrated_count
