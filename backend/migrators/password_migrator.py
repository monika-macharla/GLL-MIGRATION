import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class PasswordMigrator(BaseMigrator):

    DEFAULT_BATCH_SIZE = 10000

    MAX_BATCH_SIZE = 10000

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

    def _normalize(
        self,
        value
    ):

        return (
            str(value or "")
            .strip()
            .lower()
        )

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

    # -------------------------------------------------
    # Main Migration
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "PASSWORD MIGRATION STARTED"
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

            destination_table = str(
                mapping.get(
                    "destination_table",
                    ""
                )
            ).strip().lower()

            if destination_table in [

                "password",

                "user_hashed_password"
            ]:

                source_table_name = mapping.get(
                    "source_table"
                )

                break

        if not source_table_name:

            raise ValueError(
                "Password source table "
                "not found in mappings."
            )

        if self._normalize(source_table_name) != "jhi_user":

            logger.warning(
                f"Password hashes are sourced only from "
                f"jhi_user.password_hash; ignoring configured "
                f"password source table '{source_table_name}'."
            )

            source_table_name = "jhi_user"

        logger.info(
            f"Using password source table: "
            f"{source_table_name}"
        )

        # -------------------------------------------------
        # Reflect Tables
        # -------------------------------------------------

        source_user_table = self._manual_reflect(

            source_table_name,

            self.source_engine,

            self.metadata_source
        )

        users_table = self._manual_reflect(

            "users",

            self.dest_engine,

            self.metadata_dest
        )

        password_table = self._manual_reflect(

            "user_hashed_password",

            self.dest_engine,

            self.metadata_dest
        )

        gl_user_table = None

        if self._normalize(source_table_name) == "jhi_user":

            gl_user_table = self._manual_reflect(

                "gl_user",

                self.source_engine,

                self.metadata_source
            )

        # -------------------------------------------------
        # Validate
        # -------------------------------------------------

        if not source_user_table.columns:

            raise ValueError(
                "Source password table "
                "not found."
            )

        if not users_table.columns:

            raise ValueError(
                "Destination users table "
                "not found."
            )

        if not password_table.columns:

            raise ValueError(
                "Destination "
                "user_hashed_password table "
                "not found."
            )

        batch_size = int(
            self.config.get(
                "password_migration_batch_size",
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
                f"Configured password batch size "
                f"{batch_size} is too high; "
                f"using {self.MAX_BATCH_SIZE}."
            )

            batch_size = self.MAX_BATCH_SIZE

        logger.info(
            f"Using password migration batch size: "
            f"{batch_size}"
        )

        migrated_count = 0
        batch_number = 0
        migrated_user_uuids = set()

        with self.source_engine.connect() as source_conn:

            last_source_id = None

            while True:

                query = (
                    select(source_user_table)
                    .order_by(
                        source_user_table.c.id
                    )
                    .limit(
                        batch_size
                    )
                )

                if last_source_id is not None:

                    query = query.where(
                        source_user_table.c.id > last_source_id
                    )

                results = source_conn.execute(
                    query
                ).fetchall()

                if not results:

                    break

                chunk_start_id = results[0]._mapping.get(
                    source_user_table.c.id
                )

                chunk_end_id = results[-1]._mapping.get(
                    source_user_table.c.id
                )

                logger.info(
                    f"Processing password source chunk: "
                    f"id {chunk_start_id} to "
                    f"{chunk_end_id}, "
                    f"{len(results)} rows."
                )

                lookup_user_names = []
                source_username_lookup = {}
                source_jhi_user_ids = []

                for row in results:

                    row_dict = row._mapping

                    last_source_id = row_dict.get(
                        source_user_table.c.id
                    )

                    source_jhi_user_ids.append(
                        last_source_id
                    )

                    for column_name in (
                        "login",
                        "created_by",
                        "last_modified_by"
                    ):

                        if column_name not in source_user_table.c:

                            continue

                        user_name = row_dict.get(
                            source_user_table.c[column_name]
                        )

                        if user_name:

                            lookup_user_names.append(
                                user_name
                            )

                if (
                    gl_user_table is not None
                    and source_jhi_user_ids
                ):

                    gl_user_results = source_conn.execute(
                        select(
                            gl_user_table.c.user_id,
                            gl_user_table.c.username
                        )
                        .where(
                            gl_user_table.c.user_id.in_(
                                list(set(source_jhi_user_ids))
                            )
                        )
                        .order_by(
                            gl_user_table.c.id
                        )
                    )

                    for gl_user_row in gl_user_results:

                        gl_user_row_dict = gl_user_row._mapping

                        source_jhi_user_id = gl_user_row_dict.get(
                            gl_user_table.c.user_id
                        )

                        if source_jhi_user_id in source_username_lookup:

                            continue

                        source_username = gl_user_row_dict.get(
                            gl_user_table.c.username
                        )

                        if not source_username:

                            continue

                        source_username_lookup[
                            source_jhi_user_id
                        ] = source_username

                        lookup_user_names.append(
                            source_username
                        )

                user_lookup = {}

                lookup_user_names = list(
                    set(lookup_user_names)
                )

                if lookup_user_names:

                    with self.dest_engine.connect() as dest_conn:

                        user_results = dest_conn.execute(
                            select(
                                users_table.c.uuid,
                                users_table.c.user_name
                            ).where(
                                users_table.c.user_name.in_(
                                    lookup_user_names
                                )
                            )
                        )

                        for user_row in user_results:

                            user_row_dict = user_row._mapping

                            user_lookup[
                                self._normalize(
                                    user_row_dict.get(
                                        users_table.c.user_name
                                    )
                                )
                            ] = user_row_dict.get(
                                users_table.c.uuid
                            )

                existing_password_user_uuids = set()

                lookup_user_uuids = [
                    value
                    for value in user_lookup.values()
                    if value is not None
                ]

                if lookup_user_uuids:

                    with self.dest_engine.connect() as dest_conn:

                        existing_password_results = (
                            dest_conn.execute(
                                select(
                                    password_table.c.user_uuid
                                ).where(
                                    password_table.c.user_uuid.in_(
                                        list(set(lookup_user_uuids))
                                    )
                                )
                            )
                        )

                        for password_row in existing_password_results:

                            existing_password_user_uuids.add(
                                password_row._mapping.get(
                                    password_table.c.user_uuid
                                )
                            )

                password_insert_data = []
                prepared_user_uuids = set()
                skipped_missing_hash = 0
                skipped_missing_source_user = 0
                skipped_missing_user = 0
                skipped_existing_passwords = 0
                skipped_duplicate_passwords = 0
                created_by_mapped = 0
                updated_by_mapped = 0
                row_errors = 0

                for row in results:

                    try:

                        row_dict = row._mapping

                        # -------------------------------------------------
                        # Source Fields
                        # -------------------------------------------------

                        login = row_dict.get(
                            source_user_table.c.login
                        )

                        source_jhi_user_id = row_dict.get(
                            source_user_table.c.id
                        )

                        password_hash = row_dict.get(
                            source_user_table.c.password_hash
                        )

                        if not self._normalize(password_hash):

                            skipped_missing_hash += 1

                            continue

                        created_date = self._clean_datetime(
                            row_dict.get(
                                source_user_table.c.created_date
                            )
                        )

                        updated_date = self._clean_datetime(
                            row_dict.get(
                                source_user_table.c.last_modified_date
                            )
                        )

                        if not created_date:

                            created_date = (
                                updated_date
                                or datetime.utcnow()
                            )

                        if not updated_date:

                            updated_date = created_date

                        created_by = row_dict.get(
                            source_user_table.c.created_by
                        )

                        updated_by = row_dict.get(
                            source_user_table.c.last_modified_by
                        )

                        # -------------------------------------------------
                        # Find destination user UUID
                        # -------------------------------------------------

                        destination_user_name = login

                        if gl_user_table is not None:

                            destination_user_name = (
                                source_username_lookup.get(
                                    source_jhi_user_id
                                )
                            )

                            if not destination_user_name:

                                skipped_missing_source_user += 1

                                continue

                        user_uuid = user_lookup.get(
                            self._normalize(destination_user_name)
                        )

                        if not user_uuid:

                            skipped_missing_user += 1

                            continue

                        if user_uuid in existing_password_user_uuids:

                            skipped_existing_passwords += 1

                            continue

                        if (
                            user_uuid in migrated_user_uuids
                            or user_uuid in prepared_user_uuids
                        ):

                            skipped_duplicate_passwords += 1

                            continue

                        # -------------------------------------------------
                        # created_by UUID
                        # -------------------------------------------------

                        created_by_uuid = None

                        if created_by:

                            created_by_uuid = (
                                user_lookup.get(
                                    self._normalize(created_by)
                                )
                            )

                        if created_by_uuid:

                            created_by_mapped += 1

                        else:

                            created_by_uuid = user_uuid

                        # -------------------------------------------------
                        # updated_by UUID
                        # -------------------------------------------------

                        updated_by_uuid = None

                        if updated_by:

                            updated_by_uuid = (
                                user_lookup.get(
                                    self._normalize(updated_by)
                                )
                            )

                        if updated_by_uuid:

                            updated_by_mapped += 1

                        else:

                            updated_by_uuid = created_by_uuid

                        # -------------------------------------------------
                        # Build Password Row
                        # -------------------------------------------------

                        mapped_password_row = {

                            "uuid": str(
                                uuid.uuid4()
                            ),

                            "created_at": created_date,

                            "updated_at": (
                                updated_date
                                or created_date
                            ),

                            "deleted_at": None,

                            "hashed_password": password_hash,

                            "salt_key": None,

                            "password_hint": None,

                            "expires_at": None,

                            "user_uuid": user_uuid,

                            "created_by": created_by_uuid,

                            "updated_by": updated_by_uuid,
                        }

                        password_insert_data.append(
                            mapped_password_row
                        )

                        prepared_user_uuids.add(
                            user_uuid
                        )

                    except Exception as row_error:

                        row_errors += 1

                        logger.exception(
                            f"Failed processing row: "
                            f"{str(row_error)}"
                        )

                if password_insert_data:

                    batch_number += 1

                    logger.info(
                        f"Password chunk {batch_number}: "
                        f"inserting {len(password_insert_data)} "
                        f"passwords; "
                        f"skipped_missing_user="
                        f"{skipped_missing_user}, "
                        f"skipped_missing_hash="
                        f"{skipped_missing_hash}, "
                        f"skipped_missing_source_user="
                        f"{skipped_missing_source_user}, "
                        f"skipped_existing_passwords="
                        f"{skipped_existing_passwords}, "
                        f"skipped_duplicate_passwords="
                        f"{skipped_duplicate_passwords}, "
                        f"created_by_mapped={created_by_mapped}, "
                        f"updated_by_mapped={updated_by_mapped}, "
                        f"errors={row_errors}."
                    )

                    with self.dest_engine.begin() as dest_conn:

                        dest_conn.execute(
                            insert(password_table),
                            password_insert_data
                        )

                    migrated_count += len(password_insert_data)

                    migrated_user_uuids.update(
                        prepared_user_uuids
                    )

                else:

                    logger.warning(
                        f"Password chunk id "
                        f"{chunk_start_id} to {chunk_end_id}: "
                        f"no passwords prepared; "
                        f"skipped_missing_user="
                        f"{skipped_missing_user}, "
                        f"skipped_missing_hash="
                        f"{skipped_missing_hash}, "
                        f"skipped_missing_source_user="
                        f"{skipped_missing_source_user}, "
                        f"skipped_existing_passwords="
                        f"{skipped_existing_passwords}, "
                        f"skipped_duplicate_passwords="
                        f"{skipped_duplicate_passwords}, "
                        f"errors={row_errors}."
                    )

                if len(results) < batch_size:

                    break

        if not migrated_count:

            logger.warning(
                "No passwords found to migrate."
            )

            return 0

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} "
            f"passwords."
        )

        logger.info(
            "======================================="
        )

        return migrated_count
