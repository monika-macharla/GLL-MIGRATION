import uuid
import logging

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class PasswordMigrator(BaseMigrator):

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

        # -------------------------------------------------
        # Build Destination User Lookup
        # username -> uuid
        # -------------------------------------------------

        user_lookup = {}

        with self.dest_engine.connect() as conn:

            results = conn.execute(

                select(
                    users_table.c.uuid,
                    users_table.c.user_name
                )
            )

            for row in results:

                if row.user_name:

                    user_lookup[
                        str(row.user_name)
                        .strip()
                        .lower()
                    ] = row.uuid

        logger.info(
            f"Loaded "
            f"{len(user_lookup)} users."
        )

        # -------------------------------------------------
        # Read Source Data
        # -------------------------------------------------

        password_insert_data = []

        query = select(source_user_table)

        with self.source_engine.connect() as source_conn:

            results = source_conn.execute(query)

            for row in results:

                try:

                    row_dict = row._mapping

                    # -------------------------------------------------
                    # Source Fields
                    # -------------------------------------------------

                    login = row_dict.get(
                        source_user_table.c.login
                    )

                    password_hash = row_dict.get(
                        source_user_table.c.password_hash
                    )

                    created_date = row_dict.get(
                        source_user_table.c.created_date
                    )

                    updated_date = row_dict.get(
                        source_user_table.c.last_modified_date
                    )

                    created_by = row_dict.get(
                        source_user_table.c.created_by
                    )

                    updated_by = row_dict.get(
                        source_user_table.c.last_modified_by
                    )

                    logger.info(
                        f"Migrating password "
                        f"for user: {login}"
                    )

                    # -------------------------------------------------
                    # Find destination user UUID
                    # -------------------------------------------------

                    user_uuid = user_lookup.get(

                        str(login)
                        .strip()
                        .lower()
                    )

                    if not user_uuid:

                        logger.warning(
                            f"User not found "
                            f"for login: {login}"
                        )

                        continue

                    # -------------------------------------------------
                    # created_by UUID
                    # -------------------------------------------------

                    created_by_uuid = None

                    if created_by:

                        created_by_uuid = (
                            user_lookup.get(
                                str(created_by)
                                .strip()
                                .lower()
                            )
                        )

                    # -------------------------------------------------
                    # updated_by UUID
                    # -------------------------------------------------

                    updated_by_uuid = None

                    if updated_by:

                        updated_by_uuid = (
                            user_lookup.get(
                                str(updated_by)
                                .strip()
                                .lower()
                            )
                        )

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

                except Exception as row_error:

                    logger.exception(
                        f"Failed processing row: "
                        f"{str(row_error)}"
                    )

        # -------------------------------------------------
        # No Data
        # -------------------------------------------------

        if not password_insert_data:

            logger.warning(
                "No passwords found to migrate."
            )

            return 0

        # -------------------------------------------------
        # Insert Passwords
        # -------------------------------------------------

        logger.info(
            f"Inserting "
            f"{len(password_insert_data)} "
            f"passwords..."
        )

        with self.dest_engine.begin() as dest_conn:

            dest_conn.execute(

                insert(password_table),

                password_insert_data
            )

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{len(password_insert_data)} "
            f"passwords."
        )

        logger.info(
            "======================================="
        )

        return len(password_insert_data)