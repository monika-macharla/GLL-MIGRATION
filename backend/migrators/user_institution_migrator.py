import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UserInstitutionMigrator(BaseMigrator):

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
    # Normalize Helper
    # -------------------------------------------------

    def normalize(self, value):

        if not value:

            return ""

        return (
            str(value)
            .strip()
            .lower()
            .replace(" ", "")
        )

    # -------------------------------------------------
    # Main Migration
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "USER_INSTITUTION MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        # -------------------------------------------------
        # Reflect Source Tables
        # -------------------------------------------------

        institution_user_table = self._manual_reflect(
            "institution_user",
            self.source_engine,
            self.metadata_source
        )

        source_users_table = self._manual_reflect(
            "jhi_user",
            self.source_engine,
            self.metadata_source
        )

        source_institution_table = self._manual_reflect(
            "institution",
            self.source_engine,
            self.metadata_source
        )

        # -------------------------------------------------
        # Reflect Destination Tables
        # -------------------------------------------------

        destination_users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        destination_institutions_table = self._manual_reflect(
            "institutions",
            self.dest_engine,
            self.metadata_dest
        )

        user_institution_table = self._manual_reflect(
            "user_institution",
            self.dest_engine,
            self.metadata_dest
        )

        # -------------------------------------------------
        # SOURCE USER ID -> DEST USER UUID
        # -------------------------------------------------

        source_to_dest_user_uuid = {}

        with self.source_engine.connect() as source_conn, \
             self.dest_engine.connect() as dest_conn:

            # ---------------------------------------------
            # Source login lookup
            # ---------------------------------------------

            source_results = source_conn.execute(
                select(
                    source_users_table.c.id,
                    source_users_table.c.login
                )
            )

            source_login_lookup = {}

            for row in source_results:

                row_dict = row._mapping

                source_login_lookup[
                    self.normalize(
                        row_dict.get(
                            source_users_table.c.login
                        )
                    )
                ] = row_dict.get(
                    source_users_table.c.id
                )

            # ---------------------------------------------
            # Destination username lookup
            # ---------------------------------------------

            dest_results = dest_conn.execute(
                select(
                    destination_users_table.c.uuid,
                    destination_users_table.c.user_name
                )
            )

            destination_lookup = {}

            for row in dest_results:

                row_dict = row._mapping

                destination_lookup[
                    self.normalize(
                        row_dict.get(
                            destination_users_table.c.user_name
                        )
                    )
                ] = row_dict.get(
                    destination_users_table.c.uuid
                )

            # ---------------------------------------------
            # Final mapping
            # ---------------------------------------------

            for login, source_user_id in (
                source_login_lookup.items()
            ):

                dest_uuid = destination_lookup.get(
                    login
                )

                if dest_uuid:

                    source_to_dest_user_uuid[
                        source_user_id
                    ] = dest_uuid

        logger.info(
            f"Built "
            f"{len(source_to_dest_user_uuid)} "
            f"user UUID mappings."
        )

        # -------------------------------------------------
        # SOURCE INSTITUTION LOOKUP
        # institution.id -> institution.name
        # -------------------------------------------------

        source_institution_lookup = {}

        with self.source_engine.connect() as conn:

            results = conn.execute(
                select(
                    source_institution_table.c.id,
                    source_institution_table.c.name
                )
            )

            for row in results:

                row_dict = row._mapping

                source_institution_id = row_dict.get(
                    source_institution_table.c.id
                )

                institution_name = self.normalize(
                    row_dict.get(
                        source_institution_table.c.name
                    )
                )

                source_institution_lookup[
                    source_institution_id
                ] = institution_name

        logger.info(
            f"Loaded "
            f"{len(source_institution_lookup)} "
            f"source institutions."
        )

        # -------------------------------------------------
        # DESTINATION INSTITUTION LOOKUP
        # institutions.name -> institutions.uuid
        # -------------------------------------------------

        destination_institution_lookup = {}

        with self.dest_engine.connect() as conn:

            results = conn.execute(
                select(
                    destination_institutions_table.c.uuid,
                    destination_institutions_table.c.name
                )
            )

            for row in results:

                row_dict = row._mapping

                institution_name = self.normalize(
                    row_dict.get(
                        destination_institutions_table.c.name
                    )
                )

                destination_institution_lookup[
                    institution_name
                ] = row_dict.get(
                    destination_institutions_table.c.uuid
                )

        logger.info(
            f"Loaded "
            f"{len(destination_institution_lookup)} "
            f"destination institutions."
        )

        # -------------------------------------------------
        # Counters
        # -------------------------------------------------

        skipped_users = 0

        skipped_institutions = 0

        inserted_count = 0

        # -------------------------------------------------
        # PREPARE INSERT DATA
        # -------------------------------------------------

        insert_data = []

        with self.source_engine.connect() as conn:

            results = conn.execute(
                select(institution_user_table)
            )

            for row in results:

                try:

                    row_dict = row._mapping

                    source_user_id = row_dict.get(
                        institution_user_table.c.user_id
                    )

                    source_institution_id = row_dict.get(
                        institution_user_table.c.institution_id
                    )

                    # ---------------------------------------------
                    # USER UUID
                    # ---------------------------------------------

                    user_uuid = (
                        source_to_dest_user_uuid.get(
                            source_user_id
                        )
                    )

                    if not user_uuid:

                        skipped_users += 1

                        logger.warning(
                            f"SKIPPED USER: "
                            f"source_user_id="
                            f"{source_user_id}"
                        )

                        continue

                    # ---------------------------------------------
                    # INSTITUTION UUID
                    # ---------------------------------------------

                    institution_name = (
                        source_institution_lookup.get(
                            source_institution_id
                        )
                    )

                    institution_uuid = (
                        destination_institution_lookup.get(
                            institution_name
                        )
                    )

                    if not institution_uuid:

                        skipped_institutions += 1

                        logger.warning(
                            f"SKIPPED INSTITUTION: "
                            f"source_institution_id="
                            f"{source_institution_id}, "
                            f"institution="
                            f"{institution_name}"
                        )

                        continue

                    # ---------------------------------------------
                    # CURRENT TIMESTAMP
                    # ---------------------------------------------

                    current_time = datetime.utcnow()

                    # ---------------------------------------------
                    # INSERT ROW
                    # ---------------------------------------------

                    mapped_row = {

                        "uuid": str(
                            uuid.uuid4()
                        ),

                        "created_at": current_time,

                        "updated_at": current_time,

                        "deleted_at": None,

                        "student_id": None,

                        "user_uuid": user_uuid,

                        "institution_uuid": institution_uuid,

                        "created_by": None,
                    }

                    insert_data.append(
                        mapped_row
                    )

                    inserted_count += 1

                except Exception as row_error:

                    logger.exception(
                        f"Failed processing row: "
                        f"{str(row_error)}"
                    )

        # -------------------------------------------------
        # INSERT
        # -------------------------------------------------

        logger.info(
            f"Inserting "
            f"{len(insert_data)} "
            f"user_institution rows..."
        )

        with self.dest_engine.begin() as conn:

            conn.execute(
                insert(user_institution_table),
                insert_data
            )

        # -------------------------------------------------
        # FINAL LOGS
        # -------------------------------------------------

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{inserted_count} "
            f"user_institution rows."
        )

        logger.info(
            f"Skipped Users: "
            f"{skipped_users}"
        )

        logger.info(
            f"Skipped Institutions: "
            f"{skipped_institutions}"
        )

        logger.info(
            "======================================="
        )

        return inserted_count