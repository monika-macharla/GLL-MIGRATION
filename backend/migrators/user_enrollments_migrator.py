import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UserEnrollmentMigrator(BaseMigrator):

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
            "USER_ENROLLMENT MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        # -------------------------------------------------
        # Reflect Source Tables
        # -------------------------------------------------

        source_enrollment_table = self._manual_reflect(
            "enrollment",
            self.source_engine,
            self.metadata_source
        )

        source_users_table = self._manual_reflect(
            "jhi_user",
            self.source_engine,
            self.metadata_source
        )

        source_institutions_table = self._manual_reflect(
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

        destination_enrollment_table = self._manual_reflect(
            "user_enrollments",
            self.dest_engine,
            self.metadata_dest
        )

        # -------------------------------------------------
        # SOURCE USER -> DEST USER UUID
        # -------------------------------------------------

        source_to_dest_user_uuid = {}

        with self.source_engine.connect() as source_conn, \
             self.dest_engine.connect() as dest_conn:

            # ---------------------------------------------
            # Source users
            # ---------------------------------------------

            source_results = source_conn.execute(
                select(
                    source_users_table.c.id,
                    source_users_table.c.login
                )
            )

            source_lookup = {}

            for row in source_results:

                row_dict = row._mapping

                source_lookup[
                    row_dict.get(
                        source_users_table.c.id
                    )
                ] = self.normalize(
                    row_dict.get(
                        source_users_table.c.login
                    )
                )

            # ---------------------------------------------
            # Destination users
            # ---------------------------------------------

            destination_results = dest_conn.execute(
                select(
                    destination_users_table.c.uuid,
                    destination_users_table.c.user_name
                )
            )

            destination_lookup = {}

            for row in destination_results:

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

            for source_user_id, login in (
                source_lookup.items()
            ):

                destination_uuid = (
                    destination_lookup.get(
                        login
                    )
                )

                if destination_uuid:

                    source_to_dest_user_uuid[
                        source_user_id
                    ] = destination_uuid

        logger.info(
            f"Loaded "
            f"{len(source_to_dest_user_uuid)} "
            f"user UUID mappings."
        )

        # -------------------------------------------------
        # SOURCE INSTITUTION -> DEST UUID
        # -------------------------------------------------

        source_to_dest_institution_uuid = {}

        with self.source_engine.connect() as source_conn, \
             self.dest_engine.connect() as dest_conn:

            # ---------------------------------------------
            # Source institutions
            # ---------------------------------------------

            source_results = source_conn.execute(
                select(
                    source_institutions_table.c.id,
                    source_institutions_table.c.name
                )
            )

            source_lookup = {}

            for row in source_results:

                row_dict = row._mapping

                source_lookup[
                    row_dict.get(
                        source_institutions_table.c.id
                    )
                ] = self.normalize(
                    row_dict.get(
                        source_institutions_table.c.name
                    )
                )

            # ---------------------------------------------
            # Destination institutions
            # ---------------------------------------------

            destination_results = dest_conn.execute(
                select(
                    destination_institutions_table.c.uuid,
                    destination_institutions_table.c.name
                )
            )

            destination_lookup = {}

            for row in destination_results:

                row_dict = row._mapping

                destination_lookup[
                    self.normalize(
                        row_dict.get(
                            destination_institutions_table.c.name
                        )
                    )
                ] = row_dict.get(
                    destination_institutions_table.c.uuid
                )

            # ---------------------------------------------
            # Final mapping
            # ---------------------------------------------

            for source_id, institution_name in (
                source_lookup.items()
            ):

                destination_uuid = (
                    destination_lookup.get(
                        institution_name
                    )
                )

                if destination_uuid:

                    source_to_dest_institution_uuid[
                        source_id
                    ] = destination_uuid

        logger.info(
            f"Loaded "
            f"{len(source_to_dest_institution_uuid)} "
            f"institution UUID mappings."
        )

        # -------------------------------------------------
        # Counters
        # -------------------------------------------------

        skipped_users = 0

        skipped_institutions = 0

        inserted_count = 0

        # -------------------------------------------------
        # Prepare Insert Data
        # -------------------------------------------------

        insert_data = []

        with self.source_engine.connect() as conn:

            results = conn.execute(
                select(source_enrollment_table)
            )

            for row in results:

                try:

                    row_dict = row._mapping

                    source_student_id = row_dict.get(
                        source_enrollment_table.c.student_id
                    )

                    source_institution_id = row_dict.get(
                        source_enrollment_table.c.institution_id
                    )

                    # ---------------------------------------------
                    # USER UUID
                    # ---------------------------------------------

                    user_uuid = (
                        source_to_dest_user_uuid.get(
                            source_student_id
                        )
                    )

                    if not user_uuid:

                        skipped_users += 1

                        logger.warning(
                            f"SKIPPED USER: "
                            f"student_id="
                            f"{source_student_id}"
                        )

                        continue

                    # ---------------------------------------------
                    # INSTITUTION UUID
                    # ---------------------------------------------

                    institution_uuid = (
                        source_to_dest_institution_uuid.get(
                            source_institution_id
                        )
                    )

                    if not institution_uuid:

                        skipped_institutions += 1

                        logger.warning(
                            f"SKIPPED INSTITUTION: "
                            f"institution_id="
                            f"{source_institution_id}"
                        )

                        continue

                    # ---------------------------------------------
                    # Current Timestamp
                    # ---------------------------------------------

                    current_time = datetime.utcnow()

                    # ---------------------------------------------
                    # Insert Row
                    # ---------------------------------------------

                    mapped_row = {

                        "uuid": str(
                            uuid.uuid4()
                        ),

                        "created_at": current_time,

                        "updated_at": current_time,

                        "deleted_at": None,

                        "enrollment_code": row_dict.get(
                            source_enrollment_table.c.enrollment_UUID
                        ),

                        "student_number": None,

                        "user_uuid": user_uuid,

                        "institution_uuid": institution_uuid,

                        "created_by": None,

                        "updated_by": None,
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
        # No Data
        # -------------------------------------------------

        if not insert_data:

            logger.warning(
                "No user_enrollments found."
            )

            return 0

        # -------------------------------------------------
        # Insert
        # -------------------------------------------------

        logger.info(
            f"Inserting "
            f"{len(insert_data)} "
            f"user_enrollments rows..."
        )

        with self.dest_engine.begin() as conn:

            conn.execute(
                insert(destination_enrollment_table),
                insert_data
            )

        # -------------------------------------------------
        # Final Logs
        # -------------------------------------------------

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{inserted_count} "
            f"user_enrollments rows."
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