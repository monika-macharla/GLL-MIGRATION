import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UserEnrollmentMigrator(BaseMigrator):

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

        source_gl_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        source_gl_user_table = self._manual_reflect(
            "gl_user",
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

        if not destination_enrollment_table.columns:

            raise ValueError(
                "Destination user_enrollments table not found."
            )

        # -------------------------------------------------
        # SOURCE INSTITUTION -> DEST UUID
        # -------------------------------------------------

        source_to_dest_institution_uuid = {}

        with self.source_engine.connect() as source_conn, \
             self.dest_engine.connect() as dest_conn:

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

            for source_id, institution_name in (
                source_lookup.items()
            ):

                destination_uuid = destination_lookup.get(
                    institution_name
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

        batch_size = int(
            self.config.get(
                "user_enrollment_migration_batch_size",
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
                f"Configured user_enrollment batch size "
                f"{batch_size} is too high; "
                f"using {self.MAX_BATCH_SIZE}."
            )

            batch_size = self.MAX_BATCH_SIZE

        logger.info(
            f"Using user_enrollment migration batch size: "
            f"{batch_size}"
        )

        allowed_institution_types = [
            "university",
            "employer",
            "parentuniversity",
            "serviceprovider",
            "regionalServiceProvider"
        ]

        migrated_count = 0
        batch_number = 0
        skipped_users = 0
        skipped_institutions = 0
        skipped_existing = 0
        skipped_duplicates = 0
        row_errors = 0
        migrated_enrollment_codes = set()

        with self.source_engine.connect() as source_conn:

            last_source_id = None

            while True:

                query = (
                    select(
                        source_enrollment_table,
                        source_gl_student_table.c.school_student_id,
                        source_gl_user_table.c.username
                    )
                    .select_from(
                        source_enrollment_table
                        .join(
                            source_gl_student_table,
                            source_enrollment_table.c.student_id
                            == source_gl_student_table.c.id
                        )
                        .join(
                            source_gl_user_table,
                            source_gl_student_table.c.user_id
                            == source_gl_user_table.c.id
                        )
                        .join(
                            source_institutions_table,
                            source_enrollment_table.c.institution_id
                            == source_institutions_table.c.id
                        )
                    )
                    .where(
                        source_gl_user_table.c.user_type == "student"
                    )
                    .where(
                        source_gl_user_table.c.username.is_not(None)
                    )
                    .where(
                        source_gl_user_table.c.username != ""
                    )
                    .where(
                        source_institutions_table.c.institution_type.in_(
                            allowed_institution_types
                        )
                    )
                    .order_by(
                        source_enrollment_table.c.id
                    )
                    .limit(
                        batch_size
                    )
                )

                if last_source_id is not None:

                    query = query.where(
                        source_enrollment_table.c.id > last_source_id
                    )

                results = source_conn.execute(
                    query
                ).fetchall()

                if not results:

                    break

                chunk_start_id = results[0]._mapping.get(
                    source_enrollment_table.c.id
                )

                chunk_end_id = results[-1]._mapping.get(
                    source_enrollment_table.c.id
                )

                logger.info(
                    f"Processing user_enrollments chunk "
                    f"id {chunk_start_id} to {chunk_end_id}, "
                    f"{len(results)} rows."
                )

                user_names = []
                enrollment_codes = []

                for row in results:

                    row_dict = row._mapping

                    last_source_id = row_dict.get(
                        source_enrollment_table.c.id
                    )

                    user_name = row_dict.get(
                        source_gl_user_table.c.username
                    )

                    if user_name:

                        user_names.append(
                            user_name
                        )

                    enrollment_code = row_dict.get(
                        source_enrollment_table.c.enrollment_UUID
                    )

                    if enrollment_code:

                        enrollment_codes.append(
                            enrollment_code
                        )

                destination_user_lookup = {}

                if user_names:

                    with self.dest_engine.connect() as dest_conn:

                        user_results = dest_conn.execute(
                            select(
                                destination_users_table.c.uuid,
                                destination_users_table.c.user_name
                            ).where(
                                destination_users_table.c.user_name.in_(
                                    list(set(user_names))
                                )
                            )
                        )

                        for user_row in user_results:

                            user_row_dict = user_row._mapping

                            destination_user_lookup[
                                self.normalize(
                                    user_row_dict.get(
                                        destination_users_table.c.user_name
                                    )
                                )
                            ] = user_row_dict.get(
                                destination_users_table.c.uuid
                            )

                existing_enrollment_codes = set()

                if enrollment_codes:

                    with self.dest_engine.connect() as dest_conn:

                        existing_results = dest_conn.execute(
                            select(
                                destination_enrollment_table.c.enrollment_code
                            ).where(
                                destination_enrollment_table
                                .c
                                .enrollment_code
                                .in_(
                                    list(set(enrollment_codes))
                                )
                            )
                        )

                        for existing_row in existing_results:

                            existing_enrollment_codes.add(
                                existing_row._mapping.get(
                                    destination_enrollment_table
                                    .c
                                    .enrollment_code
                                )
                            )

                insert_data = []
                prepared_enrollment_codes = set()
                chunk_skipped_users = 0
                chunk_skipped_institutions = 0
                chunk_skipped_existing = 0
                chunk_skipped_duplicates = 0
                chunk_row_errors = 0

                for row in results:

                    try:

                        row_dict = row._mapping

                        source_institution_id = row_dict.get(
                            source_enrollment_table.c.institution_id
                        )

                        institution_uuid = (
                            source_to_dest_institution_uuid.get(
                                source_institution_id
                            )
                        )

                        if not institution_uuid:

                            chunk_skipped_institutions += 1

                            continue

                        user_uuid = destination_user_lookup.get(
                            self.normalize(
                                row_dict.get(
                                    source_gl_user_table.c.username
                                )
                            )
                        )

                        if not user_uuid:

                            chunk_skipped_users += 1

                            continue

                        enrollment_code = row_dict.get(
                            source_enrollment_table.c.enrollment_UUID
                        )

                        if enrollment_code in existing_enrollment_codes:

                            chunk_skipped_existing += 1

                            continue

                        if (
                            enrollment_code in migrated_enrollment_codes
                            or enrollment_code in prepared_enrollment_codes
                        ):

                            chunk_skipped_duplicates += 1

                            continue

                        current_time = datetime.utcnow()

                        student_number = (
                            row_dict.get(
                                source_enrollment_table.c.student_number
                            )
                            or row_dict.get(
                                source_gl_student_table.c.school_student_id
                            )
                        )

                        insert_data.append({
                            "uuid": str(
                                uuid.uuid4()
                            ),
                            "created_at": current_time,
                            "updated_at": current_time,
                            "deleted_at": None,
                            "enrollment_code": enrollment_code,
                            "student_number": student_number,
                            "user_uuid": user_uuid,
                            "institution_uuid": institution_uuid,
                            "created_by": user_uuid,
                            "updated_by": user_uuid,
                        })

                        if enrollment_code:

                            prepared_enrollment_codes.add(
                                enrollment_code
                            )

                    except Exception as row_error:

                        chunk_row_errors += 1

                        logger.exception(
                            f"Failed processing user_enrollment row: "
                            f"{str(row_error)}"
                        )

                if insert_data:

                    batch_number += 1

                    logger.info(
                        f"User_enrollments insert chunk "
                        f"{batch_number}: "
                        f"inserting {len(insert_data)} rows."
                    )

                    with self.dest_engine.begin() as dest_conn:

                        dest_conn.execute(
                            insert(destination_enrollment_table),
                            insert_data
                        )

                    migrated_count += len(insert_data)
                    migrated_enrollment_codes.update(
                        prepared_enrollment_codes
                    )

                skipped_users += chunk_skipped_users
                skipped_institutions += chunk_skipped_institutions
                skipped_existing += chunk_skipped_existing
                skipped_duplicates += chunk_skipped_duplicates
                row_errors += chunk_row_errors

                if len(results) < batch_size:

                    break

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} "
            f"user_enrollments rows."
        )

        logger.info(
            "======================================="
        )

        return migrated_count
