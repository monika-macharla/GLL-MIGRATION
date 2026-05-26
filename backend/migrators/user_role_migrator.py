import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .users_migrator import UsersMigrator

logger = logging.getLogger(__name__)


class UserRoleMigrator(UsersMigrator):

    # -------------------------------------------------
    # Main Migration
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "USER_ROLE MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        mappings = self.config.get(
            "mappings",
            []
        )

        source_table_name = None

        for mapping in mappings:

            if mapping.get(
                "destination_table"
            ) == "user_role":

                source_table_name = mapping.get(
                    "source_table"
                )

                break

        if not source_table_name:

            source_table_name = "gl_user"

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

        roles_table = self._manual_reflect(
            "role",
            self.dest_engine,
            self.metadata_dest
        )

        user_role_table = self._manual_reflect(
            "user_role",
            self.dest_engine,
            self.metadata_dest
        )

        if not user_role_table.columns:

            raise ValueError(
                "Destination user_role table not found."
            )

        if not roles_table.columns:

            raise ValueError(
                "Destination role table not found."
            )

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

        batch_size = int(
            self.config.get(
                "user_role_migration_batch_size",
                self.config.get(
                    "batch_size",
                    self.DEFAULT_BATCH_SIZE
                )
            )
        )

        if batch_size < 1:

            batch_size = self.DEFAULT_BATCH_SIZE

        if batch_size > self.MAX_BATCH_SIZE:

            batch_size = self.MAX_BATCH_SIZE

        insert_data = []
        migrated_count = 0
        batch_number = 0

        def flush_batches():

            nonlocal migrated_count
            nonlocal batch_number

            if not insert_data:

                return

            batch_number += 1

            logger.info(
                f"User_role chunk {batch_number}: "
                f"inserting {len(insert_data)} rows..."
            )

            with self.dest_engine.begin() as conn:

                conn.execute(
                    insert(user_role_table),
                    insert_data
                )

            migrated_count += len(insert_data)

            insert_data.clear()

        query = select(source_table)

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

                source_user_uuid_by_gl_id = {}
                source_user_type_by_gl_id = {}
                source_user_dates_by_gl_id = {}

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

                chunk_non_student_gl_ids = [
                    source_gl_user_id
                    for source_gl_user_id in source_user_uuid_by_gl_id
                    if source_user_type_by_gl_id.get(
                        source_gl_user_id
                    ) != "student"
                ]

                institution_user_chunk_lookup = {}

                if chunk_non_student_gl_ids:

                    institution_results = source_conn.execute(
                        select(
                            institution_user_table.c.user_id,
                            institution_user_table.c.role_id
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

                for row in results:

                    row_dict = row._mapping
                    source_user_id = row_dict.get(
                        source_table.c.id
                    )
                    last_source_id = source_user_id

                    generated_uuid = source_user_uuid_by_gl_id.get(
                        source_user_id
                    )

                    if not generated_uuid:

                        continue

                    source_user_type = source_user_type_by_gl_id.get(
                        source_user_id
                    )

                    institution_row = (
                        institution_user_chunk_lookup.get(
                            source_user_id
                        )
                    )

                    role_uuid = None

                    if institution_row:

                        source_role_id = institution_row.get(
                            institution_user_table.c.role_id
                        )

                        role_code = self.INSTITUTION_ROLE_MAPPING.get(
                            source_role_id
                        )

                        if role_code:

                            role_uuid = role_lookup.get(
                                role_code
                            )

                    if not role_uuid:

                        fallback_role_code = (
                            self.USER_TYPE_ROLE_MAPPING.get(
                                source_user_type
                            )
                        )

                        if fallback_role_code:

                            role_uuid = role_lookup.get(
                                fallback_role_code
                            )

                    if not role_uuid:

                        continue

                    source_dates = source_user_dates_by_gl_id.get(
                        source_user_id,
                        {}
                    )

                    insert_data.append({
                        "uuid": str(
                            uuid.uuid4()
                        ),
                        "created_at": source_dates.get(
                            "created_at"
                        ),
                        "updated_at": source_dates.get(
                            "updated_at"
                        ),
                        "deleted_at": None,
                        "user_uuid": generated_uuid,
                        "role": role_uuid,
                        "created_by": generated_uuid,
                    })

                flush_batches()

        flush_batches()

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} user_role rows."
        )

        logger.info(
            "======================================="
        )

        return migrated_count
