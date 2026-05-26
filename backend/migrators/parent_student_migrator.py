import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .users_migrator import UsersMigrator

logger = logging.getLogger(__name__)


class ParentStudentMigrator(UsersMigrator):

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "PARENT_STUDENT MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        parent_student_source_table = self._manual_reflect(
            "gl_parent_student",
            self.source_engine,
            self.metadata_source
        )

        parent_table = self._manual_reflect(
            "gl_parent",
            self.source_engine,
            self.metadata_source
        )

        student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        parent_student_dest_table = self._manual_reflect(
            "parent_student",
            self.dest_engine,
            self.metadata_dest
        )

        if not parent_student_dest_table.columns:

            raise ValueError(
                "Destination parent_student table not found."
            )

        batch_size = int(
            self.config.get(
                "parent_student_migration_batch_size",
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
        seen_pairs = set()

        def flush_batches():

            nonlocal migrated_count
            nonlocal batch_number

            if not insert_data:

                return

            batch_number += 1

            logger.info(
                f"Parent_student chunk {batch_number}: "
                f"inserting {len(insert_data)} rows..."
            )

            with self.dest_engine.begin() as conn:

                conn.execute(
                    insert(parent_student_dest_table),
                    insert_data
                )

            migrated_count += len(insert_data)

            insert_data.clear()

        with self.source_engine.connect() as source_conn:

            last_source_id = None

            while True:

                query = (
                    select(parent_student_source_table)
                    .order_by(
                        parent_student_source_table.c.id
                    )
                    .limit(
                        batch_size
                    )
                )

                if last_source_id is not None:

                    query = query.where(
                        parent_student_source_table.c.id
                        > last_source_id
                    )

                results = source_conn.execute(
                    query
                ).fetchall()

                if not results:

                    break

                parent_ids = []
                student_ids = []

                for row in results:

                    row_dict = row._mapping

                    last_source_id = row_dict.get(
                        parent_student_source_table.c.id
                    )

                    parent_id = row_dict.get(
                        parent_student_source_table.c.gl_parent_id
                    )

                    student_id = row_dict.get(
                        parent_student_source_table.c.student_id
                    )

                    if parent_id is not None:

                        parent_ids.append(parent_id)

                    if student_id is not None:

                        student_ids.append(student_id)

                parent_lookup = {}

                if parent_ids:

                    parent_results = source_conn.execute(
                        select(
                            parent_table.c.id,
                            parent_table.c.user_id
                        ).where(
                            parent_table.c.id.in_(
                                list(set(parent_ids))
                            )
                        )
                    )

                    for parent_row in parent_results:

                        parent_row_dict = parent_row._mapping

                        parent_lookup[
                            parent_row_dict.get(
                                parent_table.c.id
                            )
                        ] = parent_row_dict.get(
                            parent_table.c.user_id
                        )

                student_lookup = {}

                if student_ids:

                    student_results = source_conn.execute(
                        select(
                            student_table.c.id,
                            student_table.c.user_id
                        ).where(
                            student_table.c.id.in_(
                                list(set(student_ids))
                            )
                        )
                    )

                    for student_row in student_results:

                        student_row_dict = student_row._mapping

                        student_lookup[
                            student_row_dict.get(
                                student_table.c.id
                            )
                        ] = student_row_dict.get(
                            student_table.c.user_id
                        )

                source_user_ids = set(
                    value
                    for value in (
                        list(parent_lookup.values())
                        + list(student_lookup.values())
                    )
                    if value is not None
                )

                user_uuid_lookup = {}

                if source_user_ids:

                    user_results = source_conn.execute(
                        select(
                            user_table.c.id,
                            user_table.c.user_id
                        ).where(
                            user_table.c.id.in_(
                                list(source_user_ids)
                            )
                            | user_table.c.user_id.in_(
                                list(source_user_ids)
                            )
                        )
                    )

                    for user_row in user_results:

                        user_row_dict = user_row._mapping

                        source_gl_user_id = user_row_dict.get(
                            user_table.c.id
                        )

                        source_jhi_user_id = user_row_dict.get(
                            user_table.c.user_id
                        )

                        if source_jhi_user_id is not None:

                            generated_uuid = self._stable_uuid(
                                "gl_user.user_id",
                                source_jhi_user_id
                            )

                            user_uuid_lookup[
                                source_jhi_user_id
                            ] = generated_uuid

                        else:

                            generated_uuid = self._stable_uuid(
                                "gl_user.id",
                                source_gl_user_id
                            )

                        user_uuid_lookup[
                            source_gl_user_id
                        ] = generated_uuid

                now = datetime.utcnow()

                for row in results:

                    row_dict = row._mapping

                    parent_source_user_id = parent_lookup.get(
                        row_dict.get(
                            parent_student_source_table.c.gl_parent_id
                        )
                    )

                    student_source_user_id = student_lookup.get(
                        row_dict.get(
                            parent_student_source_table.c.student_id
                        )
                    )

                    parent_uuid = user_uuid_lookup.get(
                        parent_source_user_id
                    )

                    student_uuid = user_uuid_lookup.get(
                        student_source_user_id
                    )

                    if not parent_uuid or not student_uuid:

                        continue

                    pair_key = (
                        parent_uuid,
                        student_uuid
                    )

                    if pair_key in seen_pairs:

                        continue

                    seen_pairs.add(pair_key)

                    insert_data.append({
                        "uuid": str(
                            uuid.uuid4()
                        ),
                        "parent_uuid": parent_uuid,
                        "student_uuid": student_uuid,
                        "created_at": now,
                        "updated_at": now,
                        "deleted_at": None,
                    })

                flush_batches()

        flush_batches()

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} parent_student rows."
        )

        logger.info(
            "======================================="
        )

        return migrated_count
