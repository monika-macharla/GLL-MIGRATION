import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .users_migrator import UsersMigrator

logger = logging.getLogger(__name__)


class UserInstitutionMigrator(UsersMigrator):

    # -------------------------------------------------
    # Normalize Helper
    # -------------------------------------------------

    def normalize(self, value):

        return (
            str(value or "")
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

        mappings = self.config.get(
            "mappings",
            []
        )

        source_table_name = None

        for mapping in mappings:

            if mapping.get(
                "destination_table"
            ) == "user_institution":

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

        gl_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        source_institution_table = self._manual_reflect(
            "institution",
            self.source_engine,
            self.metadata_source
        )

        dest_institutions_table = self._manual_reflect(
            "institutions",
            self.dest_engine,
            self.metadata_dest
        )

        user_institution_table = self._manual_reflect(
            "user_institution",
            self.dest_engine,
            self.metadata_dest
        )

        if not user_institution_table.columns:

            raise ValueError(
                "Destination user_institution table not found."
            )

        if not dest_institutions_table.columns:

            raise ValueError(
                "Destination institutions table not found."
            )

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

                source_institution_lookup[
                    row_dict.get(
                        source_institution_table.c.id
                    )
                ] = self.normalize(
                    row_dict.get(
                        source_institution_table.c.name
                    )
                )

        destination_institution_lookup = {}

        with self.dest_engine.connect() as conn:

            results = conn.execute(
                select(
                    dest_institutions_table.c.uuid,
                    dest_institutions_table.c.name
                )
            )

            for row in results:

                row_dict = row._mapping

                destination_institution_lookup[
                    self.normalize(
                        row_dict.get(
                            dest_institutions_table.c.name
                        )
                    )
                ] = row_dict.get(
                    dest_institutions_table.c.uuid
                )

        source_to_dest_institution_uuid = {}

        for source_institution_id, institution_key in (
            source_institution_lookup.items()
        ):

            destination_uuid = (
                destination_institution_lookup.get(
                    institution_key
                )
            )

            if destination_uuid:

                source_to_dest_institution_uuid[
                    source_institution_id
                ] = destination_uuid

        logger.info(
            f"Mapped "
            f"{len(source_to_dest_institution_uuid)} "
            f"source institutions to destination institutions."
        )

        batch_size = int(
            self.config.get(
                "user_institution_migration_batch_size",
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
                f"Configured user_institution batch size "
                f"{batch_size} is too high; "
                f"using {self.MAX_BATCH_SIZE}."
            )

            batch_size = self.MAX_BATCH_SIZE

        logger.info(
            f"Using user_institution migration batch size: "
            f"{batch_size}"
        )

        insert_batch_size = int(
            self.config.get(
                "user_institution_insert_batch_size",
                batch_size
            )
        )

        if insert_batch_size < 1:

            insert_batch_size = batch_size

        if insert_batch_size > batch_size:

            insert_batch_size = batch_size

        logger.info(
            f"Using user_institution insert batch size: "
            f"{insert_batch_size}"
        )

        insert_data = []
        migrated_count = 0
        batch_number = 0
        seen_user_institution_pairs = set()
        total_student_rows_prepared = 0
        total_non_student_rows_prepared = 0
        total_missing_institutions = 0
        total_duplicate_pairs = 0
        total_skipped_users = 0
        total_missing_student_ids = 0

        with self.dest_engine.connect() as conn:

            existing_results = conn.execute(
                select(
                    user_institution_table.c.user_uuid,
                    user_institution_table.c.institution_uuid
                ).where(
                    user_institution_table.c.deleted_at.is_(None)
                )
            )

            for existing_row in existing_results:

                existing_row_dict = existing_row._mapping

                seen_user_institution_pairs.add((
                    existing_row_dict.get(
                        user_institution_table.c.user_uuid
                    ),
                    existing_row_dict.get(
                        user_institution_table.c.institution_uuid
                    )
                ))

        logger.info(
            f"Loaded {len(seen_user_institution_pairs)} "
            f"existing destination user_institution pairs "
            f"for idempotent reruns."
        )

        def flush_batches(force=False):

            nonlocal migrated_count
            nonlocal batch_number

            if not insert_data:

                return

            if (
                not force
                and len(insert_data) < insert_batch_size
            ):

                return

            batch_number += 1

            batch_rows = insert_data[:insert_batch_size]

            logger.info(
                f"User_institution insert chunk {batch_number}: "
                f"inserting {len(batch_rows)} rows; "
                f"already_inserted={migrated_count}."
            )

            with self.dest_engine.begin() as conn:

                conn.execute(
                    insert(user_institution_table),
                    batch_rows
                )

            migrated_count += len(batch_rows)

            logger.info(
                f"User_institution insert chunk {batch_number}: "
                f"insert complete; "
                f"total_inserted={migrated_count}."
            )

            del insert_data[:insert_batch_size]

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

                chunk_start_id = results[0]._mapping.get(
                    source_table.c.id
                )

                chunk_end_id = results[-1]._mapping.get(
                    source_table.c.id
                )

                logger.info(
                    f"Processing user_institution source chunk: "
                    f"id {chunk_start_id} to {chunk_end_id}, "
                    f"{len(results)} gl_user rows."
                )

                chunk_student_rows_prepared = 0
                chunk_non_student_rows_prepared = 0
                chunk_missing_institutions = 0
                chunk_duplicate_pairs = 0
                chunk_skipped_users = 0
                chunk_missing_student_ids = 0
                chunk_student_source_rows = 0
                chunk_institution_user_source_rows = 0

                source_user_uuid_by_jhi_id = {}
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

                        source_user_uuid_by_jhi_id[
                            source_jhi_user_id
                        ] = generated_uuid

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

                gl_student_chunk_lookup = {}
                seen_student_pairs = set()

                if chunk_gl_user_ids:

                    student_source_user_ids = list(
                        set(chunk_gl_user_ids)
                    )

                    if student_source_user_ids:

                        gl_student_results = source_conn.execute(
                            select(
                                gl_student_table.c.user_id,
                                gl_student_table.c.institution_id,
                                gl_student_table.c.school_student_id
                            ).where(
                                gl_student_table.c.user_id.in_(
                                    student_source_user_ids
                                )
                            )
                        )

                        for gl_student_row in gl_student_results:

                            gl_student_row_dict = (
                                gl_student_row._mapping
                            )

                            source_student_user_id = (
                                gl_student_row_dict.get(
                                    gl_student_table.c.user_id
                                )
                            )

                            source_institution_id = (
                                gl_student_row_dict.get(
                                    gl_student_table.c.institution_id
                                )
                            )

                            generated_uuid = (
                                source_user_uuid_by_gl_id.get(
                                    source_student_user_id
                                )
                                or source_user_uuid_by_jhi_id.get(
                                    source_student_user_id
                                )
                            )

                            if (
                                not generated_uuid
                                or source_institution_id is None
                            ):

                                continue

                            student_pair_key = (
                                generated_uuid,
                                source_institution_id
                            )

                            if student_pair_key in seen_student_pairs:

                                continue

                            seen_student_pairs.add(
                                student_pair_key
                            )

                            gl_student_chunk_lookup.setdefault(
                                source_student_user_id,
                                []
                            ).append({
                                "source_institution_id": (
                                    source_institution_id
                                ),
                                "student_number": (
                                    gl_student_row_dict.get(
                                        gl_student_table.c.school_student_id
                                    )
                                ),
                            })

                for row in results:

                    row_dict = row._mapping
                    source_user_id = row_dict.get(
                        source_table.c.id
                    )
                    source_jhi_user_id = row_dict.get(
                        source_table.c.user_id
                    )
                    last_source_id = source_user_id

                    generated_uuid = source_user_uuid_by_gl_id.get(
                        source_user_id
                    )

                    if not generated_uuid:

                        chunk_skipped_users += 1

                        continue

                    source_dates = source_user_dates_by_gl_id.get(
                        source_user_id,
                        {}
                    )

                    source_user_type = source_user_type_by_gl_id.get(
                        source_user_id
                    )

                    created_date = source_dates.get(
                        "created_at"
                    )

                    updated_date = source_dates.get(
                        "updated_at",
                        created_date
                    )

                    student_institution_rows = []

                    student_institution_rows = (
                        gl_student_chunk_lookup.get(
                            source_user_id,
                            []
                        )
                    )

                    for student_institution_data in (
                        student_institution_rows
                    ):

                        chunk_student_source_rows += 1

                        append_status = self._append_user_institution(
                            insert_data,
                            seen_user_institution_pairs,
                            generated_uuid,
                            student_institution_data.get(
                                "source_institution_id"
                            ),
                            source_to_dest_institution_uuid,
                            created_date,
                            updated_date,
                            generated_uuid,
                            student_institution_data.get(
                                "student_number"
                            ),
                            require_student_id=True
                        )

                        if append_status == "inserted":

                            chunk_student_rows_prepared += 1

                        elif append_status == "missing_institution":

                            chunk_missing_institutions += 1

                        elif append_status == "duplicate":

                            chunk_duplicate_pairs += 1

                        elif append_status == "missing_student_id":

                            chunk_missing_student_ids += 1

                        flush_batches()

                    if source_user_type == "student":

                        continue

                    institution_row = (
                        institution_user_chunk_lookup.get(
                            source_user_id
                        )
                    )

                    if not institution_row:

                        continue

                    chunk_institution_user_source_rows += 1

                    append_status = self._append_user_institution(
                        insert_data,
                        seen_user_institution_pairs,
                        generated_uuid,
                        institution_row.get(
                            institution_user_table.c.institution_id
                        ),
                        source_to_dest_institution_uuid,
                        created_date,
                        updated_date,
                        generated_uuid,
                        None
                    )

                    if append_status == "inserted":

                        chunk_non_student_rows_prepared += 1

                    elif append_status == "missing_institution":

                        chunk_missing_institutions += 1

                    elif append_status == "duplicate":

                        chunk_duplicate_pairs += 1

                    flush_batches()

                total_student_rows_prepared += (
                    chunk_student_rows_prepared
                )
                total_non_student_rows_prepared += (
                    chunk_non_student_rows_prepared
                )
                total_missing_institutions += (
                    chunk_missing_institutions
                )
                total_duplicate_pairs += chunk_duplicate_pairs
                total_skipped_users += chunk_skipped_users
                total_missing_student_ids += (
                    chunk_missing_student_ids
                )

                logger.info(
                    f"Prepared user_institution source chunk: "
                    f"id {chunk_start_id} to {chunk_end_id}; "
                    f"student_source_rows="
                    f"{chunk_student_source_rows}, "
                    f"institution_user_source_rows="
                    f"{chunk_institution_user_source_rows}, "
                    f"student_rows_prepared="
                    f"{chunk_student_rows_prepared}, "
                    f"non_student_rows_prepared="
                    f"{chunk_non_student_rows_prepared}, "
                    f"missing_institutions="
                    f"{chunk_missing_institutions}, "
                    f"missing_student_ids="
                    f"{chunk_missing_student_ids}, "
                    f"duplicate_pairs="
                    f"{chunk_duplicate_pairs}, "
                    f"skipped_users="
                    f"{chunk_skipped_users}, "
                    f"pending_insert_rows="
                    f"{len(insert_data)}."
                )

                flush_batches(force=True)

        flush_batches(force=True)

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} user_institution rows."
        )

        logger.info(
            f"user_institution summary: "
            f"student_rows_prepared="
            f"{total_student_rows_prepared}, "
            f"non_student_rows_prepared="
            f"{total_non_student_rows_prepared}, "
            f"missing_institutions="
            f"{total_missing_institutions}, "
            f"missing_student_ids="
            f"{total_missing_student_ids}, "
            f"duplicate_pairs="
            f"{total_duplicate_pairs}, "
            f"skipped_users="
            f"{total_skipped_users}."
        )

        logger.info(
            "======================================="
        )

        return migrated_count

    def _append_user_institution(
        self,
        insert_data,
        seen_user_institution_pairs,
        user_uuid,
        source_institution_id,
        source_to_dest_institution_uuid,
        created_at,
        updated_at,
        created_by,
        student_id,
        require_student_id=False
    ):

        if (
            require_student_id
            and (
                student_id is None
                or not str(student_id).strip()
            )
        ):

            return "missing_student_id"

        institution_uuid = source_to_dest_institution_uuid.get(
            source_institution_id
        )

        if not institution_uuid:

            return "missing_institution"

        user_institution_key = (
            user_uuid,
            institution_uuid
        )

        if user_institution_key in seen_user_institution_pairs:

            return "duplicate"

        seen_user_institution_pairs.add(
            user_institution_key
        )

        insert_data.append({
            "uuid": str(
                uuid.uuid4()
            ),
            "created_at": created_at,
            "updated_at": updated_at,
            "deleted_at": None,
            "student_id": student_id,
            "user_uuid": user_uuid,
            "institution_uuid": institution_uuid,
            "created_by": created_by,
        })

        return "inserted"
