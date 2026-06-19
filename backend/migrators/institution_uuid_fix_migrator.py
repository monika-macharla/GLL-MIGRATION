import logging
import re
import time
import uuid
import zlib

from collections import defaultdict

from sqlalchemy import bindparam, text
from sqlalchemy.exc import OperationalError

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class InstitutionUuidFixMigrator(BaseMigrator):

    DEFAULT_AUTH_DATABASE = "gllauthserviceuatmigration"
    DEFAULT_DATA_DATABASE = "glldataingestionuatmigration"

    TARGET_COLUMNS = [
        "institution_id",
        "institution_uuid",
    ]

    STRING_DATA_TYPES = [
        "char",
        "varchar",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
    ]

    DEFAULT_BATCH_SIZE = 100000
    DEFAULT_ENSURE_INDEXES = True
    DEFAULT_LOCK_WAIT_TIMEOUT = 5
    DEFAULT_SKIP_LOCKED_TARGETS = True
    DEFAULT_DIRECT_UPDATE_THRESHOLD = 1000000

    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")

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

        run_started_at = time.monotonic()

        logger.info(
            "[RUN] Starting Institution UUID Fix Migration"
        )

        settings = self.config.get(
            "institution_uuid_fix",
            {}
        )

        auth_database = settings.get(
            "auth_database"
        ) or self.config.get(
            "auth_database"
        ) or self.DEFAULT_AUTH_DATABASE

        data_database = settings.get(
            "data_database"
        ) or self.config.get(
            "data_database"
        ) or self.DEFAULT_DATA_DATABASE

        self._validate_identifier(auth_database)
        self._validate_identifier(data_database)

        targets = self._resolve_targets(
            data_database,
            settings
        )

        batch_size = int(
            settings.get(
                "batch_size",
                self.DEFAULT_BATCH_SIZE
            )
        )

        if batch_size <= 0:

            raise ValueError(
                "institution_uuid_fix.batch_size must be greater than 0"
            )

        ensure_indexes = settings.get(
            "ensure_indexes",
            self.DEFAULT_ENSURE_INDEXES
        )
        lock_wait_timeout = int(
            settings.get(
                "lock_wait_timeout",
                self.DEFAULT_LOCK_WAIT_TIMEOUT
            )
        )
        skip_locked_targets = bool(
            settings.get(
                "skip_locked_targets",
                self.DEFAULT_SKIP_LOCKED_TARGETS
            )
        )
        direct_update_threshold = int(
            settings.get(
                "direct_update_threshold",
                self.DEFAULT_DIRECT_UPDATE_THRESHOLD
            )
        )
        skip_count_tables = {
            str(table).strip().lower()
            for table in settings.get(
                "skip_count_tables",
                []
            )
        }

        logger.info(
            "[RUN] Config: "
            f"auth_database={auth_database}, "
            f"data_database={data_database}, "
            f"batch_size={batch_size}, "
            f"ensure_indexes={bool(ensure_indexes)}, "
            f"lock_wait_timeout={lock_wait_timeout}, "
            f"skip_locked_targets={skip_locked_targets}, "
            f"direct_update_threshold={direct_update_threshold}, "
            f"skip_count_tables={sorted(skip_count_tables)}, "
            f"target_count={len(targets)}"
        )

        logger.info(
            "[MAPPING] Building institution UUID mapping"
        )

        mapping, ambiguous, missing = self._build_mapping(
            auth_database
        )

        logger.info(
            f"[MAPPING] Institution UUID mapping rows: {len(mapping)}"
        )
        logger.info(
            f"[MAPPING] Ambiguous source-name matches skipped: "
            f"{len(ambiguous)}"
        )
        logger.info(
            f"[MAPPING] Missing auth-name matches skipped: "
            f"{len(missing)}"
        )

        if not mapping:

            logger.info(
                "[RUN] No institution UUID rows need repair"
            )

            return 0

        updated_total = self._apply_updates(
            data_database,
            targets,
            mapping,
            batch_size,
            bool(ensure_indexes),
            lock_wait_timeout,
            skip_locked_targets,
            direct_update_threshold,
            skip_count_tables
        )

        logger.info(
            "[RUN] Institution UUID fix completed: "
            f"updated_total={updated_total}, "
            f"elapsed={self._format_elapsed(run_started_at)}"
        )

        return updated_total

    def _resolve_targets(
        self,
        data_database,
        settings
    ):

        configured_targets = settings.get(
            "targets"
        )

        if configured_targets:

            targets = [
                (table, column)
                for table, column in configured_targets
            ]

            targets = self._filter_skipped_targets(
                targets,
                settings
            )

            for table, column in targets:

                self._validate_identifier(table)
                self._validate_identifier(column)

            logger.info(
                f"[TARGETS] Using {len(targets)} configured "
                f"institution UUID fix targets"
            )

            return targets

        include_views = bool(
            settings.get("include_views")
        )

        table_types = [
            "BASE TABLE"
        ]

        if include_views:

            table_types.append(
                "VIEW"
            )

        with self.dest_engine.connect() as dest_conn:

            rows = dest_conn.execute(
                text(
                    """
                    SELECT
                      c.TABLE_NAME AS table_name,
                      c.COLUMN_NAME AS column_name
                    FROM information_schema.COLUMNS c
                    JOIN information_schema.TABLES t
                      ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                     AND t.TABLE_NAME = c.TABLE_NAME
                    WHERE c.TABLE_SCHEMA = :data_database
                      AND c.COLUMN_NAME IN :target_columns
                      AND c.DATA_TYPE IN :string_data_types
                      AND t.TABLE_TYPE IN :table_types
                    ORDER BY
                      CASE
                        WHEN c.TABLE_NAME = 'import_course_information'
                        THEN 1
                        ELSE 0
                      END,
                      c.TABLE_NAME,
                      c.COLUMN_NAME
                    """
                ).bindparams(
                    bindparam(
                        "target_columns",
                        expanding=True
                    ),
                    bindparam(
                        "string_data_types",
                        expanding=True
                    ),
                    bindparam(
                        "table_types",
                        expanding=True
                    ),
                ),
                {
                    "data_database": data_database,
                    "target_columns": self.TARGET_COLUMNS,
                    "string_data_types": self.STRING_DATA_TYPES,
                    "table_types": table_types,
                }
            ).mappings().all()

        targets = [
            (
                row["table_name"],
                row["column_name"],
            )
            for row in rows
        ]

        targets = self._filter_skipped_targets(
            targets,
            settings
        )

        for table, column in targets:

            self._validate_identifier(table)
            self._validate_identifier(column)

        logger.info(
            f"[TARGETS] Discovered {len(targets)} institution UUID "
            f"fix targets in {data_database}"
        )

        for table, column in targets:

            logger.info(
                f"[TARGETS] "
                f"{data_database}.{table}.{column}"
            )

        return targets

    def _filter_skipped_targets(
        self,
        targets,
        settings
    ):

        skip_tables = {
            str(table).strip().lower()
            for table in settings.get(
                "skip_tables",
                []
            )
        }

        skip_targets = {
            (
                str(target[0]).strip().lower(),
                str(target[1]).strip().lower()
            )
            for target in settings.get(
                "skip_targets",
                []
            )
        }

        if not skip_tables and not skip_targets:

            return targets

        filtered_targets = []

        for table, column in targets:

            normalized_table = str(table).strip().lower()
            normalized_column = str(column).strip().lower()

            if normalized_table in skip_tables:

                logger.info(
                    f"[TARGETS] Skipping configured table: "
                    f"{table}"
                )

                continue

            if (
                normalized_table,
                normalized_column
            ) in skip_targets:

                logger.info(
                    f"[TARGETS] Skipping configured target: "
                    f"{table}.{column}"
                )

                continue

            filtered_targets.append(
                (
                    table,
                    column
                )
            )

        return filtered_targets

    def _build_mapping(self, auth_database):

        with self.source_engine.connect() as source_conn:

            source_rows = source_conn.execute(
                text(
                    """
                    SELECT id, name
                    FROM institution
                    WHERE name IS NOT NULL
                      AND TRIM(name) <> ''
                    """
                )
            ).mappings().all()

        with self.dest_engine.connect() as dest_conn:

            auth_rows = dest_conn.execute(
                text(
                    f"""
                    SELECT uuid, name
                    FROM `{auth_database}`.`institutions`
                    WHERE deleted_at IS NULL
                      AND name IS NOT NULL
                      AND TRIM(name) <> ''
                    """
                )
            ).mappings().all()

        auth_by_name = defaultdict(list)

        for row in auth_rows:

            auth_by_name[
                self._norm(row["name"])
            ].append(row)

        mapping = []
        ambiguous = []
        missing = []

        for row in source_rows:

            old_uuid = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"gll:institution:{row['id']}"
                )
            )

            matches = auth_by_name.get(
                self._norm(row["name"]),
                []
            )

            if len(matches) == 1:

                new_uuid = str(matches[0]["uuid"])

                if old_uuid != new_uuid:

                    mapping.append(
                        {
                            "old_uuid": old_uuid,
                            "new_uuid": new_uuid,
                            "source_institution_id": row["id"],
                            "institution_name": row["name"],
                        }
                    )

            elif len(matches) > 1:

                ambiguous.append(
                    (
                        row["id"],
                        row["name"],
                        [
                            match["uuid"]
                            for match in matches
                        ],
                    )
                )

            else:

                missing.append(
                    (
                        row["id"],
                        row["name"],
                        old_uuid,
                    )
                )

        return mapping, ambiguous, missing

    def _apply_updates(
        self,
        data_database,
        targets,
        mapping,
        batch_size,
        ensure_indexes,
        lock_wait_timeout,
        skip_locked_targets,
        direct_update_threshold,
        skip_count_tables
    ):

        updated_total = 0
        update_summary = []

        with self.dest_engine.connect() as dest_conn:

            self._set_lock_timeouts(
                dest_conn,
                lock_wait_timeout
            )

            logger.info(
                "[TEMP] Creating temporary institution UUID mapping table"
            )

            dest_conn.execute(
                text(
                    """
                    CREATE TEMPORARY TABLE tmp_institution_uuid_fix (
                      old_uuid varchar(36) PRIMARY KEY,
                      new_uuid varchar(36) NOT NULL,
                      source_institution_id bigint NOT NULL,
                      institution_name varchar(256) NOT NULL
                    )
                    """
                )
            )

            dest_conn.execute(
                text(
                    """
                    INSERT INTO tmp_institution_uuid_fix
                      (
                        old_uuid,
                        new_uuid,
                        source_institution_id,
                        institution_name
                      )
                    VALUES
                      (
                        :old_uuid,
                        :new_uuid,
                        :source_institution_id,
                        :institution_name
                      )
                    """
                ),
                mapping
            )
            dest_conn.commit()

            logger.info(
                f"[TEMP] Loaded {len(mapping)} mapping rows into "
                "temporary table"
            )

            for target_index, (table, column) in enumerate(
                targets,
                start=1
            ):

                table_started_at = time.monotonic()
                target_name = f"{data_database}.{table}.{column}"

                logger.info(
                    f"[TARGET {target_index}/{len(targets)}] "
                    f"Starting {target_name}"
                )

                try:

                    if ensure_indexes:

                        self._ensure_column_index(
                            dest_conn,
                            data_database,
                            table,
                            column
                        )

                    if table.lower() in skip_count_tables:

                        before_count = None

                        logger.info(
                            f"[COUNT] Skipping before count for "
                            f"{target_name}"
                        )

                    else:

                        logger.info(
                            f"[COUNT] Counting rows needing fix "
                            f"before update for {target_name}"
                        )

                        before_count = self._count_rows_to_update(
                            dest_conn,
                            data_database,
                            table,
                            column
                        )

                        logger.info(
                            f"[COUNT] Before update for {target_name}: "
                            f"rows_to_fix={before_count}"
                        )

                    updated = self._update_table_in_batches(
                        dest_conn,
                        data_database,
                        table,
                        column,
                        before_count,
                        batch_size,
                        mapping,
                        direct_update_threshold
                    )
                    updated_total += updated

                    if table.lower() in skip_count_tables:

                        after_count = "skipped"

                        logger.info(
                            f"[COUNT] Skipping after count for "
                            f"{target_name}"
                        )

                    else:

                        logger.info(
                            f"[COUNT] Counting rows needing fix "
                            f"after update for {target_name}"
                        )

                        after_count = self._count_rows_to_update(
                            dest_conn,
                            data_database,
                            table,
                            column
                        )

                        logger.info(
                            f"[COUNT] After update for {target_name}: "
                            f"remaining_rows_to_fix={after_count}"
                        )

                    logger.info(
                        f"[TARGET {target_index}/{len(targets)}] "
                        f"Finished {target_name}: "
                        f"before={before_count}, "
                        f"updated={updated}, "
                        f"remaining_after={after_count}, "
                        f"elapsed={self._format_elapsed(table_started_at)}"
                    )

                    update_summary.append(
                        {
                            "table": table,
                            "column": column,
                            "before": before_count,
                            "updated": updated,
                            "remaining_after": after_count,
                            "elapsed": self._format_elapsed(table_started_at),
                            "status": "done",
                        }
                    )

                except OperationalError as exc:

                    dest_conn.rollback()

                    if not skip_locked_targets:

                        raise

                    logger.warning(
                        f"[SKIP] Skipping locked/busy target {target_name}: "
                        f"{exc}"
                    )

                    update_summary.append(
                        {
                            "table": table,
                            "column": column,
                            "before": "skipped",
                            "updated": 0,
                            "remaining_after": "unknown",
                            "elapsed": self._format_elapsed(table_started_at),
                            "status": "skipped_locked",
                        }
                    )

        self._log_update_summary(
            data_database,
            update_summary
        )

        return updated_total

    def _set_lock_timeouts(
        self,
        dest_conn,
        lock_wait_timeout
    ):

        timeout = int(lock_wait_timeout)

        logger.info(
            f"[LOCK] Setting lock wait timeout to {timeout}s"
        )

        dest_conn.execute(
            text(
                f"SET SESSION lock_wait_timeout = {timeout}"
            )
        )
        dest_conn.execute(
            text(
                f"SET SESSION innodb_lock_wait_timeout = {timeout}"
            )
        )
        dest_conn.commit()

    def _ensure_column_index(
        self,
        dest_conn,
        data_database,
        table,
        column
    ):

        existing_index = dest_conn.execute(
            text(
                """
                SELECT s.INDEX_NAME
                FROM information_schema.STATISTICS s
                WHERE s.TABLE_SCHEMA = :data_database
                  AND s.TABLE_NAME = :table
                  AND s.COLUMN_NAME = :column
                  AND s.SEQ_IN_INDEX = 1
                LIMIT 1
                """
            ),
            {
                "data_database": data_database,
                "table": table,
                "column": column,
            }
        ).scalar()

        if existing_index:

            logger.info(
                f"[INDEX] Existing index for "
                f"{data_database}.{table}.{column}: "
                f"{existing_index}"
            )

            return

        index_name = self._index_name(
            table,
            column
        )
        index_started_at = time.monotonic()

        logger.info(
            f"[INDEX] Creating {index_name} on "
            f"{data_database}.{table}.{column}"
        )

        dest_conn.execute(
            text(
                f"""
                CREATE INDEX `{index_name}`
                ON `{data_database}`.`{table}` (`{column}`)
                """
            )
        )
        dest_conn.commit()

        logger.info(
            f"[INDEX] Created {index_name} on "
            f"{data_database}.{table}.{column}: "
            f"elapsed={self._format_elapsed(index_started_at)}"
        )

    def _index_name(
        self,
        table,
        column
    ):

        base_name = f"idx_inst_uuid_fix_{table}_{column}"

        if len(base_name) <= 64:

            return base_name

        checksum = format(
            zlib.crc32(base_name.encode("utf-8")) & 0xffffffff,
            "08x"
        )

        return f"idx_inst_uuid_fix_{checksum}_{column}"[:64]

    def _update_table_in_batches(
        self,
        dest_conn,
        data_database,
        table,
        column,
        before_count,
        batch_size,
        mapping,
        direct_update_threshold
    ):

        updated_total = 0

        if before_count == 0:

            logger.info(
                f"[UPDATE] No rows to update in "
                f"{data_database}.{table}.{column}"
            )

            return 0

        if (
            before_count is not None
            and before_count <= direct_update_threshold
        ):

            return self._update_table_directly(
                dest_conn,
                data_database,
                table,
                column,
                before_count
            )

        update_started_at = time.monotonic()

        logger.info(
            f"[UPDATE] Starting mapped UUID updates for "
            f"{data_database}.{table}.{column}: "
            f"mapping_rows={len(mapping)}, "
            f"rows_to_fix={before_count if before_count is not None else 'skipped'}, "
            f"commit_interval={batch_size}"
        )

        for mapping_index, row in enumerate(
            mapping,
            start=1
        ):

            mapping_updated = 0
            mapping_batch = 0

            while True:

                mapping_batch += 1

                result = dest_conn.execute(
                    text(
                        f"""
                        UPDATE `{data_database}`.`{table}`
                        SET `{column}` = :new_uuid
                        WHERE `{column}` = :old_uuid
                        LIMIT {int(batch_size)}
                        """
                    ),
                    {
                        "old_uuid": row["old_uuid"],
                        "new_uuid": row["new_uuid"],
                    }
                )

                batch_updated = result.rowcount or 0

                if not batch_updated:

                    break

                dest_conn.commit()
                mapping_updated += batch_updated
                updated_total += batch_updated

                remaining_estimate = (
                    max(
                        before_count - updated_total,
                        0
                    )
                    if before_count is not None
                    else "unknown"
                )
                percent_complete = (
                    updated_total / before_count * 100
                    if before_count
                    else None
                )

                percent_text = (
                    f"{percent_complete:.2f}"
                    if percent_complete is not None
                    else "unknown"
                )

                logger.info(
                    f"[UPDATE] Progress {data_database}.{table}.{column}: "
                    f"mapping={mapping_index}/{len(mapping)}, "
                    f"mapping_batch={mapping_batch}, "
                    f"updated_this_batch={batch_updated}, "
                    f"updated_this_mapping={mapping_updated}, "
                    f"updated_total={updated_total}/{before_count}, "
                    f"remaining_estimate={remaining_estimate}, "
                    f"percent={percent_text}, "
                    f"elapsed={self._format_elapsed(update_started_at)}"
                )

                if batch_updated < int(batch_size):

                    break

            if not mapping_updated and mapping_index % 100 == 0:

                dest_conn.commit()

                logger.info(
                    f"[UPDATE] Checked {mapping_index}/{len(mapping)} "
                    f"mapping rows for {data_database}.{table}.{column}: "
                    f"updated_total={updated_total}/{before_count}, "
                    f"elapsed={self._format_elapsed(update_started_at)}"
                )

        dest_conn.commit()

        logger.info(
            f"[UPDATE] Completed "
            f"{data_database}.{table}.{column}"
            f": updated_total={updated_total}, "
            f"elapsed={self._format_elapsed(update_started_at)}"
        )

        return updated_total

    def _update_table_directly(
        self,
        dest_conn,
        data_database,
        table,
        column,
        before_count
    ):

        update_started_at = time.monotonic()

        logger.info(
            f"[UPDATE] Starting direct join update for "
            f"{data_database}.{table}.{column}: "
            f"rows_to_fix={before_count}"
        )

        result = dest_conn.execute(
            text(
                f"""
                UPDATE `{data_database}`.`{table}` t
                JOIN tmp_institution_uuid_fix m
                  ON t.`{column}` = m.old_uuid
                SET t.`{column}` = m.new_uuid
                """
            )
        )

        dest_conn.commit()

        updated = result.rowcount or 0

        logger.info(
            f"[UPDATE] Completed direct join update for "
            f"{data_database}.{table}.{column}: "
            f"updated_total={updated}/{before_count}, "
            f"elapsed={self._format_elapsed(update_started_at)}"
        )

        return updated

    def _log_update_summary(
        self,
        data_database,
        update_summary
    ):

        logger.info(
            "[SUMMARY] Institution UUID fix outcome summary"
        )

        logger.info(
            "[SUMMARY] database | table | column | before | updated | "
            "remaining_after | status | elapsed"
        )

        for row in update_summary:

            logger.info(
                f"[SUMMARY] {data_database} | "
                f"{row['table']} | "
                f"{row['column']} | "
                f"{row['before']} | "
                f"{row['updated']} | "
                f"{row['remaining_after']} | "
                f"{row.get('status', 'done')} | "
                f"{row['elapsed']}"
            )

    def _count_rows_to_update(
        self,
        dest_conn,
        data_database,
        table,
        column
    ):

        result = dest_conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS row_count
                FROM `{data_database}`.`{table}` t
                JOIN tmp_institution_uuid_fix m
                  ON t.`{column}` = m.old_uuid
                """
            )
        )

        return int(
            result.scalar() or 0
        )

    def _format_elapsed(self, started_at):

        elapsed = time.monotonic() - started_at

        if elapsed < 60:

            return f"{elapsed:.1f}s"

        minutes = int(elapsed // 60)
        seconds = elapsed % 60

        return f"{minutes}m {seconds:.1f}s"

    def _norm(self, value):

        return " ".join(
            str(value or "")
            .strip()
            .lower()
            .split()
        )

    def _validate_identifier(self, value):

        if not self.IDENTIFIER_PATTERN.match(
            str(value or "")
        ):

            raise ValueError(
                f"Unsafe SQL identifier: {value}"
            )
