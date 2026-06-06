import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    inspect,
    select,
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CredentialsSharedMigrator(BaseMigrator):

    SHARE_TABLE = "share"
    DESTINATION_TABLE = "credentials_shared"
    HISTORY_DESTINATION_TABLE = "student_credentials_share_history"
    LEGACY_HISTORY_DESTINATION_TABLE = "students_credentials_share_history"
    DYNAMIC_VALUE = "DYNAMIC"
    S3_BASE_URL = "https://greenlightlocker-com.s3.us-west-2.amazonaws.com"
    DEFAULT_BATCH_SIZE = 10000
    MAX_BATCH_SIZE = 10000
    SHARE_PATHS = {
        "badge_shared": {
            "id_source": "credential",
            "template": "badges/{id}/pdf_badge",
        },
        "certificate_share": {
            "id_source": "credential",
            "template": "certificate/{id}/certificate_data",
        },
        "other_credential_share": {
            "id_source": "credential",
            "template": "other_credential/{id}/credential_data",
        },
        "recommendation_letter_share": {
            "id_source": "credential",
            "template": "recommendationletter/{id}/file",
        },
        "resume_share": {
            "id_source": "credential",
            "template": "resume/{id}/resume_data",
        },
        "transcript_shared": {
            "id_source": "share_link",
            "template": "shared/{id}/pdf_transcript",
        },
        "hs_transcript_shared": {
            "id_source": "share_link",
            "template": "highschoolshare/{id}/pdf_transcript",
        },
        "cc_transcript_shared": {
            "id_source": "share_link",
            "template": "communitycollegeshare/{id}/pdf_transcript",
        },
        "4yr_transcript_shared": {
            "id_source": "share_link",
            "template": "fouryear-share/{id}/pdf_transcript",
        },
    }

    SHARE_SOURCES = [
        {
            "share_table": "badge_shared",
            "credential_table": "badge",
            "credential_type": 3,
            "credential_id_columns": ["badge_id"],
            "credentials_all_link_column": "digital_badges",
            "uuid_prefix": "badge",
            "owner_columns": ["user_id", "issuer_id"],
        },
        {
            "share_table": "certificate_share",
            "credential_table": "certificate",
            "credential_type": 1,
            "credential_id_columns": ["certificate_id"],
            "credentials_all_link_column": "certifications",
            "uuid_prefix": "certificate",
            "owner_columns": ["user_id"],
        },
        {
            "share_table": "other_credential_share",
            "credential_table": "other_credentials",
            "credential_type": 5,
            "credential_id_columns": ["other_credential_id"],
            "credentials_all_link_column": "self_uploads",
            "uuid_prefix": "other_credentials",
            "owner_columns": ["user_id"],
        },
        {
            "share_table": "recommendation_letter_share",
            "credential_table": "recommendation_letter",
            "credential_type": 4,
            "credential_id_columns": ["recommendation_letter_id"],
            "credentials_all_link_column": "recommendation_letters",
            "uuid_prefix": "recommendation-letter",
            "owner_columns": ["user_id"],
            "target_id_column": "reference_id",
        },
        {
            "share_table": "resume_share",
            "credential_table": "resume",
            "credential_type": 6,
            "credential_id_columns": ["resume_id"],
            "credentials_all_link_column": "resume",
            "uuid_prefix": "resume",
            "owner_columns": ["user_id"],
        },
        {
            "share_table": "transcript_shared",
            "credential_table": "transcript",
            "credential_type": 2,
            "credential_id_columns": ["transcript_id"],
            "credentials_all_link_column": "transcripts",
            "uuid_prefix": "transcript",
            "owner_columns": ["user_id"],
        },
        {
            "share_table": "hs_transcript_shared",
            "credential_table": "hs_transcript",
            "credential_type": 2,
            "credential_id_columns": ["transcript_id"],
            "credentials_all_link_column": "transcripts",
            "uuid_prefix": "transcript",
            "owner_columns": ["user_id"],
            "target_id_column": "credential_id",
        },
        {
            "share_table": "cc_transcript_shared",
            "credential_table": "cc_transcript",
            "credential_type": 2,
            "credential_id_columns": ["transcript_id"],
            "credentials_all_link_column": "transcripts",
            "uuid_prefix": "transcript",
            "owner_columns": ["user_id"],
            "target_id_column": "credential_id",
        },
        {
            "share_table": "4yr_transcript_shared",
            "credential_table": "4yr_transcript",
            "credential_type": 2,
            "credential_id_columns": ["transcript_id"],
            "credentials_all_link_column": "transcripts",
            "uuid_prefix": "transcript",
            "owner_columns": ["user_id"],
            "target_id_column": "credential_id",
        },
        {
            "share_table": "self_uploaded_transcript_share",
            "credential_table": "transcript",
            "credential_type": 2,
            "credential_id_columns": ["transcript_id"],
            "credentials_all_link_column": "transcripts",
            "uuid_prefix": "transcript",
            "owner_columns": ["user_id"],
        },
    ]

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

        logger.info(
            "Starting Credentials Shared Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        source_table_names = set(
            inspect(self.source_engine).get_table_names()
        )
        dest_table_names = set(
            inspect(self.dest_engine).get_table_names()
        )

        if self.SHARE_TABLE not in source_table_names:

            raise ValueError(
                "source share table not found"
            )

        if self.DESTINATION_TABLE not in dest_table_names:

            raise ValueError(
                "credentials_shared destination table not found"
            )

        history_table_name = self._resolve_history_table_name(
            dest_table_names
        )

        share_table = self._manual_reflect(
            self.SHARE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        credentials_shared_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        history_table = None

        if history_table_name:

            history_table = self._manual_reflect(
                history_table_name,
                self.dest_engine,
                self.metadata_dest
            )

        else:

            logger.warning(
                "student credentials share history destination table "
                "not found. Skipping history insert."
            )

        credentials_all_table = self._manual_reflect(
            "credentials_all",
            self.dest_engine,
            self.metadata_dest
        )

        source_user_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )
        source_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )
        source_institution_table = self._manual_reflect(
            "institution",
            self.source_engine,
            self.metadata_source
        )

        auth_users_table = self._manual_reflect(
            "users",
            auth_db_engine,
            self.metadata_dest
        )
        auth_institutions_table = self._manual_reflect(
            "institutions",
            auth_db_engine,
            self.metadata_dest
        )

        logger.info(
            f"credentials_shared columns: "
            f"{credentials_shared_table.columns.keys()}"
        )
        if history_table is not None:

            logger.info(
                f"{history_table.name} columns: "
                f"{history_table.columns.keys()}"
            )

        destination_user_lookup = self._build_destination_user_lookup(
            auth_users_table,
            auth_db_engine
        )
        destination_email_lookup = self._build_destination_email_lookup(
            auth_users_table,
            auth_db_engine
        )
        destination_institution_by_name = (
            self._build_destination_institution_lookup(
                auth_institutions_table,
                auth_db_engine
            )
        )

        existing_shared_uuids = self._load_existing_uuids(
            credentials_shared_table
        )
        existing_history_uuids = (
            self._load_existing_uuids(
                history_table
            )
            if history_table is not None
            else
            set()
        )

        batch_size = self._get_batch_size()
        total_inserted = 0
        total_history_inserted = 0
        total_fetched = 0
        total_prepared = 0
        skipped_existing = 0
        skipped_missing_credential = 0
        skipped_missing_credentials_all = 0
        row_error_count = 0
        dynamic_blockchain_hash_count = 0

        remaining_limit = self.config.get("limit")

        if remaining_limit:

            remaining_limit = int(
                remaining_limit
            )

        for source_config in self.SHARE_SOURCES:

            share_table_name = source_config[
                "share_table"
            ]

            if share_table_name not in source_table_names:

                logger.warning(
                    f"Skipping missing share source table: "
                    f"{share_table_name}"
                )

                continue

            credential_table_name = source_config[
                "credential_table"
            ]

            if credential_table_name not in source_table_names:

                logger.warning(
                    f"Skipping {share_table_name}: missing credential "
                    f"table {credential_table_name}"
                )

                continue

            source_share_table = self._manual_reflect(
                share_table_name,
                self.source_engine,
                self.metadata_source
            )
            credential_table = self._manual_reflect(
                credential_table_name,
                self.source_engine,
                self.metadata_source
            )

            logger.info(
                "Starting share source "
                f"{share_table_name} -> credentials_shared"
            )

            last_source_id = 0

            while True:

                fetch_size = batch_size

                if remaining_limit is not None:

                    if remaining_limit <= 0:

                        break

                    fetch_size = min(
                        fetch_size,
                        remaining_limit
                    )

                with self.source_engine.connect() as source_conn:

                    rows = source_conn.execute(
                        select(
                            source_share_table
                        )
                        .where(
                            source_share_table.c.id > last_source_id
                        )
                        .order_by(
                            source_share_table.c.id
                        )
                        .limit(
                            fetch_size
                        )
                    ).fetchall()

                if not rows:

                    break

                total_fetched += len(
                    rows
                )

                if remaining_limit is not None:

                    remaining_limit -= len(
                        rows
                    )

                chunk_context = self._build_chunk_context(
                    rows,
                    source_share_table,
                    share_table,
                    credential_table,
                    source_user_table,
                    source_student_table,
                    source_institution_table,
                    credentials_all_table,
                    source_config,
                    destination_user_lookup,
                    destination_email_lookup,
                    destination_institution_by_name
                )

                shared_insert_data = []
                history_insert_data = []

                for row in rows:

                    row_dict = row._mapping
                    source_share_link_id = self._get_source_value(
                        row_dict,
                        source_share_table,
                        "id"
                    )
                    last_source_id = source_share_link_id

                    try:

                        shared_uuid = self._stable_uuid(
                            "credentials-shared",
                            share_table_name,
                            source_share_link_id
                        )
                        history_uuid = self._stable_uuid(
                            "credentials-shared-history",
                            share_table_name,
                            source_share_link_id
                        )

                        if shared_uuid in existing_shared_uuids:

                            skipped_existing += 1

                            continue

                        share_id = self._get_source_value(
                            row_dict,
                            source_share_table,
                            "share_id"
                        )
                        share_row = chunk_context[
                            "share_rows"
                        ].get(
                            share_id
                        )

                        credential_source_id = self._get_source_value(
                            row_dict,
                            source_share_table,
                            *source_config[
                                "credential_id_columns"
                            ]
                        )
                        credential_row = chunk_context[
                            "credential_rows"
                        ].get(
                            credential_source_id
                        )

                        if not credential_row:

                            skipped_missing_credential += 1

                            continue

                        destination_credential_uuid = (
                            self._destination_credential_uuid(
                                source_config,
                                credential_row,
                                credential_source_id
                            )
                        )
                        credentials_all_row = chunk_context[
                            "credentials_all_rows"
                        ].get(
                            destination_credential_uuid
                        )

                        if not credentials_all_row:

                            skipped_missing_credentials_all += 1

                            continue

                        shared_by_source_user_id = (
                            self._get_share_value(
                                share_row,
                                share_table,
                                "user_id"
                            )
                            or
                            self._get_credential_owner_user_id(
                                credential_row,
                                source_config
                            )
                        )
                        shared_by_user_uuid = (
                            chunk_context[
                                "source_user_to_destination_uuid"
                            ].get(
                                shared_by_source_user_id
                            )
                        )

                        recipient_email = self._get_share_value(
                            share_row,
                            share_table,
                            "recipient_email_address"
                        )

                        recipient_institution_id = self._get_share_value(
                            share_row,
                            share_table,
                            "recipient_institution_id"
                        )

                        if recipient_institution_id:

                            shared_type = "INSTITUTE"
                            shared_with_email = None
                            shared_with_institution_id = (
                                chunk_context[
                                    "source_institution_to_destination_uuid"
                                ].get(
                                    recipient_institution_id
                                )
                            )
                            institute_name = chunk_context[
                                "source_institution_names"
                            ].get(
                                recipient_institution_id
                            )

                        else:

                            shared_type = "EMAIL"
                            shared_with_email = recipient_email
                            shared_with_institution_id = None
                            institute_name = None

                        student_id = self._resolve_student_id(
                            credentials_all_row,
                            credential_row,
                            source_config,
                            chunk_context
                        )

                        credential_path = self._share_credential_path(
                            source_config,
                            source_share_link_id,
                            credential_source_id,
                            credentials_all_row
                        )
                        share_date = (
                            self._get_share_value(
                                share_row,
                                share_table,
                                "share_date"
                            )
                            or
                            datetime.utcnow()
                        )
                        message = (
                            self._get_share_value(
                                share_row,
                                share_table,
                                "message"
                            )
                            or
                            ""
                        )
                        include_sat_act = self._map_bool(
                            self._get_share_value(
                                share_row,
                                share_table,
                                "toc"
                            )
                        )
                        shared_status = (
                            self._get_share_value(
                                share_row,
                                share_table,
                                "status"
                            )
                            or
                            self._get_share_value(
                                share_row,
                                share_table,
                                "active"
                            )
                        )
                        blockchain_hash = self._get_blockchain_hash(
                            row_dict,
                            source_share_table,
                            credential_row
                        )

                        if not blockchain_hash:

                            blockchain_hash = self.DYNAMIC_VALUE
                            dynamic_blockchain_hash_count += 1

                        edi_sst_version = self._get_source_value(
                            row_dict,
                            source_share_table,
                            "edi_sst_version",
                            "sst_version"
                        )
                        attention_of = self._get_share_value(
                            share_row,
                            share_table,
                            "recipient_name"
                        )
                        reference_id = self._get_share_value(
                            share_row,
                            share_table,
                            "reference_id"
                        )
                        shared_row = {
                            "uuid": shared_uuid,
                            "created_at": share_date,
                            "updated_at": share_date,
                            "deleted_at": None,
                            "credential_all_uuid": (
                                credentials_all_row.get("uuid")
                            ),
                            "shared_with_email": shared_with_email,
                            "shared_with_institution_id": (
                                shared_with_institution_id
                            ),
                            "institute_name": institute_name,
                            "message": message,
                            "include_sat_act": include_sat_act,
                            "shared_on": str(share_date),
                            "user_id": shared_by_user_uuid,
                            "student_id": (
                                str(student_id)
                                if student_id is not None
                                else
                                None
                            ),
                            "transcript_pdf_path": credential_path,
                            "edi_file_path": None,
                            "shared_status": (
                                str(shared_status)
                                if shared_status is not None
                                else
                                None
                            ),
                            "edi_sst_version": edi_sst_version,
                            "edi_acknowledgement": None,
                            "edi_acknowledgement_status": None,
                            "acknowledged_by": None,
                            "acknowledgement_status": None,
                            "acknowledgement_time": None,
                            "student_acknowledgement_informed_by": None,
                            "blockchain_hash": blockchain_hash,
                        }

                        shared_insert_data.append(
                            self._filter_to_table_columns(
                                shared_row,
                                credentials_shared_table
                            )
                        )
                        existing_shared_uuids.add(
                            shared_uuid
                        )

                        if history_table is not None:

                            history_row = {
                                "uuid": history_uuid,
                                "created_at": share_date,
                                "updated_at": share_date,
                                "deleted_at": None,
                                "student_number": (
                                    str(student_id)
                                    if student_id is not None
                                    else
                                    ""
                                ),
                                "user_id": shared_by_user_uuid,
                                "reference_id": reference_id,
                                "include_sat_act": include_sat_act,
                                "shared_mail": shared_with_email,
                                "shared_type": shared_type,
                                "message": message,
                                "credential_path": credential_path,
                                "institute_name": institute_name,
                                "attention_of": attention_of,
                                "credentials_all_uuid": (
                                    credentials_all_row.get("uuid")
                                ),
                                "shared_on": str(share_date),
                                "blockchain_hash": blockchain_hash,
                            }

                            if history_uuid not in existing_history_uuids:

                                history_insert_data.append(
                                    self._filter_to_table_columns(
                                        history_row,
                                        history_table
                                    )
                                )
                                existing_history_uuids.add(
                                    history_uuid
                                )

                    except Exception as error:

                        row_error_count += 1

                        logger.exception(
                            "Failed processing "
                            f"{share_table_name} source id "
                            f"{source_share_link_id}: {error}"
                        )

                if not shared_insert_data:

                    logger.info(
                        f"{share_table_name} chunk through id "
                        f"{last_source_id}: nothing to insert."
                    )

                    continue

                logger.info(
                    "Inserting shared credential chunk: "
                    f"source={share_table_name}, "
                    f"prepared={len(shared_insert_data)}, "
                    f"history_prepared={len(history_insert_data)}, "
                    f"source_id_through={last_source_id}, "
                    f"total_fetched={total_fetched}"
                )

                with self.dest_engine.begin() as dest_conn:

                    shared_result = dest_conn.execute(
                        insert(credentials_shared_table),
                        shared_insert_data
                    )

                    history_result = None

                    if history_table is not None and history_insert_data:

                        history_result = dest_conn.execute(
                            insert(history_table),
                            history_insert_data
                        )

                inserted_now = shared_result.rowcount or len(
                    shared_insert_data
                )
                history_inserted_now = (
                    history_result.rowcount
                    if history_result is not None
                    else
                    0
                ) or len(
                    history_insert_data
                )

                total_inserted += inserted_now
                total_history_inserted += history_inserted_now
                total_prepared += len(
                    shared_insert_data
                )

                logger.info(
                    "Shared credential chunk inserted: "
                    f"source={share_table_name}, "
                    f"credentials_shared={inserted_now}, "
                    f"history={history_inserted_now}, "
                    f"inserted_total={total_inserted}"
                )

            if remaining_limit is not None and remaining_limit <= 0:

                break

        logger.info(
            "Credentials Shared Migration summary: "
            f"inserted={total_inserted}, "
            f"history_inserted={total_history_inserted}, "
            f"prepared={total_prepared}, "
            f"fetched={total_fetched}, "
            f"skipped_existing={skipped_existing}, "
            f"skipped_missing_credential={skipped_missing_credential}, "
            f"skipped_missing_credentials_all="
            f"{skipped_missing_credentials_all}, "
            f"dynamic_blockchain_hash={dynamic_blockchain_hash_count}, "
            f"row_errors={row_error_count}"
        )

        return total_inserted

    def _resolve_history_table_name(
        self,
        dest_table_names
    ):

        if self.HISTORY_DESTINATION_TABLE in dest_table_names:

            return self.HISTORY_DESTINATION_TABLE

        if self.LEGACY_HISTORY_DESTINATION_TABLE in dest_table_names:

            return self.LEGACY_HISTORY_DESTINATION_TABLE

        return None

    def _get_batch_size(self):

        batch_size = int(
            self.config.get(
                "credentials_shared_migration_batch_size",
                self.config.get(
                    "batch_size",
                    self.DEFAULT_BATCH_SIZE
                )
            )
        )

        if batch_size < 1:

            batch_size = self.DEFAULT_BATCH_SIZE

        return min(
            batch_size,
            self.MAX_BATCH_SIZE
        )

    def _build_chunk_context(
        self,
        rows,
        source_share_table,
        share_table,
        credential_table,
        source_user_table,
        source_student_table,
        source_institution_table,
        credentials_all_table,
        source_config,
        destination_user_lookup,
        destination_email_lookup,
        destination_institution_by_name
    ):

        share_ids = set()
        credential_ids = set()

        for row in rows:

            row_dict = row._mapping
            share_id = self._get_source_value(
                row_dict,
                source_share_table,
                "share_id"
            )
            credential_id = self._get_source_value(
                row_dict,
                source_share_table,
                *source_config["credential_id_columns"]
            )

            if share_id:

                share_ids.add(
                    share_id
                )

            if credential_id:

                credential_ids.add(
                    credential_id
                )

        share_rows = self._fetch_lookup_by_ids(
            self.source_engine,
            share_table,
            share_table.c.id,
            share_ids
        )
        credential_rows = self._fetch_lookup_by_ids(
            self.source_engine,
            credential_table,
            credential_table.c.id,
            credential_ids
        )

        source_user_ids = set()
        source_institution_ids = set()
        destination_credential_uuids = set()

        for share_row in share_rows.values():

            share_user_id = share_row.get("user_id")
            recipient_institution_id = share_row.get(
                "recipient_institution_id"
            )

            if share_user_id:

                source_user_ids.add(
                    share_user_id
                )

            if recipient_institution_id:

                source_institution_ids.add(
                    recipient_institution_id
                )

        for source_credential_id, credential_row in credential_rows.items():

            owner_user_id = self._get_credential_owner_user_id(
                credential_row,
                source_config
            )

            if owner_user_id:

                source_user_ids.add(
                    owner_user_id
                )

            destination_credential_uuid = (
                self._destination_credential_uuid(
                    source_config,
                    credential_row,
                    source_credential_id
                )
            )

            if destination_credential_uuid:

                destination_credential_uuids.add(
                    destination_credential_uuid
                )

        source_users = self._fetch_lookup_by_ids(
            self.source_engine,
            source_user_table,
            source_user_table.c.id,
            source_user_ids
        )

        source_user_to_destination_uuid = {}

        for source_user_id, source_user in source_users.items():

            destination_uuid = destination_user_lookup.get(
                self._normalize(
                    source_user.get("username")
                )
            )

            if destination_uuid:

                source_user_to_destination_uuid[
                    source_user_id
                ] = destination_uuid

        students_by_user_id = self._fetch_students_by_user_ids(
            source_student_table,
            source_user_ids
        )

        source_institutions = self._fetch_lookup_by_ids(
            self.source_engine,
            source_institution_table,
            source_institution_table.c.id,
            source_institution_ids
        )
        source_institution_names = {}
        source_institution_to_destination_uuid = {}

        for source_institution_id, source_institution in (
            source_institutions.items()
        ):

            institution_name = source_institution.get("name")

            if not institution_name:

                continue

            source_institution_names[
                source_institution_id
            ] = institution_name
            destination_uuid = destination_institution_by_name.get(
                self._normalize(
                    institution_name
                )
            )

            if destination_uuid:

                source_institution_to_destination_uuid[
                    source_institution_id
                ] = destination_uuid

        credentials_all_rows = self._fetch_credentials_all_rows(
            credentials_all_table,
            source_config,
            destination_credential_uuids
        )

        for credential_row in credentials_all_rows.values():

            user_uuid = credential_row.get("user_id")
            user_email = destination_email_lookup.get(
                user_uuid
            )

            if user_email and not credential_row.get("student_email"):

                credential_row["student_email"] = user_email

        return {
            "share_rows": share_rows,
            "credential_rows": credential_rows,
            "source_user_to_destination_uuid": (
                source_user_to_destination_uuid
            ),
            "students_by_user_id": students_by_user_id,
            "source_institution_names": source_institution_names,
            "source_institution_to_destination_uuid": (
                source_institution_to_destination_uuid
            ),
            "credentials_all_rows": credentials_all_rows,
        }

    def _fetch_lookup_by_ids(
        self,
        engine,
        table,
        id_column,
        ids
    ):

        if not ids:

            return {}

        with engine.connect() as conn:

            rows = conn.execute(
                select(
                    table
                ).where(
                    id_column.in_(
                        list(ids)
                    )
                )
            ).fetchall()

        return {
            row._mapping.get(id_column): dict(row._mapping)
            for row in rows
        }

    def _fetch_students_by_user_ids(
        self,
        source_student_table,
        source_user_ids
    ):

        if not source_user_ids or "user_id" not in source_student_table.c:

            return {}

        with self.source_engine.connect() as conn:

            rows = conn.execute(
                select(
                    source_student_table
                ).where(
                    source_student_table.c.user_id.in_(
                        list(source_user_ids)
                    )
                )
            ).fetchall()

        students = {}

        for row in rows:

            row_dict = dict(
                row._mapping
            )
            user_id = row_dict.get(
                "user_id"
            )

            if user_id not in students:

                students[
                    user_id
                ] = row_dict

        return students

    def _fetch_credentials_all_rows(
        self,
        credentials_all_table,
        source_config,
        destination_credential_uuids
    ):

        link_column_name = source_config[
            "credentials_all_link_column"
        ]

        if (
            not destination_credential_uuids
            or
            link_column_name not in credentials_all_table.c
        ):

            return {}

        selected_columns = [
            credentials_all_table.c.uuid,
            credentials_all_table.c[link_column_name],
        ]

        for column_name in [
            "credential_path",
            "user_id",
            "student_id",
            "student_number",
            "student_email",
            "institution_id",
            "institution_name",
        ]:

            if column_name in credentials_all_table.c:

                selected_columns.append(
                    credentials_all_table.c[column_name]
                )

        uuid_values = list(
            destination_credential_uuids
        )

        with self.dest_engine.connect() as conn:

            rows = []

            for index in range(
                0,
                len(uuid_values),
                500
            ):

                uuid_chunk = uuid_values[
                    index:index + 500
                ]

                rows.extend(
                    conn.execute(
                        select(
                            *selected_columns
                        ).where(
                            credentials_all_table.c.credential_type
                            == source_config["credential_type"]
                        ).where(
                            credentials_all_table.c[link_column_name].in_(
                                uuid_chunk
                            )
                        )
                    ).fetchall()
                )

        lookup = {}

        for row in rows:

            row_dict = dict(
                row._mapping
            )
            linked_uuid = row_dict.get(
                link_column_name
            )

            if linked_uuid and linked_uuid not in lookup:

                lookup[
                    linked_uuid
                ] = row_dict

        return lookup

    def _build_destination_user_lookup(
        self,
        users_table,
        auth_db_engine
    ):

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    users_table.c.uuid,
                    users_table.c.user_name,
                    users_table.c.email
                )
            ).fetchall()

        for row in rows:

            row_dict = row._mapping
            user_uuid = row_dict.get(
                users_table.c.uuid
            )

            for column in [
                users_table.c.user_name,
                users_table.c.email
            ]:

                value = row_dict.get(
                    column
                )

                if value:

                    lookup[
                        self._normalize(value)
                    ] = user_uuid

        logger.info(
            f"Built {len(lookup)} shared credential user lookups"
        )

        return lookup

    def _build_destination_email_lookup(
        self,
        users_table,
        auth_db_engine
    ):

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    users_table.c.uuid,
                    users_table.c.user_name,
                    users_table.c.email
                )
            ).fetchall()

        for row in rows:

            row_dict = row._mapping
            user_uuid = row_dict.get(
                users_table.c.uuid
            )

            if user_uuid:

                lookup[
                    user_uuid
                ] = (
                    row_dict.get(users_table.c.email)
                    or
                    row_dict.get(users_table.c.user_name)
                )

        return lookup

    def _build_destination_institution_lookup(
        self,
        institutions_table,
        auth_db_engine
    ):

        lookup = {}

        with auth_db_engine.connect() as conn:

            rows = conn.execute(
                select(
                    institutions_table.c.uuid,
                    institutions_table.c.name
                )
            ).fetchall()

        for row in rows:

            row_dict = row._mapping
            name = row_dict.get(
                institutions_table.c.name
            )
            institution_uuid = row_dict.get(
                institutions_table.c.uuid
            )

            if name and institution_uuid:

                lookup[
                    self._normalize(name)
                ] = institution_uuid

        return lookup

    def _load_existing_uuids(
        self,
        table
    ):

        if table is None:

            return set()

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    table.c.uuid
                )
            ).fetchall()

        return {
            row._mapping.get(table.c.uuid)
            for row in rows
            if row._mapping.get(table.c.uuid)
        }

    def _destination_credential_uuid(
        self,
        source_config,
        credential_row,
        source_credential_id
    ):

        target_id_column = source_config.get(
            "target_id_column"
        )

        if target_id_column:

            target_id = credential_row.get(
                target_id_column
            )

        else:

            target_id = source_credential_id

        if not target_id:

            return None

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"gll:{source_config['uuid_prefix']}:{target_id}"
            )
        )

    def _get_credential_owner_user_id(
        self,
        credential_row,
        source_config
    ):

        if not credential_row:

            return None

        for column_name in source_config.get(
            "owner_columns",
            []
        ):

            value = credential_row.get(
                column_name
            )

            if value:

                return value

        return None

    def _get_blockchain_hash(
        self,
        row,
        source_share_table,
        credential_row
    ):

        return (
            self._get_source_value(
                row,
                source_share_table,
                "blockchain_hash"
            )
            or
            (
                credential_row.get("blockchain_hash")
                if credential_row
                else
                None
            )
        )

    def _resolve_student_id(
        self,
        credentials_all_row,
        credential_row,
        source_config,
        chunk_context
    ):

        student_id = credentials_all_row.get(
            "student_id"
        )

        if student_id:

            return student_id

        owner_user_id = self._get_credential_owner_user_id(
            credential_row,
            source_config
        )

        source_student = chunk_context[
            "students_by_user_id"
        ].get(
            owner_user_id
        )

        if not source_student:

            return None

        return (
            source_student.get("school_student_id")
            or
            source_student.get("student_number")
            or
            source_student.get("id")
        )

    def _share_credential_path(
        self,
        source_config,
        source_share_link_id,
        credential_source_id,
        credentials_all_row
    ):

        path_config = self.SHARE_PATHS.get(
            source_config["share_table"]
        )

        if not path_config:

            return credentials_all_row.get(
                "credential_path"
            )

        path_id = (
            source_share_link_id
            if path_config["id_source"] == "share_link"
            else
            credential_source_id
        )

        if path_id is None:

            return credentials_all_row.get(
                "credential_path"
            )

        path = path_config["template"].format(
            id=path_id
        )

        return f"{self.S3_BASE_URL}/{path}"

    def _get_share_value(
        self,
        share_row,
        share_table,
        *column_names
    ):

        if not share_row:

            return None

        for column_name in column_names:

            if column_name in share_table.c:

                value = share_row.get(
                    column_name
                )

                if value is not None:

                    return value

        return None

    def _get_source_value(
        self,
        row,
        table,
        *column_names
    ):

        for column_name in column_names:

            if column_name in table.c:

                value = row.get(
                    table.c[column_name]
                )

                if value is not None:

                    return value

        return None

    def _filter_to_table_columns(
        self,
        row,
        table
    ):

        return {
            column_name: value
            for column_name, value in row.items()
            if column_name in table.c
        }

    def _map_bool(
        self,
        value
    ):

        if value is None:

            return None

        if isinstance(value, bool):

            return 1 if value else 0

        if isinstance(value, bytes):

            return 1 if value == b"\x01" else 0

        return 1 if str(value).strip().lower() in [
            "1",
            "true",
            "yes",
            "y",
            "\\x01",
        ] else 0

    def _stable_uuid(
        self,
        *parts
    ):

        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                ":".join(
                    str(part)
                    for part in parts
                    if part is not None
                )
            )
        )

    def _normalize(
        self,
        value
    ):

        if value is None:

            return ""

        return (
            str(value)
            .strip()
            .lower()
            .replace(" ", "")
        )
