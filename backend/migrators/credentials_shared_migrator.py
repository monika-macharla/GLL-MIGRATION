import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    inspect,
    select
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class CredentialsSharedMigrator(BaseMigrator):

    DESTINATION_TABLE = "credentials_shared"
    HISTORY_DESTINATION_TABLE = "student_credentials_share_history"

    SHARE_SOURCES = [
        {
            "share_table": "badge_shared",
            "credential_table": "badge",
            "credential_type": 3,
            "credentials_all_columns": ["digital_badges"],
            "credential_id_columns": [
                "badge_id",
                "credential_id",
                "digital_badge_id"
            ],
        },
        {
            "share_table": "certificate_shared",
            "credential_table": "certificate",
            "credential_type": 2,
            "credentials_all_columns": [
                "certificate",
                "certificates",
                "cerificate",
                "credentials_certifications"
            ],
            "credential_id_columns": [
                "certificate_id",
                "credential_id"
            ],
        },
        {
            "share_table": "other_credential_share",
            "credential_table": "other_credentials",
            "credential_type": 5,
            "credentials_all_columns": [
                "self_uploads",
                "self_upload"
            ],
            "credential_id_columns": [
                "other_credential_id",
                "credential_id",
                "self_upload_id"
            ],
        },
        {
            "share_table": "recommendation_letter_share",
            "credential_table": "recommendation_request",
            "credential_type": 4,
            "credentials_all_columns": [
                "recommendation_letters",
                "recommendation_letter"
            ],
            "credential_id_columns": [
                "recommendation_request_id",
                "recommendation_letter_id",
                "credential_id"
            ],
        },
        {
            "share_table": "self_uploaded_transcript_share",
            "credential_table": "transcript",
            "credential_type": 1,
            "credentials_all_columns": [
                "transcripts",
                "transcript"
            ],
            "credential_id_columns": [
                "transcript_id",
                "credential_id"
            ],
        },
        {
            "share_table": "transcript_shared",
            "credential_table": "transcript",
            "credential_type": 1,
            "credentials_all_columns": [
                "transcripts",
                "transcript"
            ],
            "credential_id_columns": [
                "transcript_id",
                "credential_id"
            ],
        },
        {
            "share_table": "resume_share",
            "credential_table": "resume",
            "credential_type": 6,
            "credentials_all_columns": ["resume"],
            "credential_id_columns": [
                "resume_id",
                "credential_id"
            ],
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

        if self.DESTINATION_TABLE not in dest_table_names:

            raise ValueError(
                "credentials_shared destination table not found"
            )

        credentials_shared_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        history_table = None

        if self.HISTORY_DESTINATION_TABLE in dest_table_names:

            history_table = self._manual_reflect(
                self.HISTORY_DESTINATION_TABLE,
                self.dest_engine,
                self.metadata_dest
            )

        else:

            logger.warning(
                "student_credentials_share_history "
                "destination table not found. "
                "Skipping history insert."
            )

        credentials_all_table = None

        if "credentials_all" in dest_table_names:

            credentials_all_table = self._manual_reflect(
                "credentials_all",
                self.dest_engine,
                self.metadata_dest
            )

        insert_data = []

        history_insert_data = []

        skipped_count = 0

        for source_config in self.SHARE_SOURCES:

            share_table_name = source_config["share_table"]

            if share_table_name not in source_table_names:

                logger.warning(
                    f"Skipping missing share table: "
                    f"{share_table_name}"
                )

                continue

            share_table = self._manual_reflect(
                share_table_name,
                self.source_engine,
                self.metadata_source
            )

            credential_table = None

            credential_table_name = source_config[
                "credential_table"
            ]

            if credential_table_name in source_table_names:

                credential_table = self._manual_reflect(
                    credential_table_name,
                    self.source_engine,
                    self.metadata_source
                )

            query = select(
                share_table
            )

            if self.config.get("limit"):

                query = query.limit(
                    self.config["limit"]
                )

            with self.source_engine.connect() as source_conn:

                rows = source_conn.execute(
                    query
                ).fetchall()

            logger.info(
                f"Found {len(rows)} rows in "
                f"{share_table_name}"
            )

            for index, row in enumerate(
                rows,
                start=1
            ):

                try:

                    row_dict = row._mapping

                    credential_source_id = (
                        self._get_source_value(
                            row_dict,
                            share_table,
                            *source_config[
                                "credential_id_columns"
                            ]
                        )
                    )

                    credential_row = (
                        self._get_credential_row(
                            credential_table,
                            credential_source_id
                        )
                    )

                    shared_by_user_uuid = (
                        self._get_destination_user_uuid(
                            auth_db_engine,
                            self._get_shared_by_user_id(
                                row_dict,
                                share_table,
                                credential_row
                            )
                        )
                    )

                    shared_with_user_uuid = (
                        self._get_destination_user_uuid(
                            auth_db_engine,
                            self._get_source_value(
                                row_dict,
                                share_table,
                                "shared_with_user_id",
                                "shared_to_user_id",
                                "recipient_user_id",
                                "to_user_id"
                            )
                        )
                    )

                    shared_email = self._get_source_value(
                        row_dict,
                        share_table,
                        "email",
                        "shared_email",
                        "shared_to_email",
                        "recipient_email",
                        "to_email"
                    )

                    credential_uuid = (
                        self._resolve_destination_credential_uuid(
                            credentials_all_table,
                            source_config,
                            credential_row,
                            shared_by_user_uuid
                        )
                    )

                    credentials_all_row = (
                        self._get_credentials_all_row(
                            credentials_all_table,
                            credential_uuid
                        )
                    )

                    if (
                        self._requires_credential_all_uuid(
                            credentials_shared_table,
                            history_table
                        )
                        and
                        not credential_uuid
                    ):

                        skipped_count += 1

                        logger.warning(
                            "Skipping "
                            f"{share_table_name} row {index}: "
                            "could not resolve credentials_all.uuid"
                        )

                        continue

                    shared_with_email = (
                        shared_email
                        or
                        self._get_destination_user_email(
                            auth_db_engine,
                            shared_with_user_uuid
                        )
                    )

                    shared_with_institution_id = (
                        self._get_shared_with_institution_id(
                            auth_db_engine,
                            shared_with_user_uuid,
                            credentials_all_row,
                            credential_row
                        )
                    )

                    institute_name = self._get_institute_name(
                        auth_db_engine,
                        shared_with_institution_id,
                        credentials_all_row,
                        credential_row
                    )

                    student_id = self._get_student_id(
                        credentials_all_row,
                        credential_row
                    )

                    message = (
                        self._get_source_value(
                            row_dict,
                            share_table,
                            "message",
                            "share_message",
                            "shared_message",
                            "personalized_message",
                            "description",
                            "note",
                            "notes"
                        )
                        or
                        ""
                    )

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            share_table,
                            "created_at",
                            "created_date",
                            "shared_at",
                            "shared_on",
                            "share_date",
                            "date_shared"
                        )
                        or
                        datetime.utcnow()
                    )

                    shared_row = {
                        "uuid": str(uuid.uuid4()),
                        "created_at": created_at,
                        "updated_at": created_at,
                        "deleted_at": None,
                        "user_id": shared_by_user_uuid,
                        "shared_by": shared_by_user_uuid,
                        "shared_by_user_id": shared_by_user_uuid,
                        "shared_with": shared_with_user_uuid,
                        "shared_with_user_id": shared_with_user_uuid,
                        "shared_to": shared_with_user_uuid,
                        "shared_to_user_id": shared_with_user_uuid,
                        "email": shared_email,
                        "shared_email": shared_email,
                        "shared_with_email": shared_with_email,
                        "shared_to_email": shared_email,
                        "recipient_email": shared_email,
                        "shared_with_institution_id": (
                            shared_with_institution_id
                        ),
                        "institution_id": shared_with_institution_id,
                        "institute_name": institute_name,
                        "institution_name": institute_name,
                        "student_id": (
                            str(student_id)
                            if student_id is not None
                            else None
                        ),
                        "student_number": (
                            str(student_id)
                            if student_id is not None
                            else ""
                        ),
                        "message": message,
                        "credential_type": source_config[
                            "credential_type"
                        ],
                        "credential_id": credential_uuid,
                        "credential_uuid": credential_uuid,
                        "credentials_all_id": credential_uuid,
                        "credential_all_uuid": credential_uuid,
                        "source_credential_id": (
                            str(credential_source_id)
                            if credential_source_id is not None
                            else None
                        ),
                        "source_table": share_table_name,
                        "status": self._map_status(
                            self._get_source_value(
                                row_dict,
                                share_table,
                                "status",
                                "active"
                            )
                        ),
                        "created_by": shared_by_user_uuid,
                        "updated_by": shared_by_user_uuid,
                        "deleted_by": None,
                    }

                    if not shared_row["user_id"]:

                        shared_row["user_id"] = (
                            shared_with_user_uuid
                        )

                    insert_data.append(
                        self._filter_to_table_columns(
                            shared_row,
                            credentials_shared_table
                        )
                    )

                    if history_table is not None:

                        history_insert_data.append(
                            self._filter_to_table_columns(
                                shared_row,
                                history_table
                            )
                        )

                except Exception as error:

                    skipped_count += 1

                    logger.exception(
                        "Failed processing "
                        f"{share_table_name} row "
                        f"{index}: {error}"
                    )

        if not insert_data:

            logger.warning(
                "No valid shared credential records "
                "available for insertion"
            )

            return 0

        with self.dest_engine.begin() as dest_conn:

            result = dest_conn.execute(
                insert(credentials_shared_table),
                insert_data
            )

            history_result = None

            if history_table is not None and history_insert_data:

                history_result = dest_conn.execute(
                    insert(history_table),
                    history_insert_data
                )

        logger.info(
            "Credentials Shared Migration summary: "
            f"inserted={len(insert_data)}, "
            f"history_inserted={len(history_insert_data)}, "
            f"skipped={skipped_count}, "
            f"rowcount={result.rowcount}, "
            f"history_rowcount="
            f"{history_result.rowcount if history_result else 0}"
        )

        return len(
            insert_data
        )

    def _requires_credential_all_uuid(
        self,
        *tables
    ):

        for table in tables:

            if (
                table is not None
                and
                "credential_all_uuid" in table.c
            ):

                return True

        return False

    def _get_credential_row(
        self,
        credential_table,
        credential_source_id
    ):

        if credential_table is None or credential_source_id is None:

            return None

        return self.fetch_one_by_column(
            self.source_engine,
            credential_table.name,
            "id",
            credential_source_id
        )

    def _get_shared_by_user_id(
        self,
        row,
        share_table,
        credential_row
    ):

        share_user_id = self._get_source_value(
            row,
            share_table,
            "shared_by_user_id",
            "shared_by",
            "user_id",
            "created_by",
            "from_user_id"
        )

        if share_user_id:

            return share_user_id

        if credential_row:

            return (
                credential_row.get("user_id")
                or
                credential_row.get("issuer_id")
            )

        return None

    def _get_destination_user_uuid(
        self,
        auth_db_engine,
        source_gl_user_id
    ):

        if not source_gl_user_id:

            return None

        source_gl_user = self.fetch_one_by_column(
            self.source_engine,
            "gl_user",
            "id",
            source_gl_user_id
        )

        if not source_gl_user:

            return None

        source_username = source_gl_user.get(
            "username"
        )

        if not source_username:

            return None

        dest_user = self.fetch_one_by_column(
            auth_db_engine,
            "users",
            "user_name",
            source_username
        )

        if dest_user:

            return dest_user.get(
                "uuid"
            )

        return None

    def _get_destination_user_email(
        self,
        auth_db_engine,
        user_uuid
    ):

        if not user_uuid:

            return None

        dest_user = self.fetch_one_by_column(
            auth_db_engine,
            "users",
            "uuid",
            user_uuid
        )

        if not dest_user:

            return None

        return (
            dest_user.get("email")
            or
            dest_user.get("user_name")
        )

    def _get_credentials_all_row(
        self,
        credentials_all_table,
        credential_uuid
    ):

        if credentials_all_table is None or not credential_uuid:

            return None

        with self.dest_engine.connect() as conn:

            row = conn.execute(
                select(
                    credentials_all_table
                ).where(
                    credentials_all_table.c.uuid
                    == credential_uuid
                )
            ).fetchone()

        if row:

            return row._mapping

        return None

    def _get_shared_with_institution_id(
        self,
        auth_db_engine,
        shared_with_user_uuid,
        credentials_all_row,
        credential_row
    ):

        if credentials_all_row:

            institution_id = credentials_all_row.get(
                "institution_id"
            )

            if institution_id:

                return institution_id

        if shared_with_user_uuid:

            user_institution = self.fetch_one_by_column(
                auth_db_engine,
                "user_institution",
                "user_uuid",
                shared_with_user_uuid
            )

            if user_institution:

                institution_uuid = user_institution.get(
                    "institution_uuid"
                )

                if institution_uuid:

                    return institution_uuid

        if credential_row:

            source_institution_id = credential_row.get(
                "institution_id"
            )

            return self._get_destination_institution_uuid(
                auth_db_engine,
                source_institution_id
            )

        return None

    def _get_institute_name(
        self,
        auth_db_engine,
        institution_uuid,
        credentials_all_row,
        credential_row
    ):

        if credentials_all_row:

            institution_name = (
                credentials_all_row.get("institution_name")
                or
                credentials_all_row.get("institute_name")
            )

            if institution_name:

                return institution_name

        if institution_uuid:

            institution_row = self.fetch_one_by_column(
                auth_db_engine,
                "institutions",
                "uuid",
                institution_uuid
            )

            if institution_row:

                return institution_row.get(
                    "name"
                )

        if credential_row:

            source_institution_id = credential_row.get(
                "institution_id"
            )

            source_institution = self.fetch_one_by_column(
                self.source_engine,
                "institution",
                "id",
                source_institution_id
            )

            if source_institution:

                return source_institution.get(
                    "name"
                )

        return None

    def _get_student_id(
        self,
        credentials_all_row,
        credential_row
    ):

        if credentials_all_row:

            student_id = (
                credentials_all_row.get("student_id")
                or
                credentials_all_row.get("student_number")
            )

            if student_id:

                return student_id

        if not credential_row:

            return None

        source_student_id = credential_row.get(
            "student_id"
        )

        if not source_student_id:

            source_user_id = credential_row.get(
                "user_id"
            )

            source_student = self.fetch_one_by_column(
                self.source_engine,
                "gl_student",
                "user_id",
                source_user_id
            )

        else:

            source_student = self.fetch_one_by_column(
                self.source_engine,
                "gl_student",
                "id",
                source_student_id
            )

        if source_student:

            return (
                source_student.get("school_student_id")
                or
                source_student.get("student_number")
                or
                source_student.get("id")
            )

        return source_student_id

    def _get_destination_institution_uuid(
        self,
        auth_db_engine,
        source_institution_id
    ):

        if not source_institution_id:

            return None

        source_institution = self.fetch_one_by_column(
            self.source_engine,
            "institution",
            "id",
            source_institution_id
        )

        if not source_institution:

            return None

        institution_name = source_institution.get(
            "name"
        )

        if not institution_name:

            return None

        destination_institution = self.fetch_one_by_column(
            auth_db_engine,
            "institutions",
            "name",
            institution_name
        )

        if destination_institution:

            return destination_institution.get(
                "uuid"
            )

        return None

    def _resolve_destination_credential_uuid(
        self,
        credentials_all_table,
        source_config,
        credential_row,
        user_uuid
    ):

        if credentials_all_table is None or not user_uuid:

            return None

        with self.dest_engine.connect() as conn:

            query = select(
                credentials_all_table
            ).where(
                credentials_all_table.c.credential_type
                == source_config["credential_type"]
            ).where(
                credentials_all_table.c.user_id
                == user_uuid
            )

            rows = conn.execute(
                query
            ).fetchall()

        if not rows:

            return None

        if len(rows) == 1:

            return self._get_first_credentials_uuid(
                rows[0]._mapping,
                credentials_all_table,
                source_config
            )

        credential_created_at = None

        if credential_row:

            credential_created_at = (
                credential_row.get("issued_on")
                or
                credential_row.get("issued_date")
                or
                credential_row.get("uploaded_date")
                or
                credential_row.get("upload_date")
                or
                credential_row.get("date_of_request")
                or
                credential_row.get("requested_time")
            )

        if credential_created_at:

            for row in rows:

                row_map = row._mapping

                row_created_at = (
                    row_map.get(
                        credentials_all_table.c.created_at
                    )
                    if "created_at" in credentials_all_table.c
                    else None
                )

                row_issued_on = (
                    row_map.get(
                        credentials_all_table.c.issued_on
                    )
                    if "issued_on" in credentials_all_table.c
                    else None
                )

                if (
                    str(row_created_at) == str(credential_created_at)
                    or
                    str(row_issued_on) == str(credential_created_at)
                ):

                    return self._get_first_credentials_uuid(
                        row_map,
                        credentials_all_table,
                        source_config
                    )

        return self._get_first_credentials_uuid(
            rows[0]._mapping,
            credentials_all_table,
            source_config
        )

    def _get_first_credentials_uuid(
        self,
        row,
        table,
        source_config
    ):

        if "uuid" in table.c:

            value = row.get(
                table.c.uuid
            )

            if value:

                return value

        for column_name in source_config[
            "credentials_all_columns"
        ]:

            if column_name in table.c:

                value = row.get(
                    table.c[column_name]
                )

                if value:

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

    def _map_status(
        self,
        status
    ) -> int:

        if isinstance(status, bool):

            return 1 if status else 0

        normalized_status = (
            str(status or "")
            .strip()
            .lower()
        )

        if normalized_status in [
            "active",
            "shared",
            "success",
            "true",
            "1"
        ]:

            return 1

        if normalized_status in [
            "inactive",
            "revoked",
            "failed",
            "false",
            "0"
        ]:

            return 0

        return 1
