import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class TranscriptMigrator(BaseMigrator):

    CREDENTIAL_TYPE = 1

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
            "Starting Transcript Migration..."
        )

        auth_db_engine = self.get_lookup_engine(
            "auth_db"
        )

        if not auth_db_engine:

            raise ValueError(
                "auth_db lookup engine not configured"
            )

        transcript_table = self._manual_reflect(
            "transcript",
            self.source_engine,
            self.metadata_source
        )

        transcript_dest_table = self._manual_reflect(
            "credentials_transcripts",
            self.dest_engine,
            self.metadata_dest
        )

        credentials_table = self._manual_reflect(
            "credentials_all",
            self.dest_engine,
            self.metadata_dest
        )

        query = select(
            transcript_table
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
            f"Found {len(rows)} transcript records"
        )

        transcript_insert_data = []

        credentials_insert_data = []

        skipped_count = 0

        for index, row in enumerate(
            rows,
            start=1
        ):

            try:

                row_dict = row._mapping

                source_transcript_id = row_dict.get(
                    transcript_table.c.id
                )

                source_gl_user_id = row_dict.get(
                    transcript_table.c.user_id
                )

                source_student_id = row_dict.get(
                    transcript_table.c.student_id
                )

                source_gl_student = None

                if source_student_id:

                    source_gl_student = self.fetch_one_by_column(
                        self.source_engine,
                        "gl_student",
                        "id",
                        source_student_id
                    )

                    if source_gl_student and not source_gl_user_id:

                        source_gl_user_id = source_gl_student.get(
                            "user_id"
                        )

                if not source_gl_user_id:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping transcript row {index}: "
                        "user_id is null"
                    )

                    continue

                source_gl_user = self.fetch_one_by_column(
                    self.source_engine,
                    "gl_user",
                    "id",
                    source_gl_user_id
                )

                if not source_gl_user:

                    skipped_count += 1

                    logger.warning(
                        "Skipping transcript row "
                        f"{index}: no gl_user for id "
                        f"{source_gl_user_id}"
                    )

                    continue

                source_username = source_gl_user.get(
                    "username"
                )

                if not source_username:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping transcript row {index}: "
                        "username is null"
                    )

                    continue

                dest_user = self.fetch_one_by_column(
                    auth_db_engine,
                    "users",
                    "user_name",
                    source_username
                )

                if not dest_user:

                    skipped_count += 1

                    logger.warning(
                        "Skipping transcript row "
                        f"{index}: no destination user for "
                        f"{source_username}"
                    )

                    continue

                destination_user_uuid = dest_user.get(
                    "uuid"
                )

                user_institution = self.fetch_one_by_column(
                    auth_db_engine,
                    "user_institution",
                    "user_uuid",
                    destination_user_uuid
                )

                fallback_institution_uuid = None

                if user_institution:

                    fallback_institution_uuid = (
                        user_institution.get(
                            "institution_uuid"
                        )
                    )

                institution_uuid = (
                    self._get_destination_institution_uuid(
                        auth_db_engine,
                        row_dict.get(
                            transcript_table.c.institution_id
                        )
                    )
                    or
                    fallback_institution_uuid
                )

                if not institution_uuid:

                    skipped_count += 1

                    logger.warning(
                        f"Skipping transcript row {index}: "
                        "institution_uuid is null"
                    )

                    continue

                institution_row = self.fetch_one_by_column(
                    auth_db_engine,
                    "institutions",
                    "uuid",
                    institution_uuid
                )

                institution_name = (
                    institution_row.get("name")
                    if institution_row
                    else None
                )

                if not source_gl_student and source_student_id:

                    source_gl_student = self.fetch_one_by_column(
                        self.source_engine,
                        "gl_student",
                        "id",
                        source_student_id
                    )

                student_number = None

                student_first_name = None

                student_last_name = None

                student_date_of_birth = None

                if source_gl_student:

                    student_number = (
                        source_gl_student.get(
                            "school_student_id"
                        )
                        or
                        source_gl_student.get(
                            "student_number"
                        )
                        or
                        source_student_id
                    )

                    student_first_name = source_gl_student.get(
                        "first_name"
                    )

                    student_last_name = source_gl_student.get(
                        "last_name"
                    )

                    student_date_of_birth = source_gl_student.get(
                        "date_of_birth"
                    )

                enrollment_code = self._get_enrollment_code(
                    auth_db_engine,
                    destination_user_uuid,
                    row_dict.get(
                        transcript_table.c.enrollment_id
                    ),
                    source_student_id
                )

                if enrollment_code is None:

                    enrollment_code = ""

                created_at = (
                    row_dict.get(
                        transcript_table.c.issued_date
                    )
                    or
                    row_dict.get(
                        transcript_table.c.requested_time
                    )
                )

                if not created_at:

                    created_at = datetime.utcnow()

                    logger.warning(
                        "Transcript row "
                        f"{source_transcript_id} has no issued_date "
                        "or requested_time. Using current UTC time "
                        "for created_at/updated_at."
                    )

                transcript_uuid = str(
                    uuid.uuid4()
                )

                credential_path = (
                    f"transcript/"
                    f"{source_transcript_id}/"
                    f"credential_data"
                )

                status = self._map_status(
                    row_dict.get(
                        transcript_table.c.status
                    )
                )

                transcript_row = {
                    "uuid": transcript_uuid,
                    "created_at": created_at,
                    "updated_at": created_at,
                    "deleted_at": None,
                    "user_id": destination_user_uuid,
                    "institution_id": institution_uuid,
                    "status": status,
                    "credential_type": self.CREDENTIAL_TYPE,
                    "credential_path": credential_path,
                    "enrollment_code": enrollment_code,
                    "created_by": destination_user_uuid,
                    "updated_by": destination_user_uuid,
                    "deleted_by": None,
                    "generated_on": None,
                }

                transcript_insert_data.append(
                    transcript_row
                )

                credentials_row = {
                    "uuid": str(uuid.uuid4()),
                    "created_at": created_at,
                    "updated_at": created_at,
                    "deleted_at": None,
                    "user_id": destination_user_uuid,
                    "student_user_name": source_username,
                    "student_first_name": student_first_name,
                    "student_last_name": student_last_name,
                    "institution_name": institution_name,
                    "student_id": (
                        str(student_number)
                        if student_number is not None
                        else None
                    ),
                    "student_email": source_username,
                    "date_of_birth": (
                        str(student_date_of_birth)
                        if student_date_of_birth is not None
                        else None
                    ),
                    "credential_claim_status": 0,
                    "is_registered": 1,
                    "institution_id": institution_uuid,
                    "status": status,
                    "credential_type": self.CREDENTIAL_TYPE,
                    "credential_path": credential_path,
                    "enrollment_code": enrollment_code,
                    "created_by": destination_user_uuid,
                    "updated_by": destination_user_uuid,
                    "deleted_by": None,
                    "transcripts": transcript_uuid,
                    "generated_on": None,
                    "issued_on": (
                        str(created_at)
                        if created_at is not None
                        else None
                    ),
                    "student_number": (
                        str(student_number)
                        if student_number is not None
                        else None
                    ),
                }

                self._set_if_column(
                    credentials_row,
                    credentials_table,
                    "blockchain_hash",
                    row_dict.get(
                        transcript_table.c.blockchain_hash
                    )
                )

                credentials_insert_data.append(
                    credentials_row
                )

            except Exception as error:

                skipped_count += 1

                logger.exception(
                    f"Failed processing transcript row "
                    f"{index}: {error}"
                )

        if not transcript_insert_data:

            logger.warning(
                "No valid transcript records available for insertion"
            )

            return 0

        with self.dest_engine.begin() as dest_conn:

            result = dest_conn.execute(
                insert(transcript_dest_table),
                transcript_insert_data
            )

            logger.info(
                f"Inserted transcript rows: {result.rowcount}"
            )

            credentials_result = dest_conn.execute(
                insert(credentials_table),
                credentials_insert_data
            )

            logger.info(
                f"Inserted credentials_all rows: "
                f"{credentials_result.rowcount}"
            )

        logger.info(
            "Transcript Migration summary: "
            f"inserted={len(transcript_insert_data)}, "
            f"skipped={skipped_count}"
        )

        return len(
            transcript_insert_data
        )

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

    def _get_enrollment_code(
        self,
        auth_db_engine,
        destination_user_uuid,
        source_enrollment_id,
        source_student_id
    ):

        if source_enrollment_id:

            source_enrollment = self.fetch_one_by_column(
                self.source_engine,
                "enrollment",
                "id",
                source_enrollment_id
            )

            enrollment_code = self._extract_enrollment_code(
                source_enrollment
            )

            if enrollment_code:

                return enrollment_code

        if source_student_id:

            source_enrollment = self.fetch_one_by_column(
                self.source_engine,
                "enrollment",
                "student_id",
                source_student_id
            )

            enrollment_code = self._extract_enrollment_code(
                source_enrollment
            )

            if enrollment_code:

                return enrollment_code

        destination_enrollment = self.fetch_one_by_column(
            auth_db_engine,
            "user_enrollments",
            "user_uuid",
            destination_user_uuid
        )

        if destination_enrollment:

            return (
                destination_enrollment.get(
                    "enrollment_code"
                )
                or
                ""
            )

        return ""

    def _extract_enrollment_code(
        self,
        enrollment_row
    ):

        if not enrollment_row:

            return None

        return (
            enrollment_row.get("enrollment_UUID")
            or
            enrollment_row.get("enrollment_code")
        )

    def _map_status(
        self,
        status
    ) -> int:

        normalized_status = (
            str(status or "")
            .strip()
            .lower()
        )

        if normalized_status in [
            "issued",
            "active",
            "completed",
            "success",
            "generated"
        ]:

            return 3

        if normalized_status in [
            "inactive",
            "failed",
            "error",
            "rejected"
        ]:

            return 2

        return 1

    def _set_if_column(
        self,
        row,
        table,
        column_name,
        value
    ) -> None:

        if column_name in table.c:

            row[column_name] = value
