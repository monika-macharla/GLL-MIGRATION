import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UserProfileMigrator(BaseMigrator):

    DEFAULT_BATCH_SIZE = 10000

    MAX_BATCH_SIZE = 10000

    GENDER_MAPPING = {

        "male": 1,

        "female": 2,

        "other": 3,

        "prefer_not_to_say": 4,
    }

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

    def _normalize(
        self,
        value
    ):

        return (
            str(value or "")
            .strip()
            .lower()
        )

    def _clean_datetime(
        self,
        value,
        fallback=None
    ):

        if value is None:

            return fallback

        if isinstance(value, str):

            clean_value = value.strip()

            if (
                not clean_value
                or clean_value.startswith("0000-00-00")
            ):

                return fallback

        return value

    def _map_bool(
        self,
        value
    ):

        if value is None:

            return False

        if isinstance(value, (bytes, bytearray)):

            return value != b"\x00"

        if isinstance(value, bool):

            return value

        return self._normalize(value) in {
            "1",
            "true",
            "yes",
            "y"
        }

    def _first_value(
        self,
        *values
    ):

        for value in values:

            if value is None:

                continue

            if isinstance(value, str) and not value.strip():

                continue

            return value

        return None

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "USER PROFILE MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        source_table = self._manual_reflect(
            "gl_user",
            self.source_engine,
            self.metadata_source
        )

        address_table = self._manual_reflect(
            "address",
            self.source_engine,
            self.metadata_source
        )

        state_table = self._manual_reflect(
            "state",
            self.source_engine,
            self.metadata_source
        )

        country_table = self._manual_reflect(
            "country",
            self.source_engine,
            self.metadata_source
        )

        gl_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        gl_parent_table = self._manual_reflect(
            "gl_parent",
            self.source_engine,
            self.metadata_source
        )

        users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
        )

        user_profile_table = self._manual_reflect(
            "user_profile",
            self.dest_engine,
            self.metadata_dest
        )

        if not user_profile_table.columns:

            raise ValueError(
                "Destination user_profile table not found."
            )

        batch_size = int(
            self.config.get(
                "user_profile_migration_batch_size",
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
                f"Configured user_profile batch size "
                f"{batch_size} is too high; "
                f"using {self.MAX_BATCH_SIZE}."
            )

            batch_size = self.MAX_BATCH_SIZE

        logger.info(
            f"Using user_profile migration batch size: "
            f"{batch_size}"
        )

        migrated_count = 0
        batch_number = 0
        migrated_user_uuids = set()

        with self.source_engine.connect() as source_conn:

            last_source_id = None

            while True:

                query = (
                    select(source_table)
                    .order_by(
                        source_table.c.id
                    )
                    .limit(
                        batch_size
                    )
                )

                if last_source_id is not None:

                    query = query.where(
                        source_table.c.id > last_source_id
                    )

                results = source_conn.execute(
                    query
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
                    f"Processing user_profile source chunk: "
                    f"id {chunk_start_id} to "
                    f"{chunk_end_id}, "
                    f"{len(results)} rows."
                )

                usernames = []
                audit_user_names = []
                address_ids = []
                student_gl_user_ids = []
                student_legacy_user_ids = []
                parent_gl_user_ids = []

                for row in results:

                    row_dict = row._mapping

                    last_source_id = row_dict.get(
                        source_table.c.id
                    )

                    username = row_dict.get(
                        source_table.c.username
                    )

                    if username:

                        usernames.append(username)

                    source_gl_user_id = row_dict.get(
                        source_table.c.id
                    )

                    source_user_type = self._normalize(
                        row_dict.get(
                            source_table.c.user_type
                        )
                    )

                    if (
                        source_user_type == "student"
                        and source_gl_user_id is not None
                    ):

                        student_gl_user_ids.append(
                            source_gl_user_id
                        )

                    source_user_id = row_dict.get(
                        source_table.c.user_id
                    )

                    if (
                        source_user_type == "student"
                        and source_user_id is not None
                    ):

                        student_legacy_user_ids.append(
                            source_user_id
                        )

                    if (
                        source_user_type == "parent"
                        and source_gl_user_id is not None
                    ):

                        parent_gl_user_ids.append(
                            source_gl_user_id
                        )

                    for audit_column_name in (
                        "created_user",
                        "last_modified_user"
                    ):

                        if audit_column_name not in source_table.c:

                            continue

                        audit_user_name = row_dict.get(
                            source_table.c[audit_column_name]
                        )

                        if audit_user_name:

                            audit_user_names.append(
                                audit_user_name
                            )

                    source_address_id = row_dict.get(
                        source_table.c.address_id
                    )

                    if source_address_id is not None:

                        address_ids.append(
                            source_address_id
                        )

                parent_lookup = {}

                if parent_gl_user_ids:

                    parent_results = source_conn.execute(
                        select(
                            gl_parent_table.c.user_id,
                            gl_parent_table.c.first_name,
                            gl_parent_table.c.middle_name,
                            gl_parent_table.c.last_name,
                            gl_parent_table.c.date_of_birth,
                            gl_parent_table.c.address_id,
                            gl_parent_table.c.gender,
                            gl_parent_table.c.phone_number,
                            gl_parent_table.c.email_address
                        )
                        .where(
                            gl_parent_table.c.user_id.in_(
                                list(set(parent_gl_user_ids))
                            )
                        )
                        .order_by(
                            gl_parent_table.c.id
                        )
                    )

                    for parent_row in parent_results:

                        parent_row_dict = parent_row._mapping
                        parent_user_id = parent_row_dict.get(
                            gl_parent_table.c.user_id
                        )

                        if parent_user_id in parent_lookup:

                            continue

                        parent_lookup[
                            parent_user_id
                        ] = parent_row_dict

                        parent_address_id = parent_row_dict.get(
                            gl_parent_table.c.address_id
                        )

                        if parent_address_id is not None:

                            address_ids.append(
                                parent_address_id
                            )

                student_lookup = {}

                if student_gl_user_ids:

                    student_results = source_conn.execute(
                        select(
                            gl_student_table.c.user_id,
                            gl_student_table.c.first_name,
                            gl_student_table.c.middle_name,
                            gl_student_table.c.last_name,
                            gl_student_table.c.date_of_birth,
                            gl_student_table.c.address_id,
                            gl_student_table.c.gender,
                            gl_student_table.c.phone_no,
                            gl_student_table.c.last4_ssn,
                            gl_student_table.c.person_ethnics
                        )
                        .where(
                            gl_student_table.c.user_id.in_(
                                list(set(student_gl_user_ids))
                            )
                        )
                        .order_by(
                            gl_student_table.c.id
                        )
                    )

                    for student_row in student_results:

                        student_row_dict = student_row._mapping

                        student_user_id = student_row_dict.get(
                            gl_student_table.c.user_id
                        )

                        if student_user_id not in student_lookup:

                            student_lookup[
                                student_user_id
                            ] = dict(student_row_dict)

                        else:

                            existing_student = student_lookup[
                                student_user_id
                            ]

                            for student_column in (
                                gl_student_table.c.first_name,
                                gl_student_table.c.middle_name,
                                gl_student_table.c.last_name,
                                gl_student_table.c.date_of_birth,
                                gl_student_table.c.address_id,
                                gl_student_table.c.gender,
                                gl_student_table.c.phone_no,
                                gl_student_table.c.last4_ssn,
                                gl_student_table.c.person_ethnics
                            ):

                                if self._first_value(
                                    existing_student.get(
                                        student_column
                                    )
                                ) is not None:

                                    continue

                                existing_student[
                                    student_column
                                ] = student_row_dict.get(
                                    student_column
                                )

                        student_address_id = student_row_dict.get(
                            gl_student_table.c.address_id
                        )

                        if student_address_id is not None:

                            address_ids.append(
                                student_address_id
                            )

                legacy_student_ssn_lookup = {}

                if student_legacy_user_ids:

                    legacy_student_results = source_conn.execute(
                        select(
                            gl_student_table.c.user_id,
                            gl_student_table.c.last4_ssn
                        )
                        .where(
                            gl_student_table.c.user_id.in_(
                                list(set(student_legacy_user_ids))
                            )
                        )
                        .where(
                            gl_student_table.c.last4_ssn.is_not(
                                None
                            )
                        )
                        .order_by(
                            gl_student_table.c.id
                        )
                    )

                    for legacy_student_row in legacy_student_results:

                        legacy_student_row_dict = (
                            legacy_student_row._mapping
                        )

                        legacy_user_id = legacy_student_row_dict.get(
                            gl_student_table.c.user_id
                        )

                        legacy_ssn = self._first_value(
                            legacy_student_row_dict.get(
                                gl_student_table.c.last4_ssn
                            )
                        )

                        if (
                            legacy_user_id is None
                            or legacy_ssn is None
                            or legacy_user_id in legacy_student_ssn_lookup
                        ):

                            continue

                        legacy_student_ssn_lookup[
                            legacy_user_id
                        ] = legacy_ssn

                destination_user_lookup = {}

                lookup_user_names = list(
                    set(
                        usernames
                        + audit_user_names
                    )
                )

                if lookup_user_names:

                    destination_results = (
                        self.dest_engine.connect()
                    )

                    with destination_results as dest_conn:

                        user_results = dest_conn.execute(
                            select(
                                users_table.c.uuid,
                                users_table.c.user_name
                            ).where(
                                users_table.c.user_name.in_(
                                    lookup_user_names
                                )
                            )
                        )

                        for user_row in user_results:

                            user_row_dict = user_row._mapping

                            destination_user_lookup[
                                self._normalize(
                                    user_row_dict.get(
                                        users_table.c.user_name
                                    )
                                )
                            ] = user_row_dict.get(
                                users_table.c.uuid
                            )

                existing_profile_user_uuids = set()

                lookup_user_uuids = [
                    value
                    for value in destination_user_lookup.values()
                    if value is not None
                ]

                if lookup_user_uuids:

                    with self.dest_engine.connect() as dest_conn:

                        existing_profile_results = (
                            dest_conn.execute(
                                select(
                                    user_profile_table.c.user_uuid
                                ).where(
                                    user_profile_table.c.user_uuid.in_(
                                        list(set(lookup_user_uuids))
                                    )
                                )
                            )
                        )

                        for profile_row in existing_profile_results:

                            existing_profile_user_uuids.add(
                                profile_row._mapping.get(
                                    user_profile_table.c.user_uuid
                                )
                            )

                address_lookup = {}

                if address_ids:

                    address_results = source_conn.execute(
                        select(
                            address_table.c.id,
                            address_table.c.address_line_1,
                            address_table.c.address_line_2,
                            address_table.c.city,
                            address_table.c.zip_code,
                            state_table.c.state_code,
                            country_table.c.country_code
                        )
                        .select_from(
                            address_table
                            .join(
                                state_table,
                                address_table.c.state
                                == state_table.c.id,
                                isouter=True
                            )
                            .join(
                                country_table,
                                address_table.c.country
                                == country_table.c.id,
                                isouter=True
                            )
                        )
                        .where(
                            address_table.c.id.in_(
                                list(set(address_ids))
                            )
                        )
                    )

                    for address_row in address_results:

                        address_row_dict = address_row._mapping

                        address_lookup[
                            address_row_dict.get(
                                address_table.c.id
                            )
                        ] = {

                            "address_line_1": address_row_dict.get(
                                address_table.c.address_line_1
                            ),

                            "address_line_2": address_row_dict.get(
                                address_table.c.address_line_2
                            ),

                            "city": address_row_dict.get(
                                address_table.c.city
                            ),

                            "zip_code": address_row_dict.get(
                                address_table.c.zip_code
                            ),

                            "state": address_row_dict.get(
                                state_table.c.state_code
                            ),

                            "country_code": address_row_dict.get(
                                country_table.c.country_code
                            ),
                        }

                insert_data = []
                prepared_user_uuids = set()
                skipped_users = 0
                skipped_existing_profiles = 0
                skipped_duplicate_profiles = 0
                created_by_mapped = 0
                updated_by_mapped = 0
                student_profiles_sourced = 0
                parent_profiles_sourced = 0
                missing_student_details = 0
                missing_parent_details = 0
                legacy_student_ssn_used = 0
                row_errors = 0

                for row in results:

                    try:

                        row_dict = row._mapping

                        username = row_dict.get(
                            source_table.c.username
                        )

                        user_uuid = destination_user_lookup.get(
                            self._normalize(username)
                        )

                        if not user_uuid:

                            skipped_users += 1

                            continue

                        if user_uuid in existing_profile_user_uuids:

                            skipped_existing_profiles += 1

                            continue

                        if (
                            user_uuid in migrated_user_uuids
                            or user_uuid in prepared_user_uuids
                        ):

                            skipped_duplicate_profiles += 1

                            continue

                        created_date = self._clean_datetime(
                            row_dict.get(
                                source_table.c.created_date
                            )
                        )

                        updated_date = self._clean_datetime(
                            row_dict.get(
                                source_table.c.last_change_date
                            )
                        )

                        if not created_date:

                            created_date = (
                                updated_date
                                or datetime.utcnow()
                            )

                        if not updated_date:

                            updated_date = created_date

                        created_user_name = (
                            row_dict.get(
                                source_table.c.created_user
                            )
                            if "created_user" in source_table.c
                            else None
                        )

                        last_modified_user_name = (
                            row_dict.get(
                                source_table.c.last_modified_user
                            )
                            if "last_modified_user" in source_table.c
                            else None
                        )

                        created_by_uuid = (
                            destination_user_lookup.get(
                                self._normalize(
                                    created_user_name
                                )
                            )
                        )

                        if created_by_uuid:

                            created_by_mapped += 1

                        else:

                            created_by_uuid = user_uuid

                        updated_by_uuid = (
                            destination_user_lookup.get(
                                self._normalize(
                                    last_modified_user_name
                                )
                            )
                        )

                        if updated_by_uuid:

                            updated_by_mapped += 1

                        else:

                            updated_by_uuid = created_by_uuid

                        source_gender = row_dict.get(
                            source_table.c.gender
                        )

                        source_gl_user_id = row_dict.get(
                            source_table.c.id
                        )

                        source_user_type = self._normalize(
                            row_dict.get(
                                source_table.c.user_type
                            )
                        )

                        parent_data = {}
                        student_data = {}

                        if source_user_type == "student":

                            student_data = student_lookup.get(
                                source_gl_user_id,
                                {}
                            )

                            if student_data:

                                student_profiles_sourced += 1

                            else:

                                missing_student_details += 1

                        if source_user_type == "parent":

                            parent_data = parent_lookup.get(
                                source_gl_user_id,
                                {}
                            )

                            if parent_data:

                                parent_profiles_sourced += 1

                            else:

                                missing_parent_details += 1

                        source_gender = self._first_value(
                            student_data.get(
                                gl_student_table.c.gender
                            ),
                            parent_data.get(
                                gl_parent_table.c.gender
                            ),
                            source_gender
                        )

                        mapped_gender = (
                            self.GENDER_MAPPING.get(
                                self._normalize(source_gender),
                                0
                            )
                        )

                        address_data = address_lookup.get(
                            self._first_value(
                                student_data.get(
                                    gl_student_table.c.address_id
                                ),
                                parent_data.get(
                                    gl_parent_table.c.address_id
                                ),
                                row_dict.get(
                                    source_table.c.address_id
                                )
                            ),
                            {}
                        )

                        ssn_number = self._first_value(
                            student_data.get(
                                gl_student_table.c.last4_ssn
                            ),
                            legacy_student_ssn_lookup.get(
                                row_dict.get(
                                    source_table.c.user_id
                                )
                            ),
                            row_dict.get(
                                source_table.c.last4_ssn
                            )
                        )

                        if (
                            self._first_value(
                                student_data.get(
                                    gl_student_table.c.last4_ssn
                                )
                            ) is None
                            and self._first_value(
                                legacy_student_ssn_lookup.get(
                                    row_dict.get(
                                        source_table.c.user_id
                                    )
                                )
                            ) is not None
                        ):

                            legacy_student_ssn_used += 1

                        ethnicity = self._first_value(
                            student_data.get(
                                gl_student_table.c.person_ethnics
                            ),
                            row_dict.get(
                                source_table.c.ethnicity
                            )
                        )

                        dob = self._clean_datetime(
                            self._first_value(
                                student_data.get(
                                    gl_student_table.c.date_of_birth
                                ),
                                parent_data.get(
                                    gl_parent_table.c.date_of_birth
                                ),
                                row_dict.get(
                                    source_table.c.date_of_birth
                                )
                            )
                        )

                        first_name = self._first_value(
                            student_data.get(
                                gl_student_table.c.first_name
                            ),
                            parent_data.get(
                                gl_parent_table.c.first_name
                            ),
                            row_dict.get(
                                source_table.c.first_name
                            )
                        )

                        middle_name = self._first_value(
                            student_data.get(
                                gl_student_table.c.middle_name
                            ),
                            parent_data.get(
                                gl_parent_table.c.middle_name
                            ),
                            row_dict.get(
                                source_table.c.middle_name
                            )
                        )

                        last_name = self._first_value(
                            student_data.get(
                                gl_student_table.c.last_name
                            ),
                            parent_data.get(
                                gl_parent_table.c.last_name
                            ),
                            row_dict.get(
                                source_table.c.last_name
                            )
                        )

                        phone_number = self._first_value(
                            student_data.get(
                                gl_student_table.c.phone_no
                            ),
                            parent_data.get(
                                gl_parent_table.c.phone_number
                            ),
                            row_dict.get(
                                source_table.c.phone_no
                            )
                        )

                        insert_data.append({

                            "uuid": str(
                                uuid.uuid4()
                            ),

                            "created_at": created_date,

                            "updated_at": updated_date,

                            "deleted_at": None,

                            "profile_pic": None,

                            "dob": dob,

                            "prefix": None,

                            "first_name": first_name,

                            "middle_name": middle_name,

                            "last_name": last_name,

                            "suffix": None,

                            "address_line1": address_data.get(
                                "address_line_1"
                            ),

                            "address_line2": address_data.get(
                                "address_line_2"
                            ),

                            "city": address_data.get(
                                "city"
                            ),

                            "state": address_data.get(
                                "state"
                            ),

                            "zip_code": address_data.get(
                                "zip_code"
                            ),

                            "phone_number": phone_number,

                            "country": address_data.get(
                                "country_code"
                            ),

                            "country_code": address_data.get(
                                "country_code"
                            ),

                            "created_by": created_by_uuid,

                            "user_uuid": user_uuid,

                            "ssn_number": ssn_number,

                            "ethnicity": ethnicity,

                            "two_factor_auth_option": (
                                self._map_bool(
                                    row_dict.get(
                                        source_table.c.two_factor_auth
                                    )
                                )
                            ),

                            "job_alerts_email_notification": False,

                            "updated_by": updated_by_uuid,

                            "show_student_intro": False,

                            "totp_secret": None,

                            "is_totp_verified": False,

                            "has_employment_history": False,

                            "gender": mapped_gender,

                            "department": row_dict.get(
                                source_table.c.department
                            ),

                            "title": row_dict.get(
                                source_table.c.title
                            ),
                        })

                        prepared_user_uuids.add(
                            user_uuid
                        )

                    except Exception as row_error:

                        row_errors += 1

                        logger.exception(
                            f"Failed processing profile row: "
                            f"{str(row_error)}"
                        )

                if insert_data:

                    batch_number += 1

                    logger.info(
                        f"User profile chunk {batch_number}: "
                        f"inserting {len(insert_data)} profiles; "
                        f"skipped_users={skipped_users}, "
                        f"skipped_existing_profiles="
                        f"{skipped_existing_profiles}, "
                        f"skipped_duplicate_profiles="
                        f"{skipped_duplicate_profiles}, "
                        f"student_details={student_profiles_sourced}, "
                        f"parent_details={parent_profiles_sourced}, "
                        f"missing_student_details="
                        f"{missing_student_details}, "
                        f"missing_parent_details="
                        f"{missing_parent_details}, "
                        f"legacy_student_ssn_used="
                        f"{legacy_student_ssn_used}, "
                        f"created_by_mapped={created_by_mapped}, "
                        f"updated_by_mapped={updated_by_mapped}, "
                        f"errors={row_errors}."
                    )

                    with self.dest_engine.begin() as dest_conn:

                        dest_conn.execute(
                            insert(user_profile_table),
                            insert_data
                        )

                    migrated_count += len(insert_data)

                    migrated_user_uuids.update(
                        prepared_user_uuids
                    )

                else:

                    logger.warning(
                        f"User profile chunk id "
                        f"{chunk_start_id} to {chunk_end_id}: "
                        f"no profiles prepared; "
                        f"skipped_users={skipped_users}, "
                        f"skipped_existing_profiles="
                        f"{skipped_existing_profiles}, "
                        f"skipped_duplicate_profiles="
                        f"{skipped_duplicate_profiles}, "
                        f"errors={row_errors}."
                    )

                if len(results) < batch_size:

                    break

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{migrated_count} user profiles."
        )

        logger.info(
            "======================================="
        )

        return migrated_count
