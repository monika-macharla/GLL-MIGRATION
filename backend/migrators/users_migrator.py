import uuid
import logging

from sqlalchemy import (
    select,
    insert,
    update
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class UsersMigrator(BaseMigrator):

    # -------------------------------------------------
    # SOURCE institution_user.role_id
    # -> destination role.code
    # -------------------------------------------------

    INSTITUTION_ROLE_MAPPING = {

        1: "super_admin",

        2: "institution_admin",

        3: "receiver",

        4: "recruiter",

        5: "developer",

        6: "counsellor",

        7: "recommender",

        8: "service_provider",

        9: "career_services",
    }

    # -------------------------------------------------
    # SOURCE STATUS ENUM
    # -------------------------------------------------

    USER_STATUS_MAPPING = {

        "active": 1,
        "inactive": 2,
        "deleted": 3,
        "deactivated": 4,

        # numeric support
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
    }

    # -------------------------------------------------
    # Gender Mapping
    # -------------------------------------------------

    GENDER_MAPPING = {

        "male": 1,

        "female": 2,

        "other": 3,

        "prefer_not_to_say": 4,
    }

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
    # Main Migration
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "USERS MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        # -------------------------------------------------
        # Detect Source Table
        # -------------------------------------------------

        mappings = self.config.get(
            "mappings",
            []
        )

        source_table_name = None

        for mapping in mappings:

            if mapping.get(
                "destination_table"
            ) == "users":

                source_table_name = mapping.get(
                    "source_table"
                )

                break

        if not source_table_name:

            raise ValueError(
                "Users source table "
                "not found in mappings."
            )

        logger.info(
            f"Using source table: "
            f"{source_table_name}"
        )

        # -------------------------------------------------
        # Reflect Tables
        # -------------------------------------------------

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

        address_table = self._manual_reflect(
            "address",
            self.source_engine,
            self.metadata_source
        )

        users_table = self._manual_reflect(
            "users",
            self.dest_engine,
            self.metadata_dest
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

        user_profile_table = self._manual_reflect(
            "user_profile",
            self.dest_engine,
            self.metadata_dest
        )

        # -------------------------------------------------
        # Validate
        # -------------------------------------------------

        if not users_table.columns:

            raise ValueError(
                "Destination users table "
                "not found."
            )

        if not roles_table.columns:

            raise ValueError(
                "Destination role table "
                "not found."
            )

        if not user_role_table.columns:

            raise ValueError(
                "Destination user_role table "
                "not found."
            )

        if not user_profile_table.columns:

            raise ValueError(
                "Destination user_profile table "
                "not found."
            )

        # -------------------------------------------------
        # Build Role Lookup
        # role.code -> role.uuid
        # -------------------------------------------------

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

        # -------------------------------------------------
        # Build institution_user Lookup
        # user_id -> role + status
        # -------------------------------------------------

        institution_user_lookup = {}

        with self.source_engine.connect() as conn:

            institution_results = conn.execute(
                select(
                    institution_user_table.c.user_id,
                    institution_user_table.c.role_id,
                    institution_user_table.c.status
                )
            )

            for row in institution_results:

                row_dict = row._mapping

                source_user_id = row_dict.get(
                    institution_user_table.c.user_id
                )

                source_role_id = row_dict.get(
                    institution_user_table.c.role_id
                )

                source_status = row_dict.get(
                    institution_user_table.c.status
                )

                role_code = (
                    self.INSTITUTION_ROLE_MAPPING.get(
                        source_role_id
                    )
                )

                active_role_uuid = None

                if role_code:

                    active_role_uuid = role_lookup.get(
                        str(role_code)
                        .strip()
                        .lower()
                    )

                # status mapping
                mapped_status = (
                    self.USER_STATUS_MAPPING.get(
                        str(source_status)
                        .strip()
                        .lower(),
                        0
                    )
                )

                institution_user_lookup[
                    source_user_id
                ] = {

                    "source_role_id": source_role_id,

                    "role_code": role_code,

                    "active_role_uuid": active_role_uuid,

                    "status": mapped_status
                }

        logger.info(
            f"Loaded "
            f"{len(institution_user_lookup)} "
            f"institution_user mappings."
        )

        # -------------------------------------------------
        # Build Address Lookup
        # -------------------------------------------------

        address_lookup = {}

        with self.source_engine.connect() as conn:

            address_results = conn.execute(
                select(address_table)
            )

            for row in address_results:

                row_dict = row._mapping

                address_lookup[
                    row_dict.get(address_table.c.id)
                ] = {

                    "address_line_1": row_dict.get(
                        address_table.c.address_line_1
                    ),

                    "address_line_2": row_dict.get(
                        address_table.c.address_line_2
                    ),

                    "state": row_dict.get(
                        address_table.c.state
                    ),

                    "city": row_dict.get(
                        address_table.c.city
                    ),

                    "zip_code": row_dict.get(
                        address_table.c.zip_code
                    ),

                    "country": row_dict.get(
                        address_table.c.country
                    ),
                }

        logger.info(
            f"Loaded "
            f"{len(address_lookup)} addresses."
        )

        # -------------------------------------------------
        # Source Query
        # -------------------------------------------------

        query = select(source_table)

        # -------------------------------------------------
        # FIRST PASS
        # -------------------------------------------------

        user_uuid_lookup = {}

        source_rows = []

        with self.source_engine.connect() as source_conn:

            results = source_conn.execute(query)

            for row in results:

                row_dict = row._mapping

                username = row_dict.get(
                    source_table.c.username
                )

                generated_uuid = str(
                    uuid.uuid4()
                )

                if username:

                    user_uuid_lookup[
                        str(username)
                        .strip()
                        .lower()
                    ] = generated_uuid

                source_rows.append(
                    (row_dict, generated_uuid)
                )

        logger.info(
            f"Built UUID lookup for "
            f"{len(user_uuid_lookup)} users."
        )

        # -------------------------------------------------
        # Prepare Insert Data
        # -------------------------------------------------

        users_insert_data = []

        user_role_insert_data = []

        user_profile_insert_data = []

        audit_update_rows = []

        # -------------------------------------------------
        # SECOND PASS
        # -------------------------------------------------

        for row_dict, generated_uuid in source_rows:

            try:

                source_user_id = row_dict.get(
                    source_table.c.id
                )

                username = row_dict.get(
                    source_table.c.username
                )

                email = row_dict.get(
                    source_table.c.email
                )

                logger.info(
                    f"Migrating user: "
                    f"{username}"
                )

                # -------------------------------------------------
                # Audit Fields
                # -------------------------------------------------

                created_user = row_dict.get(
                    source_table.c.created_user
                )

                last_modified_user = row_dict.get(
                    source_table.c.last_modified_user
                )

                created_by_uuid = None

                updated_by_uuid = None

                if created_user:

                    created_by_uuid = (
                        user_uuid_lookup.get(
                            str(created_user)
                            .strip()
                            .lower()
                        )
                    )

                if last_modified_user:

                    updated_by_uuid = (
                        user_uuid_lookup.get(
                            str(last_modified_user)
                            .strip()
                            .lower()
                        )
                    )

                # -------------------------------------------------
                # Dates
                # -------------------------------------------------

                created_date = row_dict.get(
                    source_table.c.created_date
                )

                last_change_date = row_dict.get(
                    source_table.c.last_change_date
                )

                # -------------------------------------------------
                # institution_user lookup
                # -------------------------------------------------

                institution_user_data = (
                    institution_user_lookup.get(
                        source_user_id,
                        {}
                    )
                )

                active_role_uuid = (
                    institution_user_data.get(
                        "active_role_uuid"
                    )
                )

                user_status = (
                    institution_user_data.get(
                        "status",
                        0
                    )
                )

                # -------------------------------------------------
                # Users Row
                # -------------------------------------------------

                mapped_user_row = {

                    "uuid": generated_uuid,

                    "created_at": created_date,

                    "updated_at": (
                        last_change_date
                        or created_date
                    ),

                    "deleted_at": None,

                    "user_name": username,

                    "email": email,

                    # FIX 1
                    # status from institution_user
                    "status": user_status,

                    # FIX 2
                    # active_role_uuid from institution_user.role_id
                    "active_role_uuid": active_role_uuid,

                    "created_by": None,

                    "updated_by": None,
                }

                users_insert_data.append(
                    mapped_user_row
                )

                # -------------------------------------------------
                # Audit Update
                # -------------------------------------------------

                audit_update_rows.append({

                    "user_uuid": generated_uuid,

                    "created_by": created_by_uuid,

                    "updated_by": updated_by_uuid,
                })

                # -------------------------------------------------
                # user_role Row
                # -------------------------------------------------

                if active_role_uuid:

                    mapped_user_role_row = {

                        "uuid": str(
                            uuid.uuid4()
                        ),

                        "created_at": created_date,

                        "updated_at": (
                            last_change_date
                            or created_date
                        ),

                        "deleted_at": None,

                        "user_uuid": generated_uuid,

                        "role": active_role_uuid,

                        "created_by": created_by_uuid,
                    }

                    user_role_insert_data.append(
                        mapped_user_role_row
                    )

                # -------------------------------------------------
                # Gender Mapping
                # -------------------------------------------------

                source_gender = row_dict.get(
                    source_table.c.gender
                )

                mapped_gender = 0

                if source_gender:

                    mapped_gender = (
                        self.GENDER_MAPPING.get(
                            str(source_gender)
                            .strip()
                            .lower(),
                            0
                        )
                    )

                # -------------------------------------------------
                # Address Lookup
                # -------------------------------------------------

                source_address_id = row_dict.get(
                    source_table.c.address_id
                )

                address_data = (
                    address_lookup.get(
                        source_address_id,
                        {}
                    )
                )

                # -------------------------------------------------
                # PROFILE DATA
                # -------------------------------------------------

                mapped_profile_row = {

                    "uuid": str(
                        uuid.uuid4()
                    ),

                    "created_at": created_date,

                    "updated_at": (
                        last_change_date
                        or created_date
                    ),

                    "deleted_at": None,

                    "profile_pic": None,

                    "dob": row_dict.get(
                        source_table.c.date_of_birth
                    ),

                    "gender": mapped_gender,

                    "prefix": row_dict.get(
                        source_table.c.title
                    ),

                    "first_name": row_dict.get(
                        source_table.c.first_name
                    ),

                    "middle_name": row_dict.get(
                        source_table.c.middle_name
                    ),

                    "last_name": row_dict.get(
                        source_table.c.last_name
                    ),

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

                    "country_code": address_data.get(
                        "country"
                    ),

                    "created_by": created_by_uuid,

                    "updated_by": updated_by_uuid,

                    "user_uuid": generated_uuid,

                    "phone_number": row_dict.get(
                        source_table.c.phone_no
                    ),

                    "country": address_data.get(
                        "country"
                    ),

                    "ssn_number": row_dict.get(
                        source_table.c.last4_ssn
                    ),

                    "ethnicity": row_dict.get(
                        source_table.c.ethnicity
                    ),

                    "two_factor_auth_option": row_dict.get(
                        source_table.c.two_factor_auth
                    ),

                    "job_alerts_email_notification": False,

                    "totp_secret": None,

                    "is_totp_verified": False,

                    "show_student_intro": False,

                    "has_employment_history": False,
                }

                user_profile_insert_data.append(
                    mapped_profile_row
                )

            except Exception as row_error:

                logger.exception(
                    f"Failed processing row: "
                    f"{str(row_error)}"
                )

        # -------------------------------------------------
        # No Data
        # -------------------------------------------------

        if not users_insert_data:

            logger.warning(
                "No users found to migrate."
            )

            return 0

        # -------------------------------------------------
        # Insert Data
        # -------------------------------------------------

        logger.info(
            f"Inserting "
            f"{len(users_insert_data)} users..."
        )

        logger.info(
            f"Inserting "
            f"{len(user_role_insert_data)} user roles..."
        )

        logger.info(
            f"Inserting "
            f"{len(user_profile_insert_data)} profiles..."
        )

        with self.dest_engine.begin() as dest_conn:

            # -------------------------------------------------
            # Insert Users
            # -------------------------------------------------

            dest_conn.execute(
                insert(users_table),
                users_insert_data
            )

            # -------------------------------------------------
            # Update Audit Fields
            # -------------------------------------------------

            logger.info(
                "Updating audit fields..."
            )

            for audit_row in audit_update_rows:

                dest_conn.execute(

                    update(users_table)
                    .where(
                        users_table.c.uuid
                        == audit_row["user_uuid"]
                    )
                    .values(

                        created_by=(
                            audit_row["created_by"]
                        ),

                        updated_by=(
                            audit_row["updated_by"]
                        )
                    )
                )

            # -------------------------------------------------
            # Insert User Roles
            # -------------------------------------------------

            if user_role_insert_data:

                dest_conn.execute(
                    insert(user_role_table),
                    user_role_insert_data
                )

            # -------------------------------------------------
            # Insert User Profiles
            # -------------------------------------------------

            if user_profile_insert_data:

                dest_conn.execute(
                    insert(user_profile_table),
                    user_profile_insert_data
                )

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{len(users_insert_data)} users."
        )

        logger.info(
            "======================================="
        )

        return len(users_insert_data)