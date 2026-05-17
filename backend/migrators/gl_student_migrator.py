import uuid
import logging

from datetime import datetime

from sqlalchemy import (
    select,
    insert
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class GLStudentMigrator(BaseMigrator):

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
    # Normalize
    # -------------------------------------------------

    def normalize(self, value):

        if not value:

            return ""

        return (
            str(value)
            .strip()
            .lower()
        )

    # -------------------------------------------------
    # Main Migration
    # -------------------------------------------------

    def migrate(self) -> int:

        logger.info(
            "======================================="
        )

        logger.info(
            "GL_STUDENT MIGRATION STARTED"
        )

        logger.info(
            "======================================="
        )

        # -------------------------------------------------
        # Reflect Source Tables
        # -------------------------------------------------

        gl_student_table = self._manual_reflect(
            "gl_student",
            self.source_engine,
            self.metadata_source
        )

        address_table = self._manual_reflect(
            "address",
            self.source_engine,
            self.metadata_source
        )

        # -------------------------------------------------
        # Reflect Destination Tables
        # -------------------------------------------------

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
        # Student Role UUID
        # -------------------------------------------------

        student_role_uuid = None

        with self.dest_engine.connect() as conn:

            results = conn.execute(
                select(
                    roles_table.c.uuid,
                    roles_table.c.code
                )
            )

            for row in results:

                row_dict = row._mapping

                role_code = self.normalize(
                    row_dict.get(
                        roles_table.c.code
                    )
                )

                if role_code == "student":

                    student_role_uuid = row_dict.get(
                        roles_table.c.uuid
                    )

                    break

        if not student_role_uuid:

            raise ValueError(
                "Student role not found."
            )

        logger.info(
            f"student_role_uuid="
            f"{student_role_uuid}"
        )

        # -------------------------------------------------
        # Address Lookup
        # -------------------------------------------------

        address_lookup = {}

        with self.source_engine.connect() as conn:

            results = conn.execute(
                select(address_table)
            )

            for row in results:

                row_dict = row._mapping

                address_lookup[
                    row_dict.get(
                        address_table.c.id
                    )
                ] = {

                    "address_line_1":
                        row_dict.get(
                            address_table.c.address_line_1
                        ),

                    "address_line_2":
                        row_dict.get(
                            address_table.c.address_line_2
                        ),

                    "city":
                        row_dict.get(
                            address_table.c.city
                        ),

                    "state":
                        row_dict.get(
                            address_table.c.state
                        ),

                    "zip_code":
                        row_dict.get(
                            address_table.c.zip_code
                        ),

                    "country":
                        row_dict.get(
                            address_table.c.country
                        )
                }

        logger.info(
            f"Loaded "
            f"{len(address_lookup)} "
            f"addresses."
        )

        # -------------------------------------------------
        # Insert Data
        # -------------------------------------------------

        users_insert_data = []

        user_role_insert_data = []

        user_profile_insert_data = []

        skipped_students = 0

        # -------------------------------------------------
        # Load Students
        # -------------------------------------------------

        with self.source_engine.connect() as conn:

            results = conn.execute(

                select(gl_student_table)
            )

            for row in results:

                try:

                    row_dict = row._mapping

                    student_id = row_dict.get(
                        gl_student_table.c.id
                    )

                    generated_uuid = str(
                        uuid.uuid4()
                    )

                    # -------------------------------------------------
                    # Email / Username
                    # -------------------------------------------------

                    email = row_dict.get(
                        gl_student_table.c.email
                    )

                    # -------------------------------------------------
                    # Fallback to school_student_id
                    # -------------------------------------------------

                    if not email:

                        school_student_id = row_dict.get(
                            gl_student_table.c.school_student_id
                        )

                        if school_student_id:

                            email = str(
                                school_student_id
                            )

                    # -------------------------------------------------
                    # Fallback to source id
                    # -------------------------------------------------

                    if not email:

                        source_student_id = row_dict.get(
                            gl_student_table.c.id
                        )

                        if source_student_id:

                            email = str(
                                source_student_id
                            )

                    # -------------------------------------------------
                    # Final Validation
                    # -------------------------------------------------

                    if not email:

                        skipped_students += 1

                        logger.warning(
                            f"Skipping student "
                            f"without usable identifier: "
                            f"{student_id}"
                        )

                        continue

                    email = str(email).strip()

                    logger.info(
                        f"Migrating student: "
                        f"{email}"
                    )

                    # -------------------------------------------------
                    # Dates
                    # -------------------------------------------------

                    created_date = row_dict.get(
                        gl_student_table.c.created_date
                    )

                    updated_date = row_dict.get(
                        gl_student_table.c.last_modified_date
                    )

                    # -------------------------------------------------
                    # Fix NULL Dates
                    # -------------------------------------------------

                    if not created_date:

                        created_date = (
                            datetime.utcnow()
                        )

                    if not updated_date:

                        updated_date = (
                            created_date
                        )

                    # -------------------------------------------------
                    # USERS
                    # -------------------------------------------------

                    users_insert_data.append({

                        "uuid":
                            generated_uuid,

                        "created_at":
                            created_date,

                        "updated_at":
                            updated_date,

                        "deleted_at":
                            None,

                        "user_name":
                            email,

                        "email":
                            email,

                        "status":
                            1,

                        "active_role_uuid":
                            student_role_uuid,

                        "created_by":
                            None,

                        "updated_by":
                            None,
                    })

                    # -------------------------------------------------
                    # USER ROLE
                    # -------------------------------------------------

                    user_role_insert_data.append({

                        "uuid":
                            str(uuid.uuid4()),

                        "created_at":
                            created_date,

                        "updated_at":
                            updated_date,

                        "deleted_at":
                            None,

                        "user_uuid":
                            generated_uuid,

                        "role":
                            student_role_uuid,

                        "created_by":
                            None,
                    })

                    # -------------------------------------------------
                    # Gender
                    # -------------------------------------------------

                    source_gender = row_dict.get(
                        gl_student_table.c.gender
                    )

                    mapped_gender = (
                        self.GENDER_MAPPING.get(
                            self.normalize(
                                source_gender
                            ),
                            0
                        )
                    )

                    # -------------------------------------------------
                    # Address
                    # -------------------------------------------------

                    address_data = (
                        address_lookup.get(
                            row_dict.get(
                                gl_student_table.c.address_id
                            ),
                            {}
                        )
                    )

                    # -------------------------------------------------
                    # USER PROFILE
                    # -------------------------------------------------

                    user_profile_insert_data.append({

                        "uuid":
                            str(uuid.uuid4()),

                        "created_at":
                            created_date,

                        "updated_at":
                            updated_date,

                        "deleted_at":
                            None,

                        "profile_pic":
                            None,

                        "dob":
                            row_dict.get(
                                gl_student_table.c.date_of_birth
                            ),

                        "gender":
                            mapped_gender,

                        "prefix":
                            None,

                        "first_name":
                            row_dict.get(
                                gl_student_table.c.first_name
                            ),

                        "middle_name":
                            row_dict.get(
                                gl_student_table.c.middle_name
                            ),

                        "last_name":
                            row_dict.get(
                                gl_student_table.c.last_name
                            ),

                        "suffix":
                            None,

                        "address_line1":
                            address_data.get(
                                "address_line_1"
                            ),

                        "address_line2":
                            address_data.get(
                                "address_line_2"
                            ),

                        "city":
                            address_data.get(
                                "city"
                            ),

                        "state":
                            address_data.get(
                                "state"
                            ),

                        "zip_code":
                            address_data.get(
                                "zip_code"
                            ),

                        "country_code":
                            address_data.get(
                                "country"
                            ),

                        "created_by":
                            None,

                        "updated_by":
                            None,

                        "user_uuid":
                            generated_uuid,

                        "phone_number":
                            row_dict.get(
                                gl_student_table.c.phone_no
                            ),

                        "country":
                            address_data.get(
                                "country"
                            ),

                        "ssn_number":
                            row_dict.get(
                                gl_student_table.c.last4_ssn
                            ),

                        "ethnicity":
                            row_dict.get(
                                gl_student_table.c.person_ethnics
                            ),

                        "two_factor_auth_option":
                            False,

                        "job_alerts_email_notification":
                            False,

                        "totp_secret":
                            None,

                        "is_totp_verified":
                            False,

                        "show_student_intro":
                            False,

                        "has_employment_history":
                            False,
                    })

                except Exception as row_error:

                    logger.exception(
                        f"Failed processing row: "
                        f"{str(row_error)}"
                    )

        # -------------------------------------------------
        # Insert
        # -------------------------------------------------

        logger.info(
            f"Inserting "
            f"{len(users_insert_data)} "
            f"users..."
        )

        logger.info(
            f"Inserting "
            f"{len(user_role_insert_data)} "
            f"user roles..."
        )

        logger.info(
            f"Inserting "
            f"{len(user_profile_insert_data)} "
            f"user profiles..."
        )

        with self.dest_engine.begin() as conn:

            if users_insert_data:

                conn.execute(
                    insert(users_table),
                    users_insert_data
                )

            if user_role_insert_data:

                conn.execute(
                    insert(user_role_table),
                    user_role_insert_data
                )

            if user_profile_insert_data:

                conn.execute(
                    insert(user_profile_table),
                    user_profile_insert_data
                )

        logger.info(
            "======================================="
        )

        logger.info(
            f"Successfully migrated "
            f"{len(users_insert_data)} "
            f"students."
        )

        logger.info(
            f"Skipped students: "
            f"{skipped_students}"
        )

        logger.info(
            "======================================="
        )

        return len(users_insert_data)