import logging
import uuid

from datetime import datetime

from sqlalchemy import (
    insert,
    select,
    text,
    update
)

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class InstitutionSftpCredentialsMigrator(BaseMigrator):

    SOURCE_TABLE = "esc_sftp_user"
    SOURCE_PUBLIC_KEY_TABLE = "esc_sftp_user_public_key"
    DESTINATION_TABLE = "institution_sftp_credentials"
    INSERT_CHUNK_SIZE = 10000
    UUID_NAMESPACE = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "gll-migration:institution_sftp_credentials"
    )
    FOLDER_NAME_OVERRIDES = {
        "dallas college": "dallascollege/users/dccollege/prod",
        "richardson isd": "richardsonisd",
        "dallas isd": "disd",
        "garland isd": "gisd",
        "arlington isd": "arlingtonisd",
        "sunnyvale isd": "region10esc/sunnyvaleisd",
        "life school": "lifeschool",
        "twu": "twuedu",
        "era isd": "era",
        "callisburg isd": "callisburg",
        "dublin isd": "region11esc/dublinisd",
        "dublinisd": "region11esc/dublinisd",
        "nocona isd": "region9esc/noconaisd",
        "psp cisd": "region16esc/PSPisd",
        "bandera isd": "banderaisd",
        "valley view isd": "Valleyview",
        "muenster isd": "muenster",
        "gainesville isd": "Gainesville",
        "pine tree isd": "pinetreeisd",
        "farwell isd": "region16esc/Farwellisd",
        "unt dallas": "untd",
    }
    FOLDER_ONLY_CREDENTIALS = [
        ("Dallas College", "dallascollege/users/dccollege/prod"),
        ("Richardson ISD", "richardsonisd"),
        ("Dallas ISD", "disd"),
        ("Garland ISD", "gisd"),
        ("Arlington ISD", "arlingtonisd"),
        ("Sunnyvale ISD", "region10esc/sunnyvaleisd"),
        ("Life School", "lifeschool"),
        ("TWU", "twuedu"),
        ("Era ISD", "era"),
        ("Callisburg ISD", "callisburg"),
        ("Dublin ISD", "region11esc/dublinisd"),
        ("Nocona ISD", "region9esc/noconaisd"),
        ("PSP CISD", "region16esc/PSPisd"),
        ("Bandera ISD", "banderaisd"),
        ("Valley View ISD", "Valleyview"),
        ("Muenster ISD", "muenster"),
        ("Gainesville ISD", "Gainesville"),
        ("Pine Tree ISD", "pinetreeisd"),
        ("Farwell ISD", "region16esc/Farwellisd"),
        ("UNT Dallas", "untd"),
    ]
    INSTITUTION_LOOKUP_ALIASES = {
        "twu": [
            "texas woman's university"
        ],
        "valley view isd": [
            "valley view isd - valley view"
        ],
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
        self._institution_lookup = {}

    def migrate(self) -> int:

        logger.info(
            "Starting Institution SFTP Credentials Migration..."
        )

        sftp_user_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        public_key_table = self._manual_reflect(
            self.SOURCE_PUBLIC_KEY_TABLE,
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        institutions_table = self._manual_reflect(
            "institutions",
            self.dest_engine,
            self.metadata_dest
        )

        logger.info(
            f"esc_sftp_user source columns: "
            f"{sftp_user_table.columns.keys()}"
        )

        logger.info(
            f"esc_sftp_user_public_key source columns: "
            f"{public_key_table.columns.keys()}"
        )

        logger.info(
            f"institution_sftp_credentials destination columns: "
            f"{destination_table.columns.keys()}"
        )

        self._ensure_destination_columns(
            destination_table,
            public_key_table
        )

        self._institution_lookup = self._build_institution_lookup(
            institutions_table
        )

        existing_uuids = self._load_existing_uuids(
            destination_table
        )

        inserted_count = 0
        updated_count = 0
        prepared_count = 0
        fetched_count = 0
        skipped_count = 0
        skipped_missing_institution = 0
        skipped_missing_public_key = 0
        row_error_count = 0

        last_public_key_id = 0
        remaining_limit = self.config.get(
            "limit"
        )

        if remaining_limit:

            remaining_limit = int(
                remaining_limit
            )

        while True:

            fetch_size = self.INSERT_CHUNK_SIZE

            if remaining_limit is not None:

                if remaining_limit <= 0:

                    break

                fetch_size = min(
                    fetch_size,
                    remaining_limit
                )

            query = (
                select(
                    sftp_user_table,
                    public_key_table
                )
                .select_from(
                    sftp_user_table.join(
                        public_key_table,
                        public_key_table.c.sftp_user_id
                        == sftp_user_table.c.id
                    )
                )
                .where(
                    public_key_table.c.id > last_public_key_id
                )
                .order_by(
                    public_key_table.c.id
                )
                .limit(
                    fetch_size
                )
            )

            with self.source_engine.connect() as source_conn:

                rows = source_conn.execute(
                    query
                ).fetchall()

            if not rows:

                break

            fetched_count += len(
                rows
            )

            if remaining_limit is not None:

                remaining_limit -= len(
                    rows
                )

            insert_data = []
            update_data = []

            for row in rows:

                try:

                    row_dict = row._mapping

                    public_key_id = self._get_source_value(
                        row_dict,
                        public_key_table,
                        "id"
                    )

                    last_public_key_id = public_key_id

                    destination_uuid = self._make_destination_uuid(
                        public_key_id
                    )

                    public_key = self._clean_value(
                        self._get_source_value(
                            row_dict,
                            public_key_table,
                            "public_key"
                        )
                    )

                    if not public_key:

                        skipped_count += 1
                        skipped_missing_public_key += 1

                        continue

                    institution_name = self._clean_value(
                        self._get_source_value(
                            row_dict,
                            sftp_user_table,
                            "institution_name"
                        )
                    )

                    institution_uuid = self._lookup_institution_uuid(
                        institution_name
                    )

                    if not institution_uuid:

                        skipped_count += 1
                        skipped_missing_institution += 1

                        logger.warning(
                            "No destination institution found for SFTP "
                            f"institution_name={institution_name!r}; "
                            f"source_sftp_user_id="
                            f"{self._get_source_value(row_dict, sftp_user_table, 'id')}"
                        )

                        continue

                    created_at = (
                        self._get_source_value(
                            row_dict,
                            sftp_user_table,
                            "created_date"
                        )
                        or
                        self._get_source_value(
                            row_dict,
                            public_key_table,
                            "created_date"
                        )
                        or
                        datetime.utcnow()
                    )

                    updated_at = (
                        self._get_source_value(
                            row_dict,
                            sftp_user_table,
                            "last_modified_date"
                        )
                        or
                        self._get_source_value(
                            row_dict,
                            public_key_table,
                            "last_modified_date"
                        )
                        or
                        created_at
                    )

                    deleted_at = None

                    if (
                        not self._to_bool(
                            self._get_source_value(
                                row_dict,
                                sftp_user_table,
                                "active"
                            )
                        )
                        or
                        not self._to_bool(
                            self._get_source_value(
                                row_dict,
                                public_key_table,
                                "active"
                            )
                        )
                    ):

                        deleted_at = updated_at

                    username = self._clean_value(
                        self._get_source_value(
                            row_dict,
                            sftp_user_table,
                            "username"
                        )
                    ) or ""

                    mapped_row = {
                        "uuid": destination_uuid,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "deleted_at": deleted_at,
                        "institution_uuid": institution_uuid,
                        "username": username,
                        "password": public_key,
                        "folder_name": self._folder_name(
                            institution_name,
                            username
                        ),
                    }

                    filtered_row = self._filter_to_table_columns(
                        mapped_row,
                        destination_table
                    )

                    if destination_uuid in existing_uuids:

                        update_data.append(
                            filtered_row
                        )

                    else:

                        insert_data.append(
                            filtered_row
                        )

                        existing_uuids.add(
                            destination_uuid
                        )

                except Exception as error:

                    skipped_count += 1
                    row_error_count += 1

                    logger.exception(
                        "Failed processing institution SFTP public key id "
                        f"{last_public_key_id}: {error}"
                    )

            if not insert_data and not update_data:

                logger.info(
                    "Institution SFTP chunk fetched "
                    f"{len(rows)} rows through public key id "
                    f"{last_public_key_id}; nothing to insert or update."
                )

                continue

            logger.info(
                "Saving institution SFTP credentials chunk: "
                f"inserts={len(insert_data)}, "
                f"updates={len(update_data)}, "
                f"public_key_id_through={last_public_key_id}, "
                f"total_fetched={fetched_count}"
            )

            with self.dest_engine.begin() as dest_conn:

                inserted_now = 0

                if insert_data:

                    result = dest_conn.execute(
                        insert(destination_table),
                        insert_data
                    )

                    inserted_now = result.rowcount or len(
                        insert_data
                    )

                updated_now = 0

                for row in update_data:

                    row_uuid = row.get(
                        "uuid"
                    )

                    update_values = {
                        column_name: value
                        for column_name, value in row.items()
                        if column_name != "uuid"
                    }

                    result = dest_conn.execute(
                        update(destination_table)
                        .where(
                            destination_table.c.uuid
                            == row_uuid
                        )
                        .values(
                            **update_values
                        )
                    )

                    updated_now += result.rowcount or 0

            inserted_count += inserted_now
            updated_count += updated_now
            prepared_count += len(
                insert_data
            )
            prepared_count += len(
                update_data
            )

            logger.info(
                "Institution SFTP credentials chunk saved: "
                f"inserted_now={inserted_now}, "
                f"updated_now={updated_now}, "
                f"inserted_total={inserted_count}, "
                f"updated_total={updated_count}"
            )

        (
            folder_inserted_count,
            folder_updated_count,
            folder_skipped_missing_institution
        ) = self._upsert_folder_only_credentials(
            destination_table,
            existing_uuids
        )

        inserted_count += folder_inserted_count
        updated_count += folder_updated_count
        prepared_count += (
            folder_inserted_count
            +
            folder_updated_count
        )
        skipped_count += folder_skipped_missing_institution
        skipped_missing_institution += (
            folder_skipped_missing_institution
        )

        logger.info(
            "Institution SFTP Credentials Migration summary: "
            f"inserted={inserted_count}, "
            f"updated={updated_count}, "
            f"skipped={skipped_count}, "
            f"prepared={prepared_count}, "
            f"fetched={fetched_count}, "
            f"skipped_missing_institution={skipped_missing_institution}, "
            f"skipped_missing_public_key={skipped_missing_public_key}, "
            f"row_errors={row_error_count}"
        )

        return inserted_count + updated_count

    def _build_institution_lookup(
        self,
        institutions_table
    ):

        lookup = {}

        selected_columns = [
            institutions_table.c.uuid,
            institutions_table.c.name
        ]

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    *selected_columns
                )
            ).fetchall()

        for row in rows:

            row_map = row._mapping

            name = row_map.get(
                institutions_table.c.name
            )

            institution_uuid = row_map.get(
                institutions_table.c.uuid
            )

            if not name or not institution_uuid:

                continue

            lookup[
                self._normalize_name(name)
            ] = institution_uuid

            lookup[
                self._compact_name(name)
            ] = institution_uuid

        logger.info(
            f"Built {len(lookup)} institution SFTP institution lookups"
        )

        return lookup

    def _ensure_destination_columns(
        self,
        destination_table,
        public_key_table
    ):

        if "password" not in destination_table.c:

            raise ValueError(
                "Destination table institution_sftp_credentials is missing "
                "required column 'password' for public key migration."
            )

        if "username" not in destination_table.c:

            raise ValueError(
                "Destination table institution_sftp_credentials is missing "
                "required column 'username'."
            )

        max_length = getattr(
            destination_table.c.password.type,
            "length",
            None
        )

        longest_public_key = 0

        if max_length:

            with self.source_engine.connect() as conn:

                rows = conn.execute(
                    select(
                        public_key_table.c.public_key
                    )
                ).fetchall()

            longest_public_key = max(
                (
                    len(
                        str(
                            row._mapping.get(
                                public_key_table.c.public_key
                            ) or
                            ""
                        )
                    )
                    for row in rows
                ),
                default=0
            )

        if max_length and longest_public_key > max_length:

            logger.warning(
                "Widening institution_sftp_credentials.password from "
                f"length {max_length} to TEXT so public keys up to "
                f"{longest_public_key} characters can be migrated accurately."
            )

            with self.dest_engine.begin() as conn:

                conn.execute(
                    text(
                        "ALTER TABLE "
                        f"`{destination_table.name}` "
                        "MODIFY COLUMN `password` TEXT NOT NULL"
                    )
                )

        logger.info(
            "Allowing NULL username/password values for folder-only "
            "institution SFTP credential rows."
        )

        with self.dest_engine.begin() as conn:

            conn.execute(
                text(
                    "ALTER TABLE "
                    f"`{destination_table.name}` "
                    "MODIFY COLUMN `username` VARCHAR(255) NULL"
                )
            )

            conn.execute(
                text(
                    "ALTER TABLE "
                    f"`{destination_table.name}` "
                    "MODIFY COLUMN `password` TEXT NULL"
                )
            )

    def _upsert_folder_only_credentials(
        self,
        destination_table,
        existing_uuids
    ):

        now = datetime.utcnow()
        insert_data = []
        update_data = []
        skipped_missing_institution = 0

        for institution_name, folder_name in self.FOLDER_ONLY_CREDENTIALS:

            institution_uuid = self._lookup_institution_uuid(
                institution_name
            )

            if not institution_uuid:

                skipped_missing_institution += 1

                logger.warning(
                    "No destination institution found for folder-only SFTP "
                    f"institution_name={institution_name!r}; "
                    f"folder_name={folder_name!r}"
                )

                continue

            destination_uuid = self._make_folder_only_uuid(
                institution_name,
                folder_name
            )

            mapped_row = self._filter_to_table_columns(
                {
                    "uuid": destination_uuid,
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                    "institution_uuid": institution_uuid,
                    "username": None,
                    "password": None,
                    "folder_name": folder_name[:255],
                },
                destination_table
            )

            if destination_uuid in existing_uuids:

                update_data.append(
                    mapped_row
                )

            else:

                insert_data.append(
                    mapped_row
                )

                existing_uuids.add(
                    destination_uuid
                )

        with self.dest_engine.begin() as dest_conn:

            inserted_count = 0

            if insert_data:

                result = dest_conn.execute(
                    insert(destination_table),
                    insert_data
                )

                inserted_count = result.rowcount or len(
                    insert_data
                )

            updated_count = 0

            for row in update_data:

                row_uuid = row.get(
                    "uuid"
                )

                update_values = {
                    column_name: value
                    for column_name, value in row.items()
                    if column_name != "uuid"
                }

                result = dest_conn.execute(
                    update(destination_table)
                    .where(
                        destination_table.c.uuid
                        == row_uuid
                    )
                    .values(
                        **update_values
                    )
                )

                updated_count += result.rowcount or 0

        logger.info(
            "Folder-only institution SFTP credentials saved: "
            f"inserted={inserted_count}, "
            f"updated={updated_count}, "
            f"skipped_missing_institution={skipped_missing_institution}"
        )

        return (
            inserted_count,
            updated_count,
            skipped_missing_institution
        )

    def _load_existing_uuids(
        self,
        destination_table
    ):

        existing_uuids = set()

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                select(
                    destination_table.c.uuid
                )
            ).fetchall()

        for row in rows:

            credential_uuid = row._mapping.get(
                destination_table.c.uuid
            )

            if credential_uuid:

                existing_uuids.add(
                    credential_uuid
                )

        logger.info(
            "Loaded "
            f"{len(existing_uuids)} existing institution_sftp_credentials "
            "uuid values for idempotent reruns."
        )

        return existing_uuids

    def _lookup_institution_uuid(
        self,
        institution_name
    ):

        lookup_keys = [
            self._normalize_name(
                institution_name
            ),
            self._compact_name(
                institution_name
            )
        ]

        for key in list(
            lookup_keys
        ):

            lookup_keys.extend(
                self.INSTITUTION_LOOKUP_ALIASES.get(
                    key,
                    []
                )
            )

        for key in lookup_keys:

            if key in self._institution_lookup:

                return self._institution_lookup[
                    key
                ]

        return None

    def _make_folder_only_uuid(
        self,
        institution_name,
        folder_name
    ):

        return str(
            uuid.uuid5(
                self.UUID_NAMESPACE,
                "folder-only:"
                f"{self._normalize_name(institution_name)}:"
                f"{folder_name}"
            )
        )

    def _folder_name(
        self,
        institution_name,
        username
    ):

        for key in [
            self._normalize_name(
                institution_name
            ),
            self._compact_name(
                institution_name
            )
        ]:

            if key in self.FOLDER_NAME_OVERRIDES:

                return self.FOLDER_NAME_OVERRIDES[
                    key
                ][:255]

        return str(
            username
            or
            ""
        )[:255]

    def _make_destination_uuid(
        self,
        public_key_id
    ):

        return str(
            uuid.uuid5(
                self.UUID_NAMESPACE,
                str(public_key_id)
            )
        )

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

    def _to_bool(
        self,
        value
    ):

        if isinstance(value, bytes):

            return value not in [
                b"",
                b"\x00"
            ]

        return bool(
            value
        )

    def _clean_value(
        self,
        value
    ):

        if isinstance(value, str) and value.strip().lower() in [
            "null",
            "none",
            ""
        ]:

            return None

        return value

    def _normalize_name(
        self,
        value
    ):

        if value is None:

            return ""

        return str(
            value
        ).strip().lower()

    def _compact_name(
        self,
        value
    ):

        return "".join(
            self._normalize_name(
                value
            ).split()
        )
