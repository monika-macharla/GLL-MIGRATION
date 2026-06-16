import base64
import binascii
import logging
import os
import re

from datetime import datetime

import boto3

from botocore.exceptions import BotoCoreError, ClientError

from sqlalchemy import select, update

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class BaseInstitutionImageMigrator(BaseMigrator):

    SOURCE_TABLE = "institution"
    DESTINATION_TABLE = "institutions"
    DEFAULT_BATCH_SIZE = 500
    IMAGE_LABEL = "image"
    SOURCE_IMAGE_COLUMN = None
    DESTINATION_IMAGE_COLUMN = None
    S3_SUBFOLDER = None
    CONFIG_PREFIX = "institution_image"
    DEFAULT_S3_PREFIX = "institution-logos"

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
        self.s3_config = (
            config.get(f"{self.CONFIG_PREFIX}_s3")
            or
            config.get("institution_image_s3")
            or
            config.get("institution_logo_s3")
            or
            {}
        )
        self.bucket_name = (
            self.s3_config.get("bucket")
            or
            os.getenv("INSTITUTION_IMAGE_S3_BUCKET")
            or
            os.getenv("INSTITUTION_LOGO_S3_BUCKET")
            or
            os.getenv("AWS_S3_BUCKET")
        )
        self.region = (
            self.s3_config.get("region")
            or
            os.getenv("INSTITUTION_IMAGE_AWS_REGION")
            or
            os.getenv("INSTITUTION_LOGO_AWS_REGION")
            or
            os.getenv("AWS_REGION")
            or
            "us-west-2"
        )
        self.key_prefix = (
            self.s3_config.get("prefix")
            or
            self.DEFAULT_S3_PREFIX
        ).strip("/")
        self.destination_url_prefix = (
            self.s3_config.get("destination_url_prefix")
            or
            "/uploads"
        ).strip("/")

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=(
                self.s3_config.get("access_key_id")
                or
                os.getenv("INSTITUTION_IMAGE_AWS_ACCESS_KEY_ID")
                or
                os.getenv("INSTITUTION_LOGO_AWS_ACCESS_KEY_ID")
                or
                os.getenv("AWS_ACCESS_KEY_ID")
            ),
            aws_secret_access_key=(
                self.s3_config.get("secret_access_key")
                or
                os.getenv("INSTITUTION_IMAGE_AWS_SECRET_ACCESS_KEY")
                or
                os.getenv("INSTITUTION_LOGO_AWS_SECRET_ACCESS_KEY")
                or
                os.getenv("AWS_SECRET_ACCESS_KEY")
            ),
            region_name=self.region
        )

    def migrate(self) -> int:

        logger.info(
            f"Starting Institution {self.IMAGE_LABEL.title()} "
            "S3 Sync Migration..."
        )

        if not self.bucket_name:

            raise ValueError(
                f"{self.CONFIG_PREFIX}_s3.bucket or AWS_S3_BUCKET is required"
            )

        source_table = self._manual_reflect(
            self.SOURCE_TABLE,
            self.source_engine,
            self.metadata_source
        )

        destination_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        if self.SOURCE_IMAGE_COLUMN not in source_table.c:

            raise ValueError(
                f"Source column {self.SOURCE_IMAGE_COLUMN!r} not found"
            )

        if self.DESTINATION_IMAGE_COLUMN not in destination_table.c:

            raise ValueError(
                f"Destination column {self.DESTINATION_IMAGE_COLUMN!r} not found"
            )

        destination_by_name = self._load_destination_by_name(
            destination_table
        )

        batch_size = self._get_batch_size()
        remaining_limit = self.config.get("limit")
        last_source_id = self._get_start_after_id()

        if remaining_limit is not None:

            remaining_limit = int(
                remaining_limit
            )

        fetched_count = 0
        matched_count = 0
        uploaded_count = 0
        updated_count = 0
        skipped_no_image = 0
        skipped_no_match = 0
        skipped_ambiguous = 0
        skipped_decode_error = 0
        skipped_upload_error = 0
        batch_number = 0

        while True:

            fetch_size = batch_size

            if remaining_limit is not None:

                if remaining_limit <= 0:

                    break

                fetch_size = min(
                    fetch_size,
                    remaining_limit
                )

            query = (
                select(source_table)
                .where(
                    source_table.c.id > last_source_id
                )
                .order_by(
                    source_table.c.id
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

            batch_number += 1
            fetched_count += len(
                rows
            )

            if remaining_limit is not None:

                remaining_limit -= len(
                    rows
                )

            logger.info(
                f"Processing institution {self.IMAGE_LABEL} chunk "
                f"{batch_number}: fetched={len(rows)}, "
                f"source_id_from={rows[0]._mapping.get(source_table.c.id)}, "
                f"source_id_through={rows[-1]._mapping.get(source_table.c.id)}, "
                f"total_fetched={fetched_count}"
            )

            for row in rows:

                row_dict = row._mapping
                source_id = row_dict.get(
                    source_table.c.id
                )
                last_source_id = source_id

                image_value = row_dict.get(
                    source_table.c[self.SOURCE_IMAGE_COLUMN]
                )

                if not image_value:

                    skipped_no_image += 1
                    continue

                source_name = row_dict.get(
                    source_table.c.name
                )
                destination_rows = destination_by_name.get(
                    self._normalize_name(source_name),
                    []
                )

                if not destination_rows:

                    skipped_no_match += 1
                    continue

                if len(destination_rows) > 1:

                    skipped_ambiguous += 1
                    logger.warning(
                        f"Skipping institution {self.IMAGE_LABEL} source id "
                        f"{source_id}: destination name match is ambiguous "
                        f"for {source_name!r}"
                    )
                    continue

                matched_count += 1
                destination_uuid = destination_rows[0]["uuid"]

                try:

                    image_url = self._upload_image(
                        image_value,
                        destination_uuid
                    )

                except ValueError as error:

                    skipped_decode_error += 1
                    logger.warning(
                        f"Skipping institution {self.IMAGE_LABEL} source id "
                        f"{source_id}: {error}"
                    )
                    continue

                except (BotoCoreError, ClientError) as error:

                    skipped_upload_error += 1
                    logger.warning(
                        f"Skipping institution {self.IMAGE_LABEL} source id "
                        f"{source_id}: S3 upload failed: {error}"
                    )
                    continue

                uploaded_count += 1
                update_values = {
                    self.DESTINATION_IMAGE_COLUMN: image_url
                }

                if "updated_at" in destination_table.c:

                    update_values["updated_at"] = datetime.utcnow()

                statement = (
                    update(destination_table)
                    .where(
                        destination_table.c.uuid == destination_uuid
                    )
                    .values(
                        **update_values
                    )
                )

                with self.dest_engine.begin() as dest_conn:

                    result = dest_conn.execute(
                        statement
                    )

                updated_count += result.rowcount or 0

        logger.info(
            f"Institution {self.IMAGE_LABEL.title()} S3 Sync Summary: "
            f"fetched={fetched_count}, "
            f"matched={matched_count}, "
            f"uploaded={uploaded_count}, "
            f"updated={updated_count}, "
            f"skipped_no_image={skipped_no_image}, "
            f"skipped_no_match={skipped_no_match}, "
            f"skipped_ambiguous={skipped_ambiguous}, "
            f"skipped_decode_error={skipped_decode_error}, "
            f"skipped_upload_error={skipped_upload_error}"
        )

        return updated_count

    def _load_destination_by_name(
        self,
        destination_table
    ):

        query = select(
            destination_table.c.uuid,
            destination_table.c.name
        )

        lookup = {}

        with self.dest_engine.connect() as conn:

            rows = conn.execute(
                query
            ).mappings().all()

        for row in rows:

            normalized_name = self._normalize_name(
                row.get("name")
            )

            if not normalized_name:

                continue

            lookup.setdefault(
                normalized_name,
                []
            ).append(row)

        logger.info(
            f"Loaded {len(lookup)} destination institution name lookups"
        )

        return lookup

    def _upload_image(
        self,
        base64_value,
        destination_uuid
    ):

        image_data, extension, content_type = self._decode_image(
            base64_value
        )
        key = (
            f"{self.key_prefix}/"
            f"{self.S3_SUBFOLDER}/"
            f"{destination_uuid}.{extension}"
        )

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=image_data,
            ContentType=content_type
        )

        return f"/{self.destination_url_prefix}/{key}"

    def _decode_image(
        self,
        base64_value
    ):

        if isinstance(base64_value, bytes):

            base64_value = base64_value.decode(
                "utf-8",
                errors="ignore"
            )

        if not isinstance(base64_value, str):

            raise ValueError(
                "image value is not a base64 string"
            )

        value = base64_value.strip()

        if not value:

            raise ValueError(
                "image value is empty"
            )

        content_type = None
        header_match = re.match(
            r"^data:(image/[^;]+);base64,(.*)$",
            value,
            flags=re.IGNORECASE | re.DOTALL
        )

        if header_match:

            content_type = header_match.group(1).lower()
            value = header_match.group(2)
        elif "," in value:

            value = value.split(
                ",",
                1
            )[1]

        try:

            image_data = base64.b64decode(
                value,
                validate=True
            )

        except (binascii.Error, ValueError) as error:

            raise ValueError(
                "failed to decode base64 image"
            ) from error

        detected_content_type, extension = self._detect_image_type(
            image_data,
            content_type
        )

        return image_data, extension, detected_content_type

    def _detect_image_type(
        self,
        image_data,
        content_type
    ):

        signatures = [
            (b"\x89PNG\r\n\x1a\n", "image/png", "png"),
            (b"\xff\xd8\xff", "image/jpeg", "jpg"),
            (b"GIF87a", "image/gif", "gif"),
            (b"GIF89a", "image/gif", "gif"),
            (b"RIFF", "image/webp", "webp"),
        ]

        for signature, detected_type, extension in signatures:

            if image_data.startswith(signature):

                if (
                    detected_type == "image/webp"
                    and
                    image_data[8:12] != b"WEBP"
                ):

                    continue

                return detected_type, extension

        if content_type:

            extension = {
                "image/png": "png",
                "image/jpeg": "jpg",
                "image/jpg": "jpg",
                "image/gif": "gif",
                "image/webp": "webp",
            }.get(
                content_type,
                "bin"
            )

            return content_type, extension

        return "application/octet-stream", "bin"

    def _normalize_name(
        self,
        value
    ):

        if value is None:

            return ""

        return " ".join(
            str(value).strip().lower().split()
        )

    def _get_batch_size(self):

        configured = (
            self.config.get(f"{self.CONFIG_PREFIX}_batch_size")
            or
            self.config.get("institution_image_batch_size")
            or
            self.config.get("institution_logo_batch_size")
            or
            self.config.get("batch_size")
            or
            self.DEFAULT_BATCH_SIZE
        )

        try:

            configured = int(
                configured
            )

        except (TypeError, ValueError):

            configured = self.DEFAULT_BATCH_SIZE

        return max(
            1,
            configured
        )

    def _get_start_after_id(self):

        configured = (
            self.config.get(f"{self.CONFIG_PREFIX}_start_after_id")
            or
            self.config.get("institution_image_start_after_id")
            or
            self.config.get("institution_logo_start_after_id")
            or
            self.config.get("start_after_id")
            or
            0
        )

        try:

            return max(
                0,
                int(configured)
            )

        except (TypeError, ValueError):

            return 0


class InstitutionLogoMigrator(BaseInstitutionImageMigrator):

    IMAGE_LABEL = "logo"
    SOURCE_IMAGE_COLUMN = "logo"
    DESTINATION_IMAGE_COLUMN = "logo"
    S3_SUBFOLDER = "logos"
    CONFIG_PREFIX = "institution_logo"


class InstitutionSealMigrator(BaseInstitutionImageMigrator):

    IMAGE_LABEL = "seal"
    SOURCE_IMAGE_COLUMN = "institution_seal"
    DESTINATION_IMAGE_COLUMN = "seal"
    S3_SUBFOLDER = "seals"
    CONFIG_PREFIX = "institution_seal"
