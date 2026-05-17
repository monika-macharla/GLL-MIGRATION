import uuid
import logging
import mimetypes
from urllib.parse import urlparse
from pathlib import Path

from sqlalchemy import select, insert
from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class DigitalBadgesMigrator(BaseMigrator):
    def __init__(self, engine, source_engine, dest_engine, storage, config):
        super().__init__(engine, source_engine, dest_engine, storage)
        self.config = config

    def migrate(self) -> int:
        logger.info("Starting Digital Badges Migration...")

        # Source table
        badge_table = self._manual_reflect(
            'badge',
            self.source_engine,
            self.metadata_source
        )

        # Destination table
        dest_table = self._manual_reflect(
            'credentials_digital_badges',
            self.dest_engine,
            self.metadata_dest
        )

        if not dest_table.columns:
            logger.error(
                "Destination table 'credentials_digital_badges' not found"
            )
            raise ValueError(
                "Destination table 'credentials_digital_badges' not found"
            )

        # ==========================================
        # INSERT ONLY 5 RECORDS FOR TESTING
        # ==========================================
        query = select(badge_table).limit(5)

        with self.source_engine.connect() as source_conn:
            results = source_conn.execute(query)
            rows = results.fetchall()

            if not rows:
                logger.info("No badge records found")
                return 0

            logger.info(f"Found {len(rows)} badge records")

            insert_data = []

            for row in rows:
                try:
                    row_dict = row._mapping

                    image_path = row_dict.get(
                        badge_table.c.image
                    )

                    file_name = self._extract_file_name(
                        image_path
                    )

                    file_type = self._get_file_type(
                        file_name
                    )

                    created_at = row_dict.get(
                        badge_table.c.issued_on
                    )

                    mapped_row = {
                        # Auto generated UUID
                        'uuid': str(uuid.uuid4()),

                        # issued_on -> created_at
                        'created_at': created_at,

                        # issued_on -> updated_at
                        'updated_at': created_at,

                        # image -> file_path
                        'file_path': image_path,

                        # extracted from image
                        'file_name': file_name,

                        # detected from extension
                        'file_type': file_type,

                        # assertion_json -> badge_json
                        'badge_json': row_dict.get(
                            badge_table.c.assertion_json
                        ),

                        # default keep as 2
                        'status': 2,
                    }

                    insert_data.append(mapped_row)

                except Exception as e:
                    logger.exception(
                        f"Failed processing badge row: {e}"
                    )

            if insert_data:
                with self.dest_engine.begin() as dest_conn:
                    dest_conn.execute(
                        insert(dest_table),
                        insert_data
                    )

                logger.info(
                    f"Successfully migrated "
                    f"{len(insert_data)} digital badges"
                )

                return len(insert_data)

            return 0

    def _extract_file_name(self, file_path: str) -> str:
        """
        Extract filename from URL/path
        """

        if not file_path:
            return None

        try:
            parsed = urlparse(file_path)

            filename = Path(parsed.path).name

            if filename:
                return filename

            return Path(file_path).name

        except Exception:
            return None

    def _get_file_type(self, file_name: str) -> str:
        """
        Detect MIME type from filename extension
        """

        if not file_name:
            return 'application/octet-stream'

        ext = Path(file_name).suffix.lower()

        mime_mapping = {
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.json': 'application/json',
            '.pdf': 'application/pdf',
        }

        if ext in mime_mapping:
            return mime_mapping[ext]

        mime_type, _ = mimetypes.guess_type(file_name)

        return mime_type or 'application/octet-stream'