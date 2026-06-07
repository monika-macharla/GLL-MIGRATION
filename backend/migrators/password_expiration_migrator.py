import logging

from datetime import datetime

from sqlalchemy import update

from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)


class PasswordExpirationMigrator(BaseMigrator):

    DESTINATION_TABLE = "user_hashed_password"
    DEFAULT_EXPIRES_AT = datetime(2050, 1, 1)

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
            "PASSWORD EXPIRATION MIGRATION STARTED"
        )

        password_table = self._manual_reflect(
            self.DESTINATION_TABLE,
            self.dest_engine,
            self.metadata_dest
        )

        if not password_table.columns:

            raise ValueError(
                "Destination user_hashed_password table not found."
            )

        if "expires_at" not in password_table.c:

            raise ValueError(
                "Destination user_hashed_password.expires_at "
                "column not found."
            )

        expires_at = self.config.get(
            "password_expires_at",
            self.DEFAULT_EXPIRES_AT
        )

        if isinstance(expires_at, str):

            expires_at = datetime.fromisoformat(
                expires_at
            )

        query = (
            update(password_table)
            .where(
                password_table.c.expires_at.is_(None)
                | (password_table.c.expires_at < expires_at)
            )
            .values(
                expires_at=expires_at
            )
        )

        if "deleted_at" in password_table.c:

            query = query.where(
                password_table.c.deleted_at.is_(None)
            )

        with self.dest_engine.begin() as conn:

            result = conn.execute(
                query
            )

        updated_count = result.rowcount or 0

        logger.info(
            f"Updated {updated_count} "
            f"user_hashed_password expires_at values to "
            f"{expires_at}."
        )

        return updated_count
