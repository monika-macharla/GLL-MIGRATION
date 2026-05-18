import logging

from sqlalchemy import (

    MetaData,

    Table,

    Column,

    inspect,

    select
)

logger = logging.getLogger(__name__)


class BaseMigrator:

    def __init__(
        self,
        engine,
        source_engine,
        dest_engine,
        storage
    ):

        # -----------------------------------------
        # Main Migration Engine
        # -----------------------------------------

        self.engine = engine

        # -----------------------------------------
        # Source DB Engine
        # -----------------------------------------

        self.source_engine = source_engine

        # -----------------------------------------
        # Destination DB Engine
        # -----------------------------------------

        self.dest_engine = dest_engine

        # -----------------------------------------
        # Storage Service
        # -----------------------------------------

        self.storage = storage

        # -----------------------------------------
        # Metadata
        # -----------------------------------------

        self.metadata_source = MetaData()

        self.metadata_dest = MetaData()

        logger.info(
            "BaseMigrator initialized"
        )

    # -----------------------------------------
    # MANUAL TABLE REFLECTION
    # -----------------------------------------

    def _manual_reflect(
        self,
        table_name,
        engine,
        metadata
    ):
        """
        Reflect a table manually
        without Foreign Keys
        or constraints
        """

        logger.info(
            f"Reflecting table: "
            f"{table_name}"
        )

        # -----------------------------------------
        # Return Cached Table
        # -----------------------------------------

        if table_name in metadata.tables:

            logger.info(
                f"Using cached metadata "
                f"for table: "
                f"{table_name}"
            )

            return metadata.tables[
                table_name
            ]

        # -----------------------------------------
        # Inspect DB
        # -----------------------------------------

        inspector = inspect(engine)

        columns = inspector.get_columns(
            table_name
        )

        # -----------------------------------------
        # No Columns
        # -----------------------------------------

        if not columns:

            logger.warning(
                f"No columns found "
                f"for table "
                f"{table_name}. "
                f"Table may not exist "
                f"or may be empty."
            )

        else:

            logger.info(
                f"Reflected "
                f"{len(columns)} columns "
                f"for table "
                f"{table_name}"
            )

        # -----------------------------------------
        # Create SQLAlchemy Table
        # -----------------------------------------

        table = Table(
            table_name,
            metadata
        )

        # -----------------------------------------
        # Append Columns
        # -----------------------------------------

        for col in columns:

            logger.info(
                f"Adding column: "
                f"{col['name']}"
            )

            table.append_column(

                Column(

                    col['name'],

                    col['type']
                )
            )

        logger.info(
            f"Successfully reflected "
            f"table: {table_name}"
        )

        return table

    # -----------------------------------------
    # GENERIC FETCH HELPER
    # -----------------------------------------

    def fetch_one_by_column(
        self,
        engine,
        table_name,
        where_column,
        where_value
    ):
        """
        Generic reusable helper
        to fetch one row from
        any table from any DB
        """

        try:

            logger.info(
                f"Fetching from table "
                f"{table_name} "
                f"where "
                f"{where_column}="
                f"{where_value}"
            )

            metadata = MetaData()

            table = self._manual_reflect(

                table_name,

                engine,

                metadata
            )

            with engine.connect() as conn:

                query = select(table).where(

                    getattr(
                        table.c,
                        where_column
                    ) == where_value
                )

                logger.info(
                    f"Executing query on "
                    f"{table_name}"
                )

                result = conn.execute(
                    query
                ).fetchone()

                # -----------------------------------------
                # Record Found
                # -----------------------------------------

                if result:

                    logger.info(
                        f"Record found in "
                        f"{table_name}"
                    )

                    return result._mapping

                # -----------------------------------------
                # No Record Found
                # -----------------------------------------

                logger.warning(
                    f"No matching record "
                    f"found in "
                    f"{table_name}"
                )

                return None

        except Exception as e:

            logger.exception(
                f"Failed fetching "
                f"from table "
                f"{table_name}: {e}"
            )

            return None

    # -----------------------------------------
    # GET LOOKUP ENGINE
    # -----------------------------------------

    def get_lookup_engine(
        self,
        lookup_name
    ):
        """
        Fetch reusable
        lookup DB engine
        """

        logger.info(
            f"Fetching lookup engine: "
            f"{lookup_name}"
        )

        engine = self.engine.lookup_engines.get(
            lookup_name
        )

        if engine:

            logger.info(
                f"Lookup engine found: "
                f"{lookup_name}"
            )

            return engine

        logger.warning(
            f"Lookup engine not found: "
            f"{lookup_name}"
        )

        return None

    # -----------------------------------------
    # MIGRATE METHOD
    # -----------------------------------------

    def migrate(self) -> int:

        raise NotImplementedError(
            "Subclasses must "
            "implement migrate()"
        )