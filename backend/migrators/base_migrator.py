import logging
from sqlalchemy import MetaData, Table, Column, inspect

logger = logging.getLogger(__name__)

class BaseMigrator:
    def __init__(self, engine, source_engine, dest_engine, storage):
        self.engine = engine # The main GLLMigrationEngine instance
        self.source_engine = source_engine
        self.dest_engine = dest_engine
        self.storage = storage
        self.metadata_source = MetaData()
        self.metadata_dest = MetaData()

    def _manual_reflect(self, table_name, engine, metadata):
        """Reflect a table manually without any Foreign Keys or constraints"""
        if table_name in metadata.tables:
            return metadata.tables[table_name]
            
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        if not columns:
            logger.warning(f"No columns found for table {table_name}. Table might not exist or is empty.")
        else:
            logger.info(f"Reflected {len(columns)} columns for table {table_name}")
        
        table = Table(table_name, metadata)
        for col in columns:
            table.append_column(Column(col['name'], col['type']))
            
        return table

    def migrate(self) -> int:
        raise NotImplementedError("Subclasses must implement migrate()")
