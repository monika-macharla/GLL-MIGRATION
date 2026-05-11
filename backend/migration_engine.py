import yaml
import logging
import urllib.parse
from sqlalchemy import create_engine, Table, MetaData, select, insert, Column, inspect
from sqlalchemy.engine import URL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationEngine:
    def __init__(self, config: str | dict):
        if isinstance(config, str):
            with open(config, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = config
        
        self.source_engine = self._get_engine(self.config['source_db'])
        self.dest_engine = self._get_engine(self.config['destination_db'])
        self.metadata_source = MetaData()
        self.metadata_dest = MetaData()

    def _get_engine(self, db_config: dict):
        """Create a SQLAlchemy engine from config, handling special characters in passwords"""
        if 'url' in db_config:
            return create_engine(db_config['url'])
        
        db_type = db_config.get('type', 'mysql')
        if db_type == 'sqlite':
            return create_engine(URL.create("sqlite", database=db_config['database']))
        
        drivername = "postgresql" if db_type == "postgresql" else "mysql+pymysql"
        default_port = 5432 if db_type == "postgresql" else 3306
        
        url = URL.create(
            drivername=drivername,
            username=db_config.get('username'),
            password=db_config.get('password'),
            host=db_config.get('host', '127.0.0.1'),
            port=db_config.get('port') or default_port,
            database=db_config.get('database')
        )
        return create_engine(url)

    def _manual_reflect(self, table_name, engine, metadata):
        """Reflect a table manually without any Foreign Keys or constraints"""
        if table_name in metadata.tables:
            return metadata.tables[table_name]
            
        inspector = inspect(engine)
        try:
            columns = inspector.get_columns(table_name)
        except:
            return None # Table might not exist
        
        table = Table(table_name, metadata)
        for col in columns:
            # Create a simple column without FKs
            table.append_column(Column(col['name'], col['type']))
            
        return table

    def migrate(self):
        for mapping in self.config['mappings']:
            source_table_name = mapping['source_table']
            dest_table_name = mapping['destination_table']
            column_mapping = mapping['columns']

            logger.info(f"Starting migration: {source_table_name} -> {dest_table_name}")
            
            try:
                self._migrate_table(source_table_name, dest_table_name, column_mapping)
                logger.info(f"Successfully migrated {source_table_name} to {dest_table_name}")
            except Exception as e:
                logger.error(f"Failed to migrate {source_table_name}: {str(e)}")
                raise e

    def _migrate_table(self, source_name: str, dest_name: str, column_mapping: dict) -> int:
        # Manually reflect source table to avoid FK issues
        source_table = self._manual_reflect(source_name, self.source_engine, self.metadata_source)
        if source_table is None:
            raise ValueError(f"Source table {source_name} not found")
        
        # Manually reflect destination table
        dest_table = self._manual_reflect(dest_name, self.dest_engine, self.metadata_dest)
        
        if dest_table is None:
            logger.info(f"Table {dest_name} does not exist in destination. Creating it...")
            # Clone structure
            dest_table = source_table.to_metadata(self.metadata_dest, name=dest_name)
            self.metadata_dest.create_all(self.dest_engine)
        
        # Build selection query
        if not column_mapping:
            logger.info(f"No column mapping provided for {source_name}. Mapping all columns with same names.")
            source_cols = [source_table.c[col.name] for col in source_table.columns]
            # Update column_mapping to be a 1:1 mapping for insertion logic later
            column_mapping = {col.name: col.name for col in source_table.columns}
        else:
            source_cols = [source_table.c[src_col] for src_col in column_mapping.keys()]
            
        if not source_cols:
            raise ValueError(f"No columns found to migrate for table {source_name}")

        query = select(*source_cols)
        if self.config.get('limit'):
            query = query.limit(self.config['limit'])

        with self.source_engine.connect() as source_conn:
            result = source_conn.execute(query)
            rows = result.all()
            
            if not rows:
                logger.info(f"No data found in {source_name}")
                return 0

            # Prepare data for insertion
            insert_data = []
            for row in rows:
                mapped_row = {}
                for src_col, dest_col in column_mapping.items():
                    mapped_row[dest_col] = getattr(row, src_col)
                insert_data.append(mapped_row)

            # Insert into destination
            with self.dest_engine.begin() as dest_conn:
                dest_conn.execute(insert(dest_table), insert_data)
                logger.info(f"Inserted {len(insert_data)} rows into {dest_name}")
                return len(insert_data)

if __name__ == "__main__":
    engine = MigrationEngine('config.yaml')
    engine.migrate()
