import logging
import urllib.parse
from sqlalchemy import create_engine, MetaData, inspect, Table, Column
from sqlalchemy.engine import URL
from s3_service import S3StorageService
from migrators import InstitutionMigrator

# Configure logging
logger = logging.getLogger(__name__)

class GLLMigrationEngine:
    def __init__(self, config: dict):
        self.config = config
        self.source_engine = self._get_engine(self.config['source_db'])
        self.dest_engine = self._get_engine(self.config['destination_db'])
        self.metadata_source = MetaData()
        self.metadata_dest = MetaData()
        
        # ID Mappings to track Old ID -> New UUID
        self.id_map = {}
        
        # Initialize storage service
        self.storage = S3StorageService()
        
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

    def migrate(self):
        logger.info("Starting GLL Migration...")
        
        total_migrated = 0
        
        # Initialize migrators
        migrators = [
            InstitutionMigrator(self, self.source_engine, self.dest_engine, self.storage, self.config)
        ]
        
        for migrator in migrators:
            total_migrated += migrator.migrate()
            
        return total_migrated
