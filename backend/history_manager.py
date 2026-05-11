from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, JSON, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import os

Base = declarative_base()

class MigrationLog(Base):
    __tablename__ = 'migration_logs'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    source_db = Column(String(255))
    dest_db = Column(String(255))
    source_table = Column(String(255))
    dest_table = Column(String(255))
    status = Column(String(50)) # success, error
    records_inserted = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    mapping_details = Column(JSON, nullable=True)

class HistoryManager:
    def __init__(self, db_path='migration_history.sqlite'):
        self.engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def log_migration(self, source_db, dest_db, source_table, dest_table, status, records=0, error=None, mapping=None):
        session = self.Session()
        log = MigrationLog(
            source_db=source_db,
            dest_db=dest_db,
            source_table=source_table,
            dest_table=dest_table,
            status=status,
            records_inserted=records,
            error_message=error,
            mapping_details=mapping
        )
        session.add(log)
        session.commit()
        session.close()

    def get_history(self, limit=50):
        session = self.Session()
        logs = session.query(MigrationLog).order_by(MigrationLog.timestamp.desc()).limit(limit).all()
        result = []
        for log in logs:
            result.append({
                "id": log.id,
                "timestamp": log.timestamp.isoformat(),
                "source_db": log.source_db,
                "dest_db": log.dest_db,
                "source_table": log.source_table,
                "dest_table": log.dest_table,
                "status": log.status,
                "records": log.records_inserted,
                "error": log.error_message
            })
        session.close()
        return result
