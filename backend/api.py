from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
from migration_engine import MigrationEngine
from gll_migration_engine import GLLMigrationEngine
from history_manager import HistoryManager
import logging
import urllib.parse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Database Migration API")
history = HistoryManager()

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DBConfig(BaseModel):
    db_type: str  # sqlite, postgresql, mysql
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: str

class ColumnMapping(BaseModel):
    source_col: str
    dest_col: str

class TableMapping(BaseModel):
    source_table: str
    destination_table: str
    columns: Dict[str, str]

class MigrationRequest(BaseModel):
    source: DBConfig
    destination: DBConfig
    mappings: List[TableMapping]
    limit: Optional[int] = None

def get_url(config: DBConfig) -> str:
    if config.db_type == "sqlite":
        return f"sqlite:///{config.database}"
    elif config.db_type == "postgresql":
        return f"postgresql://{config.username}:{config.password}@{config.host}:{config.port}/{config.database}"
    elif config.db_type == "mysql":
        return f"mysql+pymysql://{config.username}:{config.password}@{config.host}:{config.port}/{config.database}"
    else:
        raise ValueError(f"Unsupported database type: {config.db_type}")


@app.post("/migrate")
async def migrate(request: MigrationRequest):
    try:
        source_url = get_url(request.source)
        dest_url = get_url(request.destination)
        
        config = {
            "source_db": {"url": source_url},
            "destination_db": {"url": dest_url},
            "mappings": [m.dict() for m in request.mappings],
            "limit": request.limit
        }
        
        # Check if this is a GLL migration (targeting 'institutions' table)
        is_gll = any(
            m.destination_table in ['institutions', 'institution_campuses'] or
            m.source_table in ['institution', 'address']
            for m in request.mappings
        )
        
        if is_gll:
            logger.info("Using GLL Migration Engine")
            engine = GLLMigrationEngine(config)
            try:
                count = engine.migrate()
                history.log_migration(
                    source_db=request.source.database,
                    dest_db=request.destination.database,
                    source_table="Multiple (GLL)",
                    dest_table="Multiple (GLL)",
                    status="success",
                    records=count
                )
            except Exception as e:
                history.log_migration(
                    source_db=request.source.database,
                    dest_db=request.destination.database,
                    source_table="Multiple (GLL)",
                    dest_table="Multiple (GLL)",
                    status="error",
                    error=str(e)
                )
                raise e
        else:
            engine = MigrationEngine(config)
            for mapping in request.mappings:
                try:
                    count = engine._migrate_table(mapping.source_table, mapping.destination_table, mapping.columns)
                    history.log_migration(
                        source_db=request.source.database,
                        dest_db=request.destination.database,
                        source_table=mapping.source_table,
                        dest_table=mapping.destination_table,
                        status="success",
                        records=count,
                        mapping=mapping.columns
                    )
                except Exception as table_err:
                    history.log_migration(
                        source_db=request.source.database,
                        dest_db=request.destination.database,
                        source_table=mapping.source_table,
                        dest_table=mapping.destination_table,
                        status="error",
                        error=str(table_err)
                    )
                    raise table_err

        return {"status": "success", "message": "Migration completed successfully"}
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history")
async def get_history():
    return history.get_history()

@app.post("/test-connection")
async def test_connection(config: DBConfig):
    try:
        from sqlalchemy import create_engine
        url = get_url(config)
        engine = create_engine(url)
        with engine.connect() as conn:
            return {"status": "success", "message": "Connection successful"}
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.post("/schema")
async def get_schema(config: DBConfig):
    try:
        from sqlalchemy import create_engine, MetaData
        url = get_url(config)
        engine = create_engine(url)
        metadata = MetaData()
        metadata.reflect(bind=engine)
        
        schema = {}
        for table_name, table in metadata.tables.items():
            schema[table_name] = [c.name for c in table.columns]
            
        return {"status": "success", "schema": schema}
    except Exception as e:
        logger.error(f"Failed to fetch schema: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
