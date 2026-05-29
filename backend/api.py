from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional

from migration_engine import MigrationEngine
from gll_migration_engine import GLLMigrationEngine
from history_manager import HistoryManager

import logging
from sqlalchemy.engine import URL

# -------------------------------------------------
# Logging
# -------------------------------------------------

logging.basicConfig(level=logging.INFO)
#add the suport of resume migrator also in this file

logger = logging.getLogger(__name__)

# -------------------------------------------------
# FastAPI App
# -------------------------------------------------

app = FastAPI(
    title="Database Migration API"
)

history = HistoryManager()

# -------------------------------------------------
# CORS
# -------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------
# Request Models
# -------------------------------------------------


class DBConfig(BaseModel):

    db_type: str

    host: Optional[str] = None

    port: Optional[int] = None

    username: Optional[str] = None

    password: Optional[str] = None

    database: str


# -------------------------------------------------
# Lookup Database Config
# -------------------------------------------------


class LookupDBConfig(BaseModel):

    name: str

    db_type: str

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

    # -----------------------------------------
    # Multiple Lookup DBs
    # -----------------------------------------

    lookup_databases: Optional[
        List[LookupDBConfig]
    ] = []

    limit: Optional[int] = None

# -------------------------------------------------
# Generate SQLAlchemy URL
# -------------------------------------------------

def get_url(config: DBConfig):

    db_type = config.db_type or "mysql"

    # -------------------------------------------------
    # SQLite
    # -------------------------------------------------

    if db_type == "sqlite":

        return URL.create(
            "sqlite",
            database=config.database
        )

    # -------------------------------------------------
    # PostgreSQL / MySQL
    # -------------------------------------------------

    drivername = (

        "postgresql"

        if db_type == "postgresql"

        else "mysql+pymysql"
    )

    default_port = (

        5432

        if db_type == "postgresql"

        else 3306
    )

    return URL.create(

        drivername=drivername,

        username=config.username,

        password=config.password,

        host=config.host or "127.0.0.1",

        port=config.port or default_port,

        database=config.database
    )

# -------------------------------------------------
# Migration API
# -------------------------------------------------

@app.post("/migrate")
async def migrate(request: MigrationRequest):

    try:

        logger.info(
            "Starting Migration API"
        )

        # -------------------------------------------------
        # Generate DB URLs
        # -------------------------------------------------

        source_url = get_url(
            request.source
        )

        dest_url = get_url(
            request.destination
        )

        # -------------------------------------------------
        # Config
        # -------------------------------------------------

        config = {

            "source_db": {

                "url": source_url
            },

            "destination_db": {

                "url": dest_url
            },

            "mappings": [

                m.model_dump()

                for m in request.mappings
            ],

            "limit": request.limit,

            # -----------------------------------------
            # Lookup Databases
            # -----------------------------------------

            "lookup_databases": [

                db.model_dump()

                for db in request.lookup_databases
            ]
        }

        logger.info(
            f"Lookup DBs received: "
            f"{config['lookup_databases']}"
        )

        # -------------------------------------------------
        # Detect GLL Migrations
        # -------------------------------------------------

        is_gll = any(

            # destination tables
            (
                m.destination_table
                or
                ""
            ).strip().lower() in [

                "institutions",

                "institution_campuses",

                "users",

                "user_profile",

                "user_role",

                "password",

                "user_hashed_password",
                
                "user_institution",

                "parent_student",

                "user_enrollments",
                
                "credentials_digital_badges",
                
                "credentials_resume",
                
                "credentials_all",

                "credentials_self_uploads",

                "credentials_transcripts",
                
                "credentials_recommendation_letters",

                "credentials_certifications",

                "credentials_cerificate",

                "credentials_certificate",

                "credentials_shared",

                "student_credentials_share_history",

                "registrars",

                "my_preferences",

                "ferpa",

                "scholarship_prefernces",

                "module_permissions",

                "permissions",

                "import_students",

                "import_parent",

                "import_parents"
            ]

            or

            # source tables
            (
                m.source_table
                or
                ""
            ).strip().lower() in [

                "institution",

                "address",

                "jhi_user",

                "gl_user",

                "institution_user",

                "gl_parent_student",

                "student_enrollment",
                
                "badge",
                
                "resume",

                "other_credentials",

                "transcript",
                
                "recommendation_letter",

                "recommendation_request",

                "certificate",

                "badge_shared",

                "certificate_shared",

                "other_credential_share",

                "recommendation_letter_share",

                "self_uploaded_transcript_share",

                "transcript_shared",

                "resume_share",

                "institution_registrat",

                "institution_registrar",

                "institution_registrars",

                "student_preference",

                "ferpa",

                "nsapi_criteria",

                "student_credential_visibility",

                "student_crdential_visibility",

                "gl_student",

                "gl_parent",
            ]

            for m in request.mappings
        )

        logger.info(
            f"is_gll={is_gll}"
        )

        # -------------------------------------------------
        # Use GLL Migration Engine
        # -------------------------------------------------

        if is_gll:

            logger.info(
                "Using GLL Migration Engine"
            )

            engine = GLLMigrationEngine(
                config
            )

            try:

                migrated_count = (
                    engine.migrate()
                )

                history.log_migration(

                    source_db=(
                        request.source.database
                    ),

                    dest_db=(
                        request.destination.database
                    ),

                    source_table=(
                        "Multiple (GLL)"
                    ),

                    dest_table=(
                        "Multiple (GLL)"
                    ),

                    status="success",

                    records=migrated_count
                )

            except Exception as migration_error:

                history.log_migration(

                    source_db=(
                        request.source.database
                    ),

                    dest_db=(
                        request.destination.database
                    ),

                    source_table=(
                        "Multiple (GLL)"
                    ),

                    dest_table=(
                        "Multiple (GLL)"
                    ),

                    status="error",

                    error=str(
                        migration_error
                    )
                )

                raise migration_error

        # -------------------------------------------------
        # Use Generic Migration Engine
        # -------------------------------------------------

        else:

            logger.info(
                "Using Generic Migration Engine"
            )

            engine = MigrationEngine(
                config
            )

            for mapping in request.mappings:

                try:

                    migrated_count = (
                        engine._migrate_table(

                            mapping.source_table,

                            mapping.destination_table,

                            mapping.columns
                        )
                    )

                    history.log_migration(

                        source_db=(
                            request.source.database
                        ),

                        dest_db=(
                            request.destination.database
                        ),

                        source_table=(
                            mapping.source_table
                        ),

                        dest_table=(
                            mapping.destination_table
                        ),

                        status="success",

                        records=migrated_count,

                        mapping=(
                            mapping.columns
                        )
                    )

                except Exception as table_error:

                    history.log_migration(

                        source_db=(
                            request.source.database
                        ),

                        dest_db=(
                            request.destination.database
                        ),

                        source_table=(
                            mapping.source_table
                        ),

                        dest_table=(
                            mapping.destination_table
                        ),

                        status="error",

                        error=str(
                            table_error
                        )
                    )

                    raise table_error

        # -------------------------------------------------
        # Success Response
        # -------------------------------------------------

        return {

            "status": "success",

            "message": (
                "Migration completed "
                "successfully"
            )
        }

    except Exception as e:

        logger.error(
            f"Migration failed: "
            f"{str(e)}"
        )

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )

# -------------------------------------------------
# Migration History
# -------------------------------------------------

@app.get("/history")
async def get_history():

    return history.get_history()

# -------------------------------------------------
# Test DB Connection
# -------------------------------------------------

@app.post("/test-connection")
async def test_connection(config: DBConfig):

    try:

        from sqlalchemy import (
            create_engine
        )

        url = get_url(config)

        engine = create_engine(url)

        with engine.connect():

            pass

        return {

            "status": "success",

            "message": (
                "Connection successful"
            )
        }

    except Exception as e:

        logger.error(
            f"Connection test failed: "
            f"{str(e)}"
        )

        return {

            "status": "error",

            "message": str(e)
        }

# -------------------------------------------------
# Get Schema
# -------------------------------------------------

@app.post("/schema")
async def get_schema(config: DBConfig):

    try:

        from sqlalchemy import (

            create_engine,

            inspect
        )

        url = get_url(config)

        engine = create_engine(url)

        inspector = inspect(engine)

        schema = {}

        for table_name in inspector.get_table_names():

            try:

                schema[table_name] = [

                    column["name"]

                    for column in inspector.get_columns(
                        table_name
                    )
                ]

            except Exception as table_error:

                logger.warning(
                    f"Failed to fetch columns for "
                    f"{table_name}: {table_error}"
                )

        return {

            "status": "success",

            "schema": schema
        }

    except Exception as e:

        logger.error(
            f"Failed to fetch schema: "
            f"{str(e)}"
        )

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )

# -------------------------------------------------
# Run Server
# -------------------------------------------------

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(

        app,

        host="0.0.0.0",

        port=8000
    )
