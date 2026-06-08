import argparse
import csv
import logging
import os
import uuid
from pathlib import Path

import yaml
from sqlalchemy import MetaData, Table, create_engine, insert
from sqlalchemy.engine import URL


DEFAULT_DATABASE = "gllreportsdevmigration"
DEFAULT_TABLE = "blockchain_mapping"
DEFAULT_BATCH_SIZE = 1000
CSV_COLUMN_ALIASES = {
    "share_id": ("id", "shareId", "shareID"),
    "generated_hash": ("generatedHash", "generated_hash"),
    "pdf_block_chain_address": (
        "pdfBlockChainAddress",
        "pdf_block_chain_address",
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_db_config(config_path: str | None, database: str | None) -> dict:
    db_config = {}

    if config_path:
        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file) or {}
        db_config = dict(config.get("destination_db") or {})

    db_config["type"] = os.getenv("MYSQL_TYPE", db_config.get("type", "mysql"))
    db_config["host"] = os.getenv("MYSQL_HOST", db_config.get("host", "localhost"))
    db_config["port"] = int(os.getenv("MYSQL_PORT", db_config.get("port", 3306)))
    db_config["username"] = os.getenv(
        "MYSQL_USER",
        os.getenv("MYSQL_USERNAME", db_config.get("username", "root")),
    )
    db_config["password"] = os.getenv(
        "MYSQL_PASSWORD",
        db_config.get("password", ""),
    )
    db_config["database"] = (
        database
        or os.getenv("MYSQL_DATABASE")
        or db_config.get("database")
        or DEFAULT_DATABASE
    )

    return db_config


def get_engine(db_config: dict):
    if db_config.get("url"):
        return create_engine(db_config["url"])

    db_type = db_config.get("type", "mysql")
    drivername = "postgresql" if db_type == "postgresql" else "mysql+pymysql"
    default_port = 5432 if db_type == "postgresql" else 3306

    return create_engine(
        URL.create(
            drivername=drivername,
            username=db_config.get("username"),
            password=db_config.get("password"),
            host=db_config.get("host", "localhost"),
            port=db_config.get("port") or default_port,
            database=db_config.get("database"),
        )
    )


def csv_files(csv_path: Path, recursive: bool) -> list[Path]:
    if csv_path.is_file():
        if csv_path.suffix.lower() != ".csv":
            raise ValueError(f"File is not a CSV: {csv_path}")
        return [csv_path]

    if not csv_path.is_dir():
        raise ValueError(f"CSV path does not exist: {csv_path}")

    pattern = "**/*.csv" if recursive else "*.csv"
    return sorted(path for path in csv_path.glob(pattern) if path.is_file())


def normalize_value(value: str | None):
    if value is None:
        return None

    value = value.strip()
    return value if value != "" else None


def is_error_status(value: str | None) -> bool:
    normalized = normalize_value(value)
    return str(normalized or "").lower() == "error"


def csv_source_for_column(column_name: str, csv_column_map: dict[str, str]):
    if column_name in csv_column_map:
        return csv_column_map[column_name]

    for alias in CSV_COLUMN_ALIASES.get(column_name, ()):
        if alias in csv_column_map:
            return csv_column_map[alias]

    return None


def generated_columns(table: Table) -> set[str]:
    columns = set()
    for column in table.columns:
        if column.default is not None or column.server_default is not None:
            columns.add(column.name)
    return columns


def required_columns(table: Table) -> set[str]:
    generated = generated_columns(table)
    return {
        column.name
        for column in table.columns
        if not column.nullable
        and column.name not in generated
        and column.autoincrement is not True
    }


def import_csv_file(connection, table: Table, csv_file: Path, batch_size: int) -> int:
    table_columns = {column.name for column in table.columns}
    required_table_columns = required_columns(table)
    total_rows = 0
    skipped_rows = 0
    invalid_rows = 0

    with csv_file.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        if not reader.fieldnames:
            logger.warning("Skipping empty CSV file: %s", csv_file)
            return 0

        csv_column_map = {
            column.strip(): column
            for column in reader.fieldnames
            if column and column.strip()
        }
        column_sources = {
            column: source
            for column in table_columns
            if (source := csv_source_for_column(column, csv_column_map))
        }
        if "uuid" in table_columns:
            column_sources["uuid"] = column_sources.get("uuid")

        used_csv_columns = {
            source for source in column_sources.values() if source is not None
        }
        ignored_columns = sorted(set(csv_column_map.values()) - used_csv_columns)
        missing_required_columns = sorted(
            column
            for column in required_table_columns
            if column != "uuid" and column not in column_sources
        )

        if ignored_columns:
            logger.warning(
                "Ignoring columns not present in %s: %s",
                table.name,
                ", ".join(ignored_columns),
            )

        if missing_required_columns:
            raise ValueError(
                f"{csv_file} is missing required column(s) for {table.name}: "
                f"{', '.join(missing_required_columns)}"
            )

        if not column_sources:
            raise ValueError(
                f"{csv_file} does not contain any columns from {table.name}"
            )

        batch = []
        for raw_row in reader:
            status_column = csv_column_map.get("status")
            if status_column and is_error_status(raw_row.get(status_column)):
                skipped_rows += 1
                continue

            row = {
                column: normalize_value(raw_row.get(source))
                for column, source in column_sources.items()
                if source is not None
            }
            if "uuid" in table_columns and not row.get("uuid"):
                row["uuid"] = str(uuid.uuid4())

            missing_values = [
                column
                for column in required_table_columns
                if not row.get(column)
            ]
            if missing_values:
                invalid_rows += 1
                logger.warning(
                    "Skipping row in %s because required value(s) are missing: %s",
                    csv_file,
                    ", ".join(sorted(missing_values)),
                )
                continue

            batch.append(row)

            if len(batch) >= batch_size:
                connection.execute(insert(table), batch)
                total_rows += len(batch)
                batch.clear()

        if batch:
            connection.execute(insert(table), batch)
            total_rows += len(batch)

    logger.info(
        "Imported %s rows from %s; skipped %s error status row(s); "
        "skipped %s invalid row(s)",
        total_rows,
        csv_file,
        skipped_rows,
        invalid_rows,
    )
    return total_rows


def import_csv_path(
    csv_path: Path,
    config_path: str | None,
    database: str | None,
    table_name: str,
    batch_size: int,
    recursive: bool,
) -> int:
    files = csv_files(csv_path, recursive)
    if not files:
        logger.warning("No CSV files found in %s", csv_path)
        return 0

    db_config = load_db_config(config_path, database)
    logger.info(
        "Importing %s CSV file(s) into %s.%s",
        len(files),
        db_config["database"],
        table_name,
    )

    engine = get_engine(db_config)
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    imported_rows = 0
    with engine.begin() as connection:
        for csv_file in files:
            imported_rows += import_csv_file(
                connection,
                table,
                csv_file,
                batch_size,
            )

    return imported_rows


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Import all CSV files from a path into the blockchain_mapping table."
        )
    )
    parser.add_argument(
        "csv_path",
        help="CSV file or directory containing CSV files to import.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Config file to read destination_db from. Defaults to config.yaml.",
    )
    parser.add_argument(
        "--database",
        default=None,
        help=(
            "Destination database. Defaults to destination_db.database from "
            f"the config file, then {DEFAULT_DATABASE}."
        ),
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Destination table. Defaults to {DEFAULT_TABLE}.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows inserted per batch. Defaults to {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Also import CSV files in nested directories.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    imported_rows = import_csv_path(
        csv_path=Path(args.csv_path),
        config_path=args.config,
        database=args.database,
        table_name=args.table,
        batch_size=args.batch_size,
        recursive=args.recursive,
    )
    logger.info("Finished importing %s row(s)", imported_rows)


if __name__ == "__main__":
    main()
