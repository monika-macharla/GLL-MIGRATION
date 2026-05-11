import sys
import argparse
from migration_engine import MigrationEngine

def main():
    parser = argparse.ArgumentParser(description="Database Migration Tool")
    parser.add_argument("--config", default="config.yaml", help="Path to the configuration file")
    
    args = parser.parse_args()

    try:
        engine = MigrationEngine(args.config)
        engine.migrate()
        print("Migration completed successfully.")
    except FileNotFoundError:
        print(f"Error: Configuration file '{args.config}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
