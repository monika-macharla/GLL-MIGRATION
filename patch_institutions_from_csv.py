"""
patch_institutions_from_csv.py
===============================
Reads an institutions mapping CSV (exported from Google Sheets) and updates
the `institutions` table in the configured MySQL database.

CSV column  →  DB column mapping
---------------------------------
Real Institution Type (Shrikant)  →  type            (InstitutionType enum int)
(hardcoded)                        →  status          = 'active'
Certificates Enabled               →  is_certificates_enabled  (bool)
Transcript Sharing Enabled         →  is_transcript_enabled    (bool)
EDI Segment Terminator             →  edi_segment
EDI Field Terminator               →  edi_field
NSC Receiver ID                    →  nsc_receiver_id
Qual                               →  qual_code
school Code                        →  speede_code

JOIN KEY: The CSV must contain a column whose value can be matched against the
          `institutions` table.  Configure MATCH_CSV_COL and MATCH_DB_COL below.

Usage
-----
    # 1. Export the Google Sheet as CSV first (File → Download → CSV) and save it locally.
    # 2. Run:
    python patch_institutions_from_csv.py --csv /path/to/file.csv

    # Or override DB settings at runtime:
    python patch_institutions_from_csv.py --csv /path/to/file.csv \\
        --host 52.14.86.71 --port 3307 --user glldev \\
        --password 'G11d4V@6202' --database gllauthservice

    # Dry-run (no writes):
    python patch_institutions_from_csv.py --csv /path/to/file.csv --dry-run
"""

import argparse
import csv
import os
import sys
import pymysql
import pymysql.cursors

# ===========================================================================
# ① DB CONNECTION DEFAULTS  — override via CLI args
# ===========================================================================
DEFAULT_HOST     = '52.14.86.71'
DEFAULT_PORT     = 3307
DEFAULT_USER     = 'glldev'
DEFAULT_PASSWORD = 'G11d4V@6202'
DEFAULT_DATABASE = 'gllauthservice'

# ===========================================================================
# ② MATCH KEY — which CSV column maps to which DB column for the WHERE clause
# ===========================================================================
MATCH_CSV_COL = 'Institution Name'   # column in the CSV that identifies the row
MATCH_DB_COL  = 'name'               # column in institutions table to match against

# ===========================================================================
# ③ InstitutionType enum  (mirrors the proto enum in the codebase)
# ===========================================================================
INSTITUTION_TYPE_MAP = {
    'unspecified':               0,
    'university':                1,
    'employer':                  2,
    'parent_university':         3,
    'parent university':         3,
    'service_provider':          4,
    'service provider':          4,
    'regional_service_provider': 5,
    'regional service provider': 5,
    # sub-types / aliases that appear in the sheet
    'high school':               1,   # UNIVERSITY sub-type → type UNIVERSITY
    'high_school':               1,
    'community college':         1,
    'community_college':         1,
    'higher education':          1,
    'higher_education':          1,
    'nsc':                       1,
    'college':                   1,
}

# ===========================================================================
# ④ CSV column → DB column mapping
# ===========================================================================
# Keys are matched case-insensitively against actual CSV headers
CSV_TO_DB = {
    'Real Institution Type (Shrikant)': 'type',
    'Certificates Enabled':             'is_certificates_enabled',
    'Transcript Sharing Enabled':       'is_transcript_enabled',
    'EDI Segment Terminator':           'edi_segment',
    'EDI Field Terminator':             'edi_field',
    'NSC Receiver ID':                  'nsc_receiver_id',
    'Qual':                             'qual_code',
    'School Code':                      'speede_code',   # capital S/C as in actual sheet
}

# UniversitySubType enum (for reference / logging)
UNIVERSITY_SUB_TYPE_MAP = {
    'high school':        1,
    'community college':  2,
    'higher education':   3,
    'nsc':                4,
    'college':            5,
}


# ===========================================================================
# Helpers
# ===========================================================================

def parse_bool(value: str) -> int:
    """Convert common truthy strings to 1/0 for tinyint(1) columns."""
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in ('1', 'true', 'yes', 'y', 'enabled', 'active'):
        return 1
    if v in ('0', 'false', 'no', 'n', 'disabled', 'inactive', ''):
        return 0
    return None   # unknown → skip


def resolve_type(raw: str):
    """Map the human-readable institution type string to its int enum value."""
    if raw is None or str(raw).strip() == '':
        return None
    key = str(raw).strip().lower()
    if key in INSTITUTION_TYPE_MAP:
        return INSTITUTION_TYPE_MAP[key]
    # try partial match
    for k, v in INSTITUTION_TYPE_MAP.items():
        if k in key or key in k:
            return v
    print(f"  [WARN] Unknown institution type '{raw}' — skipping type update for this row.")
    return None


def coerce(db_col: str, raw_value: str):
    """Coerce a raw CSV string to the correct Python type for the given db column."""
    if raw_value is None or str(raw_value).strip() == '':
        return None

    if db_col == 'type':
        return resolve_type(raw_value)

    if db_col in ('is_certificates_enabled', 'is_transcript_enabled'):
        return parse_bool(raw_value)

    # everything else: plain string, strip whitespace
    return str(raw_value).strip() or None


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Patch institutions table from a CSV mapping file.'
    )
    parser.add_argument('--csv',      required=True,                    help='Path to the CSV file (exported from Google Sheets)')
    parser.add_argument('--host',     default=DEFAULT_HOST,             help='MySQL host')
    parser.add_argument('--port',     default=DEFAULT_PORT, type=int,   help='MySQL port')
    parser.add_argument('--user',     default=DEFAULT_USER,             help='MySQL user')
    parser.add_argument('--password', default=DEFAULT_PASSWORD,         help='MySQL password')
    parser.add_argument('--database', default=DEFAULT_DATABASE,         help='MySQL database')
    parser.add_argument('--match-csv-col', default=MATCH_CSV_COL,      help='CSV column used as the join key')
    parser.add_argument('--match-db-col',  default=MATCH_DB_COL,       help='DB column used as the join key')
    parser.add_argument('--dry-run',  action='store_true',              help='Print SQL but do not commit')
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Read CSV or Excel file (auto-detected)
    # ------------------------------------------------------------------
    print(f"Reading file: {args.csv}")
    if not os.path.exists(args.csv):
        print(f"ERROR: File not found: {args.csv}")
        sys.exit(1)

    ext = os.path.splitext(args.csv)[1].lower()
    rows = []
    headers = []

    # --- Excel (.xlsx / .xls) ---
    if ext in ('.xlsx', '.xls') or _is_excel(args.csv):
        try:
            import openpyxl
            import shutil
            import tempfile
        except ImportError:
            print("ERROR: openpyxl is required to read Excel files. Install with: pip install openpyxl")
            sys.exit(1)
        # openpyxl rejects files whose extension isn't .xlsx/.xlsm etc.
        # Copy to a temp file with the correct extension before loading.
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        shutil.copy2(args.csv, tmp_path)
        try:
            wb = openpyxl.load_workbook(tmp_path, read_only=True, data_only=True)
            ws = wb.active
            raw = list(ws.iter_rows(values_only=True))
            wb.close()
        finally:
            os.unlink(tmp_path)
        if not raw:
            print("ERROR: Excel file is empty.")
            sys.exit(1)
        headers = [str(c).strip() if c is not None else '' for c in raw[0]]
        for r in raw[1:]:
            rows.append({headers[i]: (str(v).strip() if v is not None else '') for i, v in enumerate(r)})

    # --- CSV (try common encodings) ---
    else:
        for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
            try:
                with open(args.csv, newline='', encoding=enc) as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    headers = list(reader.fieldnames or [])
                print(f"  Detected encoding: {enc}")
                break
            except (UnicodeDecodeError, Exception):
                continue
        else:
            print("ERROR: Could not decode CSV file. Try converting to UTF-8 or xlsx.")
            sys.exit(1)


def _is_excel(path: str) -> bool:
    """Detect Excel magic bytes regardless of file extension."""
    try:
        with open(path, 'rb') as f:
            header = f.read(4)
        # xlsx: PK zip signature; xls: D0 CF magic
        return header[:4] in (b'PK\x03\x04', b'\xd0\xcf\x11\xe0')
    except Exception:
        return False

    print(f"  Columns found: {headers}")
    print(f"  Total rows   : {len(rows)}")

    # Build a case-insensitive lookup so minor capitalisation differences don't break things
    header_map = {h.lower(): h for h in headers}   # lower → actual header

    def resolve_header(wanted: str) -> str:
        """Return the actual header name matching `wanted` (case-insensitive)."""
        return header_map.get(wanted.lower(), wanted)

    # Re-map CSV_TO_DB keys to actual header casing found in file
    global CSV_TO_DB
    CSV_TO_DB = {resolve_header(k): v for k, v in CSV_TO_DB.items()}
    args.match_csv_col = resolve_header(args.match_csv_col)

    if args.match_csv_col not in headers:
        print(f"\nERROR: Match column '{args.match_csv_col}' not found in CSV.")
        print(f"Available columns: {headers}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Connect to DB
    # ------------------------------------------------------------------
    print(f"\nConnecting to {args.user}@{args.host}:{args.port}/{args.database} ...")
    try:
        conn = pymysql.connect(
            host=args.host, port=args.port,
            user=args.user, password=args.password,
            database=args.database,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except Exception as e:
        print(f"ERROR: Cannot connect to database — {e}")
        sys.exit(1)

    print("  Connected.\n")

    updated    = 0
    skipped    = 0
    not_found  = 0
    errors     = 0

    try:
        with conn.cursor() as cur:
            for i, row in enumerate(rows, start=1):
                match_value = str(row.get(args.match_csv_col, '')).strip()
                if not match_value:
                    print(f"  Row {i:>4}: [SKIP] empty match key.")
                    skipped += 1
                    continue

                # Build SET clause
                set_parts   = []
                set_values  = []

                # Always set status = 'active'
                set_parts.append('status = %s')
                set_values.append('active')

                for csv_col, db_col in CSV_TO_DB.items():
                    if csv_col not in row:
                        continue   # column not in this CSV
                    value = coerce(db_col, row[csv_col])
                    if value is None:
                        continue   # nothing to write
                    set_parts.append(f'`{db_col}` = %s')
                    set_values.append(value)

                if not set_parts:
                    print(f"  Row {i:>4}: [SKIP] no fields to update for '{match_value}'.")
                    skipped += 1
                    continue

                set_parts.append('updated_at = NOW()')

                sql = (
                    f"UPDATE institutions "
                    f"SET {', '.join(set_parts)} "
                    f"WHERE `{args.match_db_col}` = %s"
                )
                params = set_values + [match_value]

                if args.dry_run:
                    print(f"  Row {i:>4}: [DRY-RUN] {sql % tuple(repr(p) for p in params)}")
                    updated += 1
                    continue

                try:
                    cur.execute(sql, params)
                    if cur.rowcount > 0:
                        updated += 1
                        print(f"  Row {i:>4}: [OK]      '{match_value}' → updated {cur.rowcount} row(s).")
                    else:
                        not_found += 1
                        print(f"  Row {i:>4}: [MISS]    '{match_value}' not found in institutions table.")
                except Exception as e:
                    errors += 1
                    print(f"  Row {i:>4}: [ERROR]   '{match_value}' — {e}")

        if not args.dry_run:
            conn.commit()
            print("\nAll changes committed.")

    except Exception as e:
        conn.rollback()
        print(f"\nFATAL ERROR — rolled back all changes: {e}")
        raise

    finally:
        conn.close()

    print(f"""
Summary
-------
  Updated   : {updated}
  Not found : {not_found}
  Skipped   : {skipped}
  Errors    : {errors}
""")


if __name__ == '__main__':
    main()
