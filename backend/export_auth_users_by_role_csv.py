#!/usr/bin/env python3
"""
Export auth users to CSV in this format:

First Name, Last Name, Institution Name, Role, Email Address, Status

The export is read-only. Connection values can be passed with CLI flags or
AUTH_DB_* environment variables.
"""

import argparse
import csv
import os
import re
from pathlib import Path

import pymysql


HEADERS = [
    "First Name",
    "Last Name",
    "Institution Name",
    "Role",
    "Email Address",
    "Status",
]

DEFAULT_DB_CONFIG = {
    "host": "gll-prod.cx4i0k8o6lzg.us-east-2.rds.amazonaws.com",
    "port": 3306,
    "user": "gll_user",
    "password": "StrongPaSSW0rdGLL@2026",
    "database": "gll_prod",
}

INSTITUTION_ROLE_MAPPING = {
    1: "receiver_admin",
    2: "institution_admin",
    3: "receiver",
    4: "recruiter",
    5: "developer",
    6: "counsellor",
    7: "recommender",
    8: "service_provider",
    9: "career_services",
}

USER_TYPE_ROLE_MAPPING = {
    "student": "student",
    "parent": "parent",
    "recommender": "recommender",
    "employer": "recruiter",
    "university": "receiver",
    "serviceprovider": "service_provider",
    "support": "support_admin",
    "superadmin": "super_admin",
    "tcbadmin": "institution_admin",
}

STATUS_MAPPING = {
    "0": "inactive",
    "1": "active",
    "2": "inactive",
    "3": "deleted",
    "4": "deactivated",
    "active": "active",
    "inactive": "inactive",
    "deleted": "deleted",
    "deactivated": "deactivated",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export auth DB users by role to CSV."
    )
    parser.add_argument("--host", default=os.getenv("AUTH_DB_HOST", DEFAULT_DB_CONFIG["host"]))
    parser.add_argument("--port", type=int, default=int(os.getenv("AUTH_DB_PORT", DEFAULT_DB_CONFIG["port"])))
    parser.add_argument("--user", default=os.getenv("AUTH_DB_USER", DEFAULT_DB_CONFIG["user"]))
    parser.add_argument("--password", default=os.getenv("AUTH_DB_PASSWORD", DEFAULT_DB_CONFIG["password"]))
    parser.add_argument("--database", default=os.getenv("AUTH_DB_NAME", DEFAULT_DB_CONFIG["database"]))
    parser.add_argument(
        "--output",
        default="auth_users_by_role.csv",
        help="Output CSV path, or output directory when --split-by-role is used.",
    )
    parser.add_argument(
        "--split-by-role",
        action="store_true",
        help="Write one CSV per role into the output directory.",
    )
    parser.add_argument(
        "--complete-only",
        action="store_true",
        help="Only include rows where all mandatory CSV fields are populated.",
    )
    parser.add_argument(
        "--include-students",
        action="store_true",
        help="Include student role rows. Default excludes students.",
    )
    parser.add_argument(
        "--include-parents",
        action="store_true",
        help="Include parent role rows. Default excludes parents.",
    )
    return parser.parse_args()


def require_connection_args(args):
    missing = [
        name
        for name, value in (
            ("--host/AUTH_DB_HOST", args.host),
            ("--user/AUTH_DB_USER", args.user),
            ("--password/AUTH_DB_PASSWORD", args.password),
            ("--database/AUTH_DB_NAME", args.database),
        )
        if not value
    ]
    if missing:
        raise SystemExit("Missing connection values: " + ", ".join(missing))


def connect(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def table_columns(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            """,
            (table_name,),
        )
        return {row["COLUMN_NAME"] for row in cursor.fetchall()}


def require_tables(conn, table_names):
    missing = []
    for table_name in table_names:
        if not table_columns(conn, table_name):
            missing.append(table_name)
    if missing:
        raise SystemExit("Missing required table(s): " + ", ".join(missing))


def column_expr(alias, columns, names, fallback="''"):
    for name in names:
        if name in columns:
            return f"{alias}.`{name}`"
    return fallback


def role_case():
    role_parts = [
        f"WHEN iu.role_id = {role_id} THEN '{role_code}'"
        for role_id, role_code in INSTITUTION_ROLE_MAPPING.items()
    ]
    type_parts = [
        f"WHEN LOWER(TRIM(u.user_type)) = '{user_type}' THEN '{role_code}'"
        for user_type, role_code in USER_TYPE_ROLE_MAPPING.items()
    ]
    return (
        "CASE "
        + " ".join(role_parts)
        + " "
        + " ".join(type_parts)
        + " ELSE COALESCE(NULLIF(TRIM(u.user_type), ''), 'unknown') END"
    )


def status_case(institution_user_columns, gl_user_columns):
    status_source = (
        "iu.status"
        if "status" in institution_user_columns
        else "iu.active"
        if "active" in institution_user_columns
        else "NULL"
    )
    user_status = (
        "u.status"
        if "status" in gl_user_columns
        else "u.active"
        if "active" in gl_user_columns
        else "NULL"
    )
    source_expr = f"LOWER(TRIM(CAST(COALESCE({status_source}, {user_status}, 'active') AS CHAR)))"
    status_parts = [
        f"WHEN {source_expr} = '{source_value}' THEN '{status_value}'"
        for source_value, status_value in STATUS_MAPPING.items()
    ]
    return "CASE " + " ".join(status_parts) + f" ELSE {source_expr} END"


def build_query(conn):
    require_tables(conn, ["gl_user", "institution"])

    gl_user_columns = table_columns(conn, "gl_user")
    institution_user_columns = table_columns(conn, "institution_user")
    gl_student_columns = table_columns(conn, "gl_student")
    gl_parent_columns = table_columns(conn, "gl_parent")

    if "user_type" not in gl_user_columns or "id" not in gl_user_columns:
        raise SystemExit("gl_user must contain id and user_type columns.")

    user_first = column_expr("u", gl_user_columns, ["first_name"])
    user_last = column_expr("u", gl_user_columns, ["last_name"])
    user_email = column_expr("u", gl_user_columns, ["email", "username"])

    student_first = column_expr("gs", gl_student_columns, ["first_name"])
    student_last = column_expr("gs", gl_student_columns, ["last_name"])
    student_email = column_expr("gs", gl_student_columns, ["email"])

    parent_first = column_expr("gp", gl_parent_columns, ["first_name"])
    parent_last = column_expr("gp", gl_parent_columns, ["last_name"])
    parent_email = column_expr("gp", gl_parent_columns, ["email_address", "email"])

    joins = []
    institution_id_expr = "iu.institution_id"

    if institution_user_columns:
        joins.append(
            """
            LEFT JOIN institution_user iu
              ON iu.user_id = u.id
             AND LOWER(TRIM(u.user_type)) <> 'student'
            """
        )
    else:
        joins.append("LEFT JOIN (SELECT NULL user_id, NULL institution_id, NULL role_id, NULL status) iu ON 1 = 0")

    if gl_student_columns and {"user_id", "institution_id"}.issubset(gl_student_columns):
        joins.append(
            """
            LEFT JOIN gl_student gs
              ON gs.user_id = u.id
             AND LOWER(TRIM(u.user_type)) = 'student'
            """
        )
        institution_id_expr = "COALESCE(gs.institution_id, iu.institution_id)"
    else:
        joins.append("LEFT JOIN (SELECT NULL user_id, NULL institution_id) gs ON 1 = 0")

    if gl_parent_columns and "user_id" in gl_parent_columns:
        joins.append(
            """
            LEFT JOIN gl_parent gp
              ON gp.user_id = u.id
             AND LOWER(TRIM(u.user_type)) = 'parent'
            """
        )
    else:
        joins.append("LEFT JOIN (SELECT NULL user_id) gp ON 1 = 0")

    joins.append(f"LEFT JOIN institution i ON i.id = {institution_id_expr}")

    return f"""
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(CAST({student_first} AS CHAR)), ''),
                     NULLIF(TRIM(CAST({parent_first} AS CHAR)), ''),
                     NULLIF(TRIM(CAST({user_first} AS CHAR)), ''),
                     '') AS `First Name`,
            COALESCE(NULLIF(TRIM(CAST({student_last} AS CHAR)), ''),
                     NULLIF(TRIM(CAST({parent_last} AS CHAR)), ''),
                     NULLIF(TRIM(CAST({user_last} AS CHAR)), ''),
                     '') AS `Last Name`,
            COALESCE(i.name, '') AS `Institution Name`,
            {role_case()} AS `Role`,
            COALESCE(NULLIF(TRIM(CAST({parent_email} AS CHAR)), ''),
                     NULLIF(TRIM(CAST({student_email} AS CHAR)), ''),
                     NULLIF(TRIM(CAST({user_email} AS CHAR)), ''),
                     '') AS `Email Address`,
            {status_case(institution_user_columns, gl_user_columns)} AS `Status`
        FROM gl_user u
        {' '.join(joins)}
        ORDER BY `Role`, `Institution Name`, `Last Name`, `First Name`, `Email Address`
    """


def table_exists(conn, table_name):
    return bool(table_columns(conn, table_name))


def build_destination_query(conn, complete_only=False, include_students=False, include_parents=False):
    require_tables(conn, [
        "users",
        "user_profile",
        "user_role",
        "role",
        "user_institution",
        "institutions",
    ])

    first_name = "NULLIF(TRIM(COALESCE(up.first_name, '')), '')"
    last_name = "NULLIF(TRIM(COALESCE(up.last_name, '')), '')"
    institution_name = "NULLIF(TRIM(COALESCE(i.name, '')), '')"
    role_name = "NULLIF(TRIM(COALESCE(r.code, r.name, '')), '')"
    email_address = "NULLIF(TRIM(COALESCE(u.email, u.user_name, '')), '')"

    filters = [
        "AND ur.deleted_at IS NULL",
        f"AND {role_name} IS NOT NULL",
    ]
    if not include_students:
        filters.append(f"AND LOWER({role_name}) <> 'student'")
    if not include_parents:
        filters.append(f"AND LOWER({role_name}) <> 'parent'")

    if complete_only:
        filters.extend([
            f"AND {first_name} IS NOT NULL",
            f"AND {last_name} IS NOT NULL",
            f"AND {institution_name} IS NOT NULL",
            f"AND {email_address} IS NOT NULL",
            "AND u.uuid IS NOT NULL",
        ])

    extra_filters = "\n          ".join(filters)

    return f"""
        SELECT DISTINCT
            COALESCE({first_name}, '') AS `First Name`,
            COALESCE({last_name}, '') AS `Last Name`,
            COALESCE({institution_name}, '') AS `Institution Name`,
            COALESCE({role_name}, '') AS `Role`,
            COALESCE({email_address}, '') AS `Email Address`,
            COALESCE({email_address}, '') AS `_Lookup Email`,
            COALESCE(u.user_name, '') AS `_Lookup Username`,
            CASE
                WHEN u.uuid IS NULL THEN ''
                WHEN CAST(COALESCE(u.status, 1) AS CHAR) = '1' THEN 'active'
                WHEN CAST(COALESCE(u.status, 1) AS CHAR) = '2' THEN 'inactive'
                WHEN CAST(COALESCE(u.status, 1) AS CHAR) = '3' THEN 'deleted'
                WHEN CAST(COALESCE(u.status, 1) AS CHAR) = '4' THEN 'deactivated'
                ELSE CAST(COALESCE(u.status, '') AS CHAR)
            END AS `Status`
        FROM user_role ur
        JOIN role r
          ON r.uuid = ur.role
        LEFT JOIN users u
          ON u.uuid = ur.user_uuid
        LEFT JOIN user_profile up
          ON up.user_uuid = ur.user_uuid
         AND up.deleted_at IS NULL
        LEFT JOIN user_institution ui
          ON ui.user_uuid = ur.user_uuid
         AND ui.deleted_at IS NULL
        LEFT JOIN institutions i
          ON i.uuid = ui.institution_uuid
        WHERE 1 = 1
          {extra_filters}
        ORDER BY `Role`, `Institution Name`, `Last Name`, `First Name`, `Email Address`
    """


def build_export_query(conn, complete_only=False, include_students=False, include_parents=False):
    if table_exists(conn, "gl_user"):
        return build_query(conn)
    return build_destination_query(conn, complete_only=complete_only, include_students=include_students, include_parents=include_parents)


def safe_role_filename(role):
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", role or "unknown").strip("_")
    return safe or "unknown"


def normalize_key(value):
    return str(value or "").strip().lower()


def chunked(values, size=500):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def load_source_fallbacks(conn, rows):
    keys = sorted({
        normalize_key(row.get("_Lookup Email") or row.get("Email Address"))
        for row in rows
        if normalize_key(row.get("_Lookup Email") or row.get("Email Address"))
    })
    username_keys = sorted({
        normalize_key(row.get("_Lookup Username"))
        for row in rows
        if normalize_key(row.get("_Lookup Username"))
    })
    if not keys and not username_keys:
        return {}

    fallback_by_key = {}
    with conn.cursor() as cursor:
        for key_batch in chunked(keys):
            placeholders = ",".join(["%s"] * len(key_batch))
            cursor.execute(f"""
                SELECT
                    gu.email,
                    gu.username,
                    COALESCE(NULLIF(TRIM(gu.first_name), ''), NULLIF(TRIM(ju.first_name), '')) AS first_name,
                    COALESCE(NULLIF(TRIM(gu.last_name), ''), NULLIF(TRIM(ju.last_name), '')) AS last_name,
                    NULLIF(TRIM(i.name), '') AS institution_name
                FROM gll_prod.gl_user gu
                LEFT JOIN gll_prod.jhi_user ju
                  ON ju.id = gu.user_id
                LEFT JOIN gll_prod.institution_user iu
                  ON iu.user_id = gu.id
                LEFT JOIN gll_prod.institution i
                  ON i.id = iu.institution_id
                WHERE gu.email IN ({placeholders})
                   OR gu.username IN ({placeholders})
            """, key_batch + key_batch)

            for source_row in cursor.fetchall():
                fallback = {
                    "First Name": source_row.get("first_name") or "",
                    "Last Name": source_row.get("last_name") or "",
                    "Institution Name": source_row.get("institution_name") or "",
                }
                for key_value in (source_row.get("email"), source_row.get("username")):
                    key = normalize_key(key_value)
                    if not key or key in fallback_by_key:
                        continue
                    fallback_by_key[key] = fallback

        for key_batch in chunked(keys):
            placeholders = ",".join(["%s"] * len(key_batch))
            cursor.execute(f"""
                SELECT email, first_name, last_name
                FROM gll_prod.gl_student
                WHERE email IN ({placeholders})
            """, key_batch)

            for student_row in cursor.fetchall():
                key = normalize_key(student_row.get("email"))
                if not key:
                    continue
                fallback = fallback_by_key.setdefault(key, {
                    "First Name": "",
                    "Last Name": "",
                    "Institution Name": "",
                })
                if not fallback.get("First Name") and student_row.get("first_name"):
                    fallback["First Name"] = student_row.get("first_name")
                if not fallback.get("Last Name") and student_row.get("last_name"):
                    fallback["Last Name"] = student_row.get("last_name")

        for username_batch in chunked(username_keys):
            placeholders = ",".join(["%s"] * len(username_batch))
            cursor.execute(f"""
                SELECT
                    gu.email,
                    gu.username,
                    NULLIF(TRIM(i.name), '') AS institution_name
                FROM gll_prod.gl_user gu
                JOIN gll_prod.institution i
                  ON i.id = CAST(REGEXP_SUBSTR(gu.username, '^[0-9]+') AS UNSIGNED)
                WHERE gu.username IN ({placeholders})
            """, username_batch)

            for source_row in cursor.fetchall():
                for key_value in (source_row.get("email"), source_row.get("username")):
                    key = normalize_key(key_value)
                    if not key:
                        continue
                    fallback = fallback_by_key.setdefault(key, {
                        "First Name": "",
                        "Last Name": "",
                        "Institution Name": "",
                    })
                    if not fallback.get("Institution Name") and source_row.get("institution_name"):
                        fallback["Institution Name"] = source_row.get("institution_name")

    return fallback_by_key


def apply_source_fallbacks(rows, fallback_by_key):
    for row in rows:
        key = normalize_key(row.get("_Lookup Email") or row.get("Email Address"))
        fallback = fallback_by_key.get(key, {})
        for column_name in ("First Name", "Last Name", "Institution Name"):
            if not str(row.get(column_name) or "").strip() and fallback.get(column_name):
                row[column_name] = fallback[column_name]
    return rows


def write_combined_csv(conn, query, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = list(cursor.fetchall())

    fallback_by_key = load_source_fallbacks(conn, rows)
    rows = apply_source_fallbacks(rows, fallback_by_key)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in HEADERS})

    return len(rows)


def write_split_csvs(conn, query, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    writers = {}
    counts = {}

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                role = row.get("Role") or "unknown"
                if role not in writers:
                    path = output_dir / f"{safe_role_filename(role)}.csv"
                    files[role] = path.open("w", newline="", encoding="utf-8")
                    writers[role] = csv.DictWriter(files[role], fieldnames=HEADERS)
                    writers[role].writeheader()
                    counts[role] = 0
                writers[role].writerow({header: row.get(header, "") for header in HEADERS})
                counts[role] += 1
    finally:
        for file_handle in files.values():
            file_handle.close()

    return counts


def main():
    args = parse_args()
    require_connection_args(args)

    with connect(args) as conn:
        query = build_export_query(conn, complete_only=args.complete_only, include_students=args.include_students, include_parents=args.include_parents)
        if args.split_by_role:
            counts = write_split_csvs(conn, query, Path(args.output))
            total = sum(counts.values())
            print(f"Wrote {total} rows into {len(counts)} role CSV file(s): {args.output}")
            for role, count in sorted(counts.items()):
                print(f"  {role}: {count}")
        else:
            count = write_combined_csv(conn, query, Path(args.output))
            print(f"Wrote {count} rows: {args.output}")


if __name__ == "__main__":
    main()
