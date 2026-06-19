import uuid
from collections import defaultdict

import pymysql


SOURCE = {
    "host": "192.168.2.195",
    "port": 3306,
    "user": "gllmigration",
    "password": "G1lmI7rAtI0n@2026",
    "database": "gll",
}

DEST = {
    "host": "16.58.172.0",
    "port": 3307,
    "user": "uat-gll",
    "password": "GLLuAtS3rver@2026",
    "database": "glldataingestionuatmigration",
}

AUTH_DB = "gllauthserviceuatmigration"
DATA_DB = "glldataingestionuatmigration"

TARGETS = [
    ("import_students", "institution_id"),
    ("import_student_test_assessments", "institution_id"),
    ("counsellor_student_view", "institution_id"),
    ("import_schools_awarding_credits", "institution_id"),
    ("import_parents", "institution_id"),
    ("import_other_requirements", "institution_id"),
    ("import_credit_summary", "institution_id"),
    ("import_student_graduation_profile", "institution_id"),
    ("import_class_rank_gpa", "institution_id"),
    ("import_cert_lics", "institution_id"),
    ("import_apibs", "institution_id"),
    ("import_biliteracies", "institution_id"),
    ("import_dual_credits", "institution_id"),
    ("import_college_assessments", "institution_id"),
    ("holds", "institution_id"),
    ("vaccination_certificate_data", "institution_uuid"),
    ("import_course_information", "institution_id"),
]


def norm(value):
    return " ".join(str(value or "").strip().lower().split())


def q(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


source_conn = pymysql.connect(**SOURCE, cursorclass=pymysql.cursors.DictCursor)
dest_conn = pymysql.connect(**DEST, cursorclass=pymysql.cursors.DictCursor)

with source_conn, dest_conn:
    with source_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name
            FROM institution
            WHERE name IS NOT NULL AND TRIM(name) <> ''
            """
        )
        source_rows = cur.fetchall()

    with dest_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT uuid, name
            FROM {AUTH_DB}.institutions
            WHERE deleted_at IS NULL
              AND name IS NOT NULL
              AND TRIM(name) <> ''
            """
        )
        auth_rows = cur.fetchall()

    auth_by_name = defaultdict(list)
    for row in auth_rows:
        auth_by_name[norm(row["name"])].append(row)

    mapping = []
    ambiguous = []
    missing = []
    for row in source_rows:
        old_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"gll:institution:{row['id']}"))
        matches = auth_by_name.get(norm(row["name"]), [])
        if len(matches) == 1:
            new_uuid = matches[0]["uuid"]
            if old_uuid != new_uuid:
                mapping.append((old_uuid, new_uuid, row["id"], row["name"]))
        elif len(matches) > 1:
            ambiguous.append((row["id"], row["name"], [m["uuid"] for m in matches]))
        else:
            missing.append((row["id"], row["name"], old_uuid))

    print("-- Generated institution UUID repair SQL")
    print("-- Review counts inside the transaction before COMMIT.")
    print("START TRANSACTION;")
    print("CREATE TEMPORARY TABLE tmp_institution_uuid_fix (")
    print("  old_uuid varchar(36) PRIMARY KEY,")
    print("  new_uuid varchar(36) NOT NULL,")
    print("  source_institution_id bigint NOT NULL,")
    print("  institution_name varchar(256) NOT NULL")
    print(");")
    print("INSERT INTO tmp_institution_uuid_fix")
    print("  (old_uuid, new_uuid, source_institution_id, institution_name)")
    print("VALUES")
    values = [
        f"  ({q(old)}, {q(new)}, {source_id}, {q(name)})"
        for old, new, source_id, name in mapping
    ]
    print(",\n".join(values) + ";")
    print()
    for table, column in TARGETS:
        print(f"-- {table}.{column}")
        print(
            f"SELECT COUNT(*) AS rows_to_update FROM {DATA_DB}.{table} t "
            f"JOIN tmp_institution_uuid_fix m ON t.{column} = m.old_uuid;"
        )
        print(
            f"UPDATE {DATA_DB}.{table} t "
            f"JOIN tmp_institution_uuid_fix m ON t.{column} = m.old_uuid "
            f"SET t.{column} = m.new_uuid;"
        )
        print()
    print("-- COMMIT;")
    print("-- ROLLBACK;")
    print()
    print(f"-- Mapping rows: {len(mapping)}")
    print(f"-- Ambiguous source-name matches not included: {len(ambiguous)}")
    print(f"-- Missing auth-name matches not included: {len(missing)}")
