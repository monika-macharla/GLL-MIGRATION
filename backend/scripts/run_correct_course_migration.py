import pymysql
import time
import sys

def run():
    start_id = 1
    # Check if a starting ID is passed as an argument
    if len(sys.argv) > 1:
        try:
            start_id = int(sys.argv[1])
        except ValueError:
            pass

    conn = pymysql.connect(
        host='gll-prod.cx4i0k8o6lzg.us-east-2.rds.amazonaws.com',
        port=3306,
        user='gll_user',
        password='StrongPaSSW0rdGLL@2026',
        autocommit=True
    )
    try:
        with conn.cursor() as cur:
            if start_id == 1:
                print("Truncating destination table import_edi_courses...")
                cur.execute("TRUNCATE TABLE glldataingestionnew.import_edi_courses")
                print("Truncate completed successfully.")
            else:
                print(f"Resuming migration from tc.id = {start_id} (skipping truncate)...")
            
            chunk_size = 100000
            max_id = 89248724
            total_inserted = 0
            
            insert_query = """
            INSERT IGNORE INTO glldataingestionnew.import_edi_courses (
              uuid, student_number, institution_id,
              college_code, college_name, semester_title,
              course_number, course_title, semester_hrs,
              grade, core_curriculum, semester_id,
              start_date, end_date, term_gpa, gpa_credit,
              semester_code, repeat_flag, other_credit_source,
              fresh_start, field_of_study,
              created_at, updated_at
            )
            SELECT
              UUID()                              AS uuid,
              t.stu_identification                AS student_number,
              '39d22dd8-d942-4a82-b302-869efa16b944'           AS institution_id,
              tc.college_code                     AS college_code,
              tc.college_name                     AS college_name,
              tc.semester_title                   AS semester_title,
              tc.course_number                    AS course_number,
              tc.course_title                     AS course_title,
              CAST(tc.semester_hrs AS CHAR)       AS semester_hrs,
              tc.grade                            AS grade,
              tc.core_curriculum                  AS core_curriculum,
              es.semester_id                      AS semester_id,
              tc.start_date                       AS start_date,
              tc.end_date                         AS end_date,
              tc.term_gpa                         AS term_gpa,
              tc.gpa_credit                       AS gpa_credit,
              tc.semester_code                    AS semester_code,
              tc.repeat_flag                      AS repeat_flag,
              tc.other_credit_source              AS other_credit_source,
              tc.fresh_start                      AS fresh_start,
              tc.field_of_study                   AS field_of_study,
              NOW()                               AS created_at,
              NOW()                               AS updated_at
            FROM gll_prod_new.transcript_course tc
            JOIN gll_prod_new.transcript t
              ON t.id = tc.transcript_id
            JOIN gll_prod_new.semester s
              ON s.id = tc.semester_id
            JOIN glldataingestionnew.import_edi_semester es
              ON es.student_number = t.stu_identification
              AND es.institution_id = '39d22dd8-d942-4a82-b302-869efa16b944'
              AND es.start_date    = s.start_date
              AND es.end_date      = s.end_date
              AND es.deleted_at    IS NULL
            WHERE t.institution_id = 1
              AND tc.id BETWEEN %s AND %s
              AND t.id = (
                SELECT MAX(t2.id)
                FROM gll_prod_new.transcript t2
                WHERE t2.stu_identification = t.stu_identification
                  AND t2.institution_id = t.institution_id
              )
            """
            
            chunk_num = (start_id - 1) // chunk_size
            t_start = time.time()
            while start_id <= max_id:
                end_id = start_id + chunk_size - 1
                chunk_num += 1
                
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        cur.execute(insert_query, (start_id, end_id))
                        inserted = cur.rowcount or 0
                        total_inserted += inserted
                        break
                    except Exception as e:
                        if hasattr(e, "args") and len(e.args) > 0 and e.args[0] in (1205, 1213) and attempt < max_retries - 1:
                            sleep_time = (attempt + 1) * 2
                            print(f"Warning: Database lock error ({e.args[0]}). Retrying chunk {chunk_num} in {sleep_time}s... (Attempt {attempt+1}/{max_retries})")
                            time.sleep(sleep_time)
                        else:
                            raise
                
                if chunk_num % 10 == 0 or inserted > 0:
                    elapsed = time.time() - t_start
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    print(f"Chunk {chunk_num}: tc.id [{start_id}-{end_id}] -> inserted {inserted} rows. Total inserted: {total_inserted}. Rate: {rate:.1f} rows/s")
                
                start_id += chunk_size
                
            print(f"Migration completed successfully. Total inserted: {total_inserted} in {time.time() - t_start:.2f}s")
    finally:
        conn.close()

if __name__ == '__main__':
    run()
