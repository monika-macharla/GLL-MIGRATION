import os
import json
import uuid
import pymysql

# Load credentials from environment or fallback to defaults
MYSQL_HOST = os.getenv('MYSQL_HOST', 'gll-prod.cx4i0k8o6lzg.us-east-2.rds.amazonaws.com')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'gll_user')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'StrongPaSSW0rdGLL@2026')

print("Connecting to databases...")

# Connect to legacy DB
legacy_conn = pymysql.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database='gll_prod_new',
    cursorclass=pymysql.cursors.DictCursor
)

# Connect to Next.js DB
next_conn = pymysql.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database='glldataingestionnew',
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with legacy_conn.cursor() as legacy_cursor, next_conn.cursor() as next_cursor:
        print("Fetching legacy transcripts with TSI JSON data...")
        # Fetch only transcripts that actually have a JSON array
        legacy_cursor.execute("""
            SELECT id, stu_identification, institution_id, tsi_status 
            FROM transcript 
            WHERE tsi_status IS NOT NULL 
              AND tsi_status != '' 
              AND tsi_status LIKE '[%'
        """)
        
        transcripts = legacy_cursor.fetchall()
        print(f"Found {len(transcripts)} transcripts with TSI JSON.")
        
        inserted_count = 0
        
        for transcript in transcripts:
            student_number = transcript.get('stu_identification')
            legacy_tsi_status = transcript.get('tsi_status')
            
            if not student_number or not legacy_tsi_status:
                continue
                
            # Find the Next.js UUID for this student's institution
            next_cursor.execute("""
                SELECT institution_id 
                FROM import_students 
                WHERE student_number = %s 
                LIMIT 1
            """, (student_number,))
            
            inst_result = next_cursor.fetchone()
            if not inst_result:
                # Skip if we can't resolve the Next.js institution UUID for this student
                continue
                
            institution_id = inst_result['institution_id']
            
            try:
                statuses = json.loads(legacy_tsi_status)
                if not isinstance(statuses, list):
                    continue
                    
                for status in statuses:
                    test_area = status.get('testArea')
                    status_value = status.get('status')
                    explanation = status.get('reason')
                    
                    if not test_area and not status_value:
                        continue
                        
                    # Insert into Next.js table
                    next_cursor.execute("""
                        INSERT IGNORE INTO import_edi_tsi_status 
                        (id, student_number, institution_id, test_area, status, explanation)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        str(uuid.uuid4()), 
                        student_number, 
                        institution_id, 
                        test_area, 
                        status_value, 
                        explanation
                    ))
                    inserted_count += 1
                    
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON for transcript {transcript['id']}: {str(e)}")
        
        # Commit all the inserts to the Next.js database
        next_conn.commit()
        print(f"Successfully migrated {inserted_count} TSI status records into import_edi_tsi_status!")

finally:
    legacy_conn.close()
    next_conn.close()
