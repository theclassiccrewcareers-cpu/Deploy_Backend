"""
migrate_to_supabase.py
======================
Exports students + activities from local class_bridge.db (SQLite)
and upserts them into the Supabase PostgreSQL database.

Run this ONCE on your local machine:
  cd backend
  python3 migrate_to_supabase.py

Requirements: psycopg2-binary (pip install psycopg2-binary)
"""
import sqlite3
import os
import sys
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
SQLITE_DB = os.path.join(os.path.dirname(__file__), "class_bridge.db")
DATABASE_URL = os.environ.get("DATABASE_URL") or input("Paste your Supabase DATABASE_URL: ").strip()

if not DATABASE_URL:
    print("ERROR: DATABASE_URL is required.")
    sys.exit(1)

# ── Connect ───────────────────────────────────────────────────────────────────
print(f"\n[1/5] Connecting to SQLite: {SQLITE_DB}")
sq = sqlite3.connect(SQLITE_DB)
sq.row_factory = sqlite3.Row

print("[2/5] Connecting to Supabase PostgreSQL...")
pg = psycopg2.connect(DATABASE_URL)
pg.autocommit = False
cur = pg.cursor()

def safe(v):
    """Convert empty strings to None for Postgres."""
    if v == "":
        return None
    return v

# ── 1. Ensure schools ─────────────────────────────────────────────────────────
print("[3/5] Migrating schools...")
school_count = cur.execute if False else None

sq_schools = sq.execute("SELECT * FROM schools").fetchall()
for s in sq_schools:
    cur.execute("""
        INSERT INTO schools (id, name, address, contact_email, created_at, is_active)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            address = EXCLUDED.address,
            contact_email = EXCLUDED.contact_email
    """, (
        s['id'],
        s['name'],
        safe(s['address']),
        safe(s['contact_email']),
        safe(s['created_at']) or datetime.now().isoformat(),
        bool(s['is_active']) if s['is_active'] is not None else True
    ))

if not sq_schools:
    cur.execute("""
        INSERT INTO schools (id, name, address, contact_email, created_at, is_active)
        VALUES (1, 'Noble Nexus Academy', 'Ireland', 'info@noblenexus-ie.com', NOW(), true)
        ON CONFLICT (id) DO NOTHING
    """)
    print("  → Inserted default school: Noble Nexus Academy (id=1)")
else:
    print(f"  → Migrated {len(sq_schools)} school(s)")

pg.commit()

# ── 2. Migrate students ───────────────────────────────────────────────────────
print("[4/5] Migrating students...")
sq_students = sq.execute("""
    SELECT id, name, grade, preferred_subject, attendance_rate, home_language,
           password, math_score, science_score, english_language_score,
           role, failed_login_attempts, locked_until, xp, badges, school_id,
           is_super_admin, section_id, photo_url, email_verified,
           email_verification_token, email_verification_expires_at
    FROM students
""").fetchall()

migrated = 0
skipped = 0
for s in sq_students:
    try:
        cur.execute("""
            INSERT INTO students (
                id, name, grade, preferred_subject, attendance_rate, home_language,
                password, math_score, science_score, english_language_score,
                role, school_id, is_super_admin, email_verified,
                email_verification_token, email_verification_expires_at,
                failed_login_attempts, locked_until, photo_url, section_id, xp, badges
            ) VALUES (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                grade = EXCLUDED.grade,
                preferred_subject = EXCLUDED.preferred_subject,
                attendance_rate = EXCLUDED.attendance_rate,
                home_language = EXCLUDED.home_language,
                math_score = EXCLUDED.math_score,
                science_score = EXCLUDED.science_score,
                english_language_score = EXCLUDED.english_language_score,
                role = EXCLUDED.role,
                school_id = EXCLUDED.school_id,
                email_verified = EXCLUDED.email_verified,
                xp = EXCLUDED.xp,
                badges = EXCLUDED.badges
        """, (
            s['id'],
            s['name'],
            s['grade'],
            safe(s['preferred_subject']),
            s['attendance_rate'] or 0.0,
            safe(s['home_language']) or 'English',
            s['password'] or '',
            s['math_score'] or 0.0,
            s['science_score'] or 0.0,
            s['english_language_score'] or 0.0,
            s['role'] or 'Student',
            s['school_id'] or 1,
            bool(s['is_super_admin']) if s['is_super_admin'] is not None else False,
            # email_verified: set True so migrated users can log in immediately
            True,
            safe(s['email_verification_token']),
            safe(s['email_verification_expires_at']),
            s['failed_login_attempts'] or 0,
            safe(s['locked_until']),
            safe(s['photo_url']),
            safe(s['section_id']),
            s['xp'] or 0,
            safe(s['badges'])
        ))
        migrated += 1
    except Exception as e:
        pg.rollback()
        print(f"  ⚠ Skipped student '{s['id']}': {e}")
        skipped += 1
        continue

pg.commit()
print(f"  → Migrated {migrated} students, skipped {skipped}")

# ── 3. Migrate activities ─────────────────────────────────────────────────────
print("[5/5] Migrating activities...")
sq_activities = sq.execute("""
    SELECT id, student_id, date, topic, difficulty, score, time_spent_min, ai_feedback
    FROM activities
""").fetchall()

act_migrated = 0
act_skipped = 0
for a in sq_activities:
    try:
        cur.execute("""
            INSERT INTO activities (id, student_id, date, topic, difficulty, score, time_spent_min, ai_feedback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            a['id'],
            a['student_id'],
            safe(a['date']),
            safe(a['topic']),
            safe(a['difficulty']),
            a['score'] or 0.0,
            a['time_spent_min'] or 0,
            safe(a['ai_feedback'])
        ))
        act_migrated += 1
    except Exception as e:
        pg.rollback()
        print(f"  ⚠ Skipped activity {a['id']}: {e}")
        act_skipped += 1
        continue

pg.commit()
print(f"  → Migrated {act_migrated} activities, skipped {act_skipped}")

# ── Summary ───────────────────────────────────────────────────────────────────
sq.close()
cur.close()
pg.close()

print(f"""
╔══════════════════════════════════════════════════════════════╗
║  ✅ Migration complete!                                       ║
║                                                              ║
║  Students migrated : {migrated:<5}                                ║
║  Activities migrated: {act_migrated:<5}                                ║
║                                                              ║
║  Your Supabase database now has real student data.           ║
║  Refresh the deployed Teacher Dashboard — you should now     ║
║  see correct student counts and roster data.                 ║
╚══════════════════════════════════════════════════════════════╝
""")
