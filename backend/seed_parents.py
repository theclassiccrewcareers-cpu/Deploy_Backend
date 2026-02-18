
import sqlite3
import datetime

# Connect to database
conn = sqlite3.connect('class_bridge.db')
c = conn.cursor()

print("--- Seeding Missing Parents for Grade 1 Students ---")

# 1. Get all students in Grade 1
students = c.execute("SELECT id, name FROM students WHERE grade=1 AND role='Student'").fetchall()

added_guardians = 0
added_users = 0

for student_id, student_name in students:
    # 2. Check if guardian exists
    guardian = c.execute("SELECT id FROM guardians WHERE student_id=?", (student_id,)).fetchone()
    
    if not guardian:
        # Create Parent ID/Email based on student ID (e.g., student_g1_1 -> parent_g1_1)
        parent_id = student_id.replace("student", "parent")
        parent_name = f"Parent of {student_name}"
        
        print(f"Adding parent for {student_name} ({student_id})...")
        
        # 3. Create User Account for Parent (if not exists)
        parent_user = c.execute("SELECT id FROM students WHERE id=?", (parent_id,)).fetchone()
        if not parent_user:
            c.execute("""
                INSERT INTO students (id, name, role, password, grade, home_language, is_super_admin)
                VALUES (?, ?, 'Parent', '123', 0, 'English', FALSE)
            """, (parent_id, parent_name))
            added_users += 1
            print(f"  -> Created User: {parent_id}")
        else:
            print(f"  -> User {parent_id} already exists.")
            
        # 4. Link Guardian to Student
        c.execute("""
            INSERT INTO guardians (student_id, name, relationship, phone, email, is_emergency_contact)
            VALUES (?, ?, 'Parent', '555-0000', ?, TRUE)
        """, (student_id, parent_name, parent_id))
        added_guardians += 1
        print(f"  -> Linked Guardian: {parent_name} to {student_id}")

conn.commit()
conn.close()

print(f"\nDone! Added {added_users} user accounts and {added_guardians} guardian links.")
