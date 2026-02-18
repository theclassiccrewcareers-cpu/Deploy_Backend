
import sqlite3

def find_principal_credentials():
    db_path = "class_bridge.db"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        roles = ['Principal', 'Admin', 'Super_Admin', 'SuperAdmin', 'Tenant_Admin']
        print(f"Searching for users with roles: {roles}")
        
        # Check 'students' table 
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='students'")
        if cursor.fetchone():
            print("\n--- Users in 'students' table ---")
            for role in roles:
                cursor.execute(f"SELECT id, name, role, password FROM students WHERE role LIKE ?", (f'%{role}%',))
                users = cursor.fetchall()
                for user in users:
                    print(f"User: {user[1]} (ID: {user[0]}), Role: {user[2]}, Password: {user[3]}")
        else:
            print("'students' table not found.")

        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_principal_credentials()
