import psycopg2
import csv
from connect import connect

def create_table():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phonebook (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50),
                    phone_number VARCHAR(20) UNIQUE                                     
                );
            """)
            print("Table created.")                                                        # Simply creating a table

def insert_from_console():
    name = input("Enter name: ")
    phone = input("Enter phone: ")
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO phonebook (first_name, phone_number) VALUES (%s, %s)", (name, phone))                # My own insert 
            print("User added from console.")

def insert_from_csv(filename='phonebook.csv'):
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        next(reader)  
        with connect() as conn:
            with conn.cursor() as cur:
                for row in reader:
                    cur.execute("INSERT INTO phonebook (first_name, phone_number) VALUES (%s, %s) ON CONFLICT DO NOTHING", row)  # CSV Insert
            print("Data inserted from CSV.")

def update_user():
    name = input("Enter name to update: ")
    new_name = input("New name (leave empty to skip): ")
    new_phone = input("New phone (leave empty to skip): ")
    with connect() as conn:
        with conn.cursor() as cur:
            if new_name:
                cur.execute("UPDATE phonebook SET first_name = %s WHERE first_name = %s", (new_name, name))            #User updating
            if new_phone:
                cur.execute("UPDATE phonebook SET phone_number = %s WHERE first_name = %s", (new_phone, name))
            print("User updated.")

def query_data():
    filter_type = input("Filter by (name/phone/none): ").lower()
    value = input("Enter filter value (leave empty for none): ") 
    limit = int(input("Enter number of records per page: "))                                # Get limit of records
    offset = int(input("Enter the number of records to skip (offset): "))                   # Skipping records

    with connect() as conn:
        with conn.cursor() as cur:
            if filter_type == "name":
                cur.execute("""
                    SELECT * FROM phonebook 
                    WHERE first_name ILIKE %s 
                    LIMIT %s OFFSET %s
                """, (f"%{value}%", limit, offset))
            elif filter_type == "phone":
                cur.execute("""
                    SELECT * FROM phonebook 
                    WHERE phone_number ILIKE %s 
                    LIMIT %s OFFSET %s
                """, (f"%{value}%", limit, offset))
            else:
                cur.execute("""
                    SELECT * FROM phonebook
                    LIMIT %s OFFSET %s
                """, (limit, offset))
            
            rows = cur.fetchall()
            if rows:
                print("Results:")
                for row in rows:
                    print(row)
            else:
                print("No results found.")


def delete_user():
    name = input("Delete by name (leave empty if deleting by phone): ")
    phone = input("Delete by phone (leave empty if deleting by name): ")
    with connect() as conn:
        with conn.cursor() as cur:
            if name:
                cur.execute("DELETE FROM phonebook WHERE first_name = %s", (name,))                          #Deleting 
            elif phone:
                cur.execute("DELETE FROM phonebook WHERE phone_number = %s", (phone,))
            print("User deleted.")


 # Functions added for the Lab11 

def search_by_pattern():
    pattern = input("Enter search pattern (part of name or phone): ")
    pattern = f"%{pattern}%"                                                                        # This allows partial match
    
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM phonebook 
                WHERE first_name ILIKE %s 
                   OR phone_number ILIKE %s
            """, (pattern, pattern))
            rows = cur.fetchall()
            if rows:
                print("Matching records:")
                for row in rows:
                    print(row)
            else:
                print("No matches found.")

def create_insert_or_update_procedure():
    with connect() as conn:
        with conn.cursor() as cur:                                                                                  #Creating a procedure
            cur.execute("""
                CREATE OR REPLACE PROCEDURE insert_or_update_user(p_name VARCHAR, p_phone VARCHAR)                
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM phonebook WHERE first_name = p_name) THEN
                        UPDATE phonebook SET phone_number = p_phone WHERE first_name = p_name;
                    ELSE
                        INSERT INTO phonebook(first_name, phone_number) VALUES (p_name, p_phone);
                    END IF;
                END;
                $$;
            """)
            print("Procedure 'insert_or_update_user' created.")
# Function for our procedure           
def insert_or_update_via_procedure():
    name = input("Enter name: ")
    phone = input("Enter phone: ")
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL insert_or_update_user(%s, %s);", (name, phone))
            print("User inserted or updated")

def create_insert_many_users_procedure():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE OR REPLACE PROCEDURE insert_many_users(
                    IN p_names TEXT[],
                    IN p_phones TEXT[]
                )
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    i INT;
                BEGIN
                    CREATE TEMP TABLE invalid_entries (
                        name TEXT,
                        phone TEXT
                    ) ON COMMIT DROP;

                    FOR i IN 1 .. array_length(p_names, 1) LOOP
                        IF p_phones[i] ~ '^[+]?[\d\\s\\-]{7,15}$' THEN
                            BEGIN
                                INSERT INTO phonebook (first_name, phone_number)
                                VALUES (p_names[i], p_phones[i])
                                ON CONFLICT (phone_number) DO NOTHING;
                            EXCEPTION WHEN OTHERS THEN
                                INSERT INTO invalid_entries VALUES (p_names[i], p_phones[i]);
                            END;
                        ELSE
                            INSERT INTO invalid_entries VALUES (p_names[i], p_phones[i]);
                        END IF;
                    END LOOP;

                    RAISE NOTICE 'Invalid entries:';
                    FOR i IN SELECT * FROM invalid_entries LOOP
                        RAISE NOTICE 'Name: %, Phone: %', i.name, i.phone;
                    END LOOP;
                END;
                $$;
            """)
            print("Procedure 'insert_many_users' created.")



def insert_many_users():
    names = input("Enter names (comma-separated): ").split(",")
    phones = input("Enter phones (comma-separated): ").split(",")

    if len(names) != len(phones):
        print("Error: names and phones count must match.")
        return

    with connect() as conn:
        with conn.cursor() as cur:
            print("Inserting multiple users...")
            cur.execute("CALL insert_many_users(%s, %s);", (names, phones))
            print("Procedure executed.")

def query_paginated_data():
    page = int(input("Enter page number: "))
    limit = int(input("Enter number of records per page: "))
    offset = (page - 1) * limit

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_phonebook_page(%s, %s);", (limit, offset))
            rows = cur.fetchall()
            if rows:
                print("Page {} results:".format(page))
                for row in rows:
                    print(row)
            else:
                print("No more records.")

def create_delete_many_users_procedure():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE OR REPLACE PROCEDURE delete_user_by_username_or_phone(p_username VARCHAR DEFAULT NULL, p_phone_number VARCHAR DEFAULT NULL)
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    IF p_username IS NOT NULL THEN
                        DELETE FROM phonebook WHERE first_name = p_username;
                        RAISE NOTICE 'Deleted user(s) with name: %', p_username;
                    
                    ELSIF p_phone_number IS NOT NULL THEN
                        DELETE FROM phonebook WHERE phone_number = p_phone_number;
                        RAISE NOTICE 'Deleted user(s) with phone number: %', p_phone_number;
                    ELSE
                        RAISE EXCEPTION 'No username or phone number provided for deletion';
                    END IF;
                END;
                $$;
            """)
            print("Procedure 'delete_many_users' created.")

def delete_user_procedure():
    choice = input("Delete by (name/phone): ").lower()
    if choice == "name":
        username = input("Enter username to delete: ")
        phone_number = None
    elif choice == "phone":
        username = None
        phone_number = input("Enter phone number to delete: ")
    else:
        print("Invalid choice.")
        return

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL delete_user_by_username_or_phone(%s, %s);", (username, phone_number))
            print("User deleted.")





def main():
    create_table()
    create_insert_or_update_procedure()
    create_insert_many_users_procedure()
    create_delete_many_users_procedure()



    while True:
        print("PHONEBOOK MENU")
        print("1. Insert from console")
        print("2. Insert from CSV")
        print("3. Update user")
        print("4. Query users")
        print("5. Delete user")
        print("6. Exit")
        print("7. Search by pattern")
        print("8. Insert or update user (procedure)")
        print("9. Insert multiple users (procedure)")
        print("10. Query data with pagination")
        print("11. Deleting by name or phone (procedure)")





        choice = input("Choose option: ")
        if choice == '1':
            insert_from_console()
        elif choice == '2':
            insert_from_csv()
        elif choice == '3':
            update_user()
        elif choice == '4':
            query_data()
        elif choice == '5':
            delete_user()
        elif choice == '6':
            print("Exiting...")
            break
        elif choice == '7':
            search_by_pattern()
        elif choice == '8':
            insert_or_update_via_procedure()
        elif choice == '9':
            insert_many_users()
        elif choice == '10':
            query_paginated_data()
        elif choice == '11':
            delete_user_procedure()
        else:
            print("Invalid choice.")

if __name__ == '__main__':
    main()
