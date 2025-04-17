import psycopg2
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
            print("Table created.")  

def search_by_pattern(pattern):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM phonebook
                WHERE first_name LIKE %s OR phone_number LIKE %s;
            """, ('%' + pattern + '%', '%' + pattern + '%'))
            results = cur.fetchall()
            if results:
                for row in results:
                    print(row)
            else:
                print("No records found matching the pattern:", pattern)


def insert_or_update_user(name, phone):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL insert_or_update_user(%s, %s);", (name, phone))
            print("User inserted or updated.")


def insert_many_users(users_data):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL insert_many_users(%s);", (users_data,))
            print("Users inserted or invalid data was noted.")


def query_with_pagination(limit, offset):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM phonebook
                LIMIT %s OFFSET %s;
            """, (limit, offset))
            rows = cur.fetchall()
            if rows:
                for row in rows:
                    print(row)
            else:
                print("No records found.")

def delete_user(identifier):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL delete_user_by_name_or_phone(%s);", (identifier,))
            print(f"User with identifier {identifier} deleted (if exists).")


def main():
    while True:
        print("Choose an option:")
        print("1. Search by pattern")
        print("2. Insert or update user")
        print("3. Insert many users")
        print("4. Query data with pagination")
        print("5. Delete user by name or phone")
        print("6. Exit")
        
        choice = input("Enter your choice (1-6): ")

        if choice == '1':
            pattern = input("Enter the pattern to search: ")
            search_by_pattern(pattern)
        elif choice == '2':
            name = input("Enter the name: ")
            phone = input("Enter the phone: ")
            insert_or_update_user(name, phone)
        elif choice == '3':
            users_data = input("Enter users data in format [name,phone] (comma-separated): ").split(',')
            insert_many_users(users_data)
        elif choice == '4':
            limit = int(input("Enter limit: "))
            offset = int(input("Enter offset: "))
            query_with_pagination(limit, offset)
        elif choice == '5':
            identifier = input("Enter the name or phone number to delete: ")
            delete_user(identifier)
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()
