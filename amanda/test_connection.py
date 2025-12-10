"""
Simple script to test MySQL connection
"""
import mysql.connector
from mysql.connector import Error

# Database configuration (same as in database.py)
db_config = {
    'host': '144.91.96.79',
    'database': 'webscrapping_reservation',
    'password': 'FYFsS7vzyKrXbj66',
    'user': 'rootremote',
    'port': '3306'
}

print("Testing MySQL connection...")
print(f"Host: {db_config['host']}")
print(f"Port: {db_config['port']}")
print(f"Database: {db_config['database']}")
print(f"User: {db_config['user']}")
print("-" * 60)

try:
    print("\nAttempting to connect...")
    connection = mysql.connector.connect(**db_config)

    if connection.is_connected():
        db_info = connection.get_server_info()
        print(f"✓ Successfully connected to MySQL Server version {db_info}")

        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print(f"✓ Connected to database: {record[0]}")

        # Test table existence
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        print(f"\n✓ Tables in database ({len(tables)}):")
        for table in tables:
            print(f"  - {table[0]}")

        cursor.close()
        connection.close()
        print("\n✓ Connection closed successfully")

except Error as e:
    print(f"\n✗ Error connecting to MySQL: {e}")
    print(f"Error code: {e.errno}")
    print(f"SQL state: {e.sqlstate}")
