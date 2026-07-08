from engine.database.initialise import create_database

if __name__ == "__main__":
    created_path = create_database()
    print(f"Created DuckDB database at {created_path}")
