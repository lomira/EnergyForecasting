from engine.database.initialise import create_database
from engine.ingestion.load_ingestion import add_load_excel_to_db

if __name__ == "__main__":
    created_path = create_database()
    print(f"Created DuckDB database at {created_path}")

    add_load_excel_to_db(
        file_path="data/raw/excel/BDD_E.xlsx",
        sheet_name="Feuil1",
        db_path=created_path,
    )
