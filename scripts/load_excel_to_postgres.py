import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv 


load_dotenv()


DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")  
DB_PORT = os.getenv("DB_PORT", "5432")      
DB_NAME = os.getenv("DB_NAME")


if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise ValueError("Database credentials (DB_USER, DB_PASSWORD, DB_NAME) must be set in .env file.")

SOURCE_SCHEMA = "data_source"
DWH_SCHEMA = "dwh"


excel_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'Case_Study_Data___Data_Engineering__1_.xlsx')

def load_data_to_postgres():
    try:
       
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        print(f"Connecting to PostgreSQL database: {DB_NAME} at {DB_HOST}:{DB_PORT}")

        
        try:
            df_leads = pd.read_excel(excel_file_path, sheet_name='DE LEADS')
            df_sales = pd.read_excel(excel_file_path, sheet_name='DE SALES')
            print("Successfully read 'DE LEADS' and 'DE SALES' sheets from Excel.")
        except ValueError as e:
            print(f"Error reading Excel sheets. Make sure 'DE LEADS' and 'DE SALES' sheets exist: {e}")
            print(f"Please check the sheet names in '{excel_file_path}' and update the script if necessary.")
            return 

        with engine.connect() as connection:
           
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA};"))
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DWH_SCHEMA};"))
            connection.commit()
            print(f"Schemas '{SOURCE_SCHEMA}' and '{DWH_SCHEMA}' ensured to exist.")

            
            create_leads_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {SOURCE_SCHEMA}.leads (
                id INT PRIMARY KEY,
                date_of_last_request TIMESTAMP,
                buyer BOOLEAN,
                seller BOOLEAN,
                best_time_to_call VARCHAR(255),
                budget DECIMAL(15, 2),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                user_id INT,
                location VARCHAR(255),
                date_of_last_contact TIMESTAMP,
                status_name VARCHAR(255),
                commercial BOOLEAN,
                merged BOOLEAN,
                area_id INT,
                compound_id INT,
                developer_id INT,
                meeting_flag INT,
                do_not_call BOOLEAN,
                lead_type_id INT,
                customer_id INT,
                method_of_contact VARCHAR(255),
                lead_source VARCHAR(255),
                campaign VARCHAR(255),
                lead_type VARCHAR(255)
            );
            """
            connection.execute(text(create_leads_table_sql))
            print(f"Table '{SOURCE_SCHEMA}.leads' ensured to exist.")

            
            create_sales_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {SOURCE_SCHEMA}.sales (
                id INT PRIMARY KEY,
                lead_id INT,
                unit_value DECIMAL(15, 2),
                unit_location VARCHAR(255),
                expected_value DECIMAL(15, 2),
                actual_value DECIMAL(15, 2),
                date_of_reservation TIMESTAMP,
                reservation_update_date TIMESTAMP,
                date_of_contraction TIMESTAMP,
                property_type_id INT,
                area_id INT,
                compound_id INT,
                sale_category VARCHAR(255),
                years_of_payment INT,
                property_type VARCHAR(255)
            );
            """
            connection.execute(text(create_sales_table_sql))
            print(f"Table '{SOURCE_SCHEMA}.sales' ensured to exist.")
            connection.commit() 

            connection.execute(text(f"TRUNCATE TABLE {SOURCE_SCHEMA}.leads;"))
            connection.execute(text(f"TRUNCATE TABLE {SOURCE_SCHEMA}.sales;"))
            connection.commit()
            print(f"Tables '{SOURCE_SCHEMA}.leads' and '{SOURCE_SCHEMA}.sales' truncated.")

            df_leads.columns = df_leads.columns.str.lower()
            df_sales.columns = df_sales.columns.str.lower()

            df_leads.drop_duplicates(subset=['id'], keep='first', inplace=True)
            print(f"Removed duplicates from df_leads. New row count: {len(df_leads)}")

            for col in ['buyer', 'seller', 'commercial', 'merged', 'do_not_call']:
                if col in df_leads.columns:
                    
                    df_leads[col] = df_leads[col].astype(str).str.lower().map({'true': True, 'false': False, '1': True, '0': False, '': None, 'nan': None})
                    
                    df_leads[col] = df_leads[col].where(pd.notna(df_leads[col]), None)


            print(f"Inserting {len(df_leads)} rows into {SOURCE_SCHEMA}.leads...")
            df_leads.to_sql('leads', con=engine, schema=SOURCE_SCHEMA, if_exists='append', index=False)
            print("Leads data loaded successfully.")

            print(f"Inserting {len(df_sales)} rows into {SOURCE_SCHEMA}.sales...")
            df_sales.to_sql('sales', con=engine, schema=SOURCE_SCHEMA, if_exists='append', index=False)
            print("Sales data loaded successfully.")

            print("All raw data loaded successfully to PostgreSQL under 'data_source' schema!")

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please check your database credentials, connection, and Excel file structure/sheet names.")

if __name__ == "__main__":
    load_data_to_postgres()