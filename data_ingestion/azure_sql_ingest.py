import pyodbc
import pandas as pd

# Azure SQL connection details
server = 'your-azure-server.database.windows.net'
database = 'your_db'
username = 'your_user'
password = 'your_password'
driver = '{ODBC Driver 17 for SQL Server}'

def get_data_from_azure(query):
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

if __name__ == '__main__':
    # Replace with your actual query
    sample_query = "SELECT * FROM operational_metrics"
    df = get_data_from_azure(sample_query)
    df.to_csv('../data/raw_metrics.csv', index=False)
    print("Data ingested and exported to data/raw_metrics.csv")