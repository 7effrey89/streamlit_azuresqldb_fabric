#Instructions used to create this demo
# 1. Open a terminal and navigate to your project folder.
# cd c:\git\StreamlitDemo

# 2. Create environment
#python -m venv .venv

# 3. Activate the environment
# Windows command prompt
#.venv\Scripts\activate.bat
#or
# Windows PowerShell
#.venv\Scripts\Activate.ps1

# Check if pip is up-to-date
# python.exe -m pip install --upgrade pip

# 4. pip install streamlit / pip install --upgrade streamlit
# 4. pip install pyodbc sqlalchemy

# 5. Test that the installation worked..:
# streamlit hello
# If this doesn't work, use the long-form command:
# python -m streamlit hello

# 6. Create demo table on sql endpoint e.g. Azure SQL Database
# CREATE TABLE [dbo].[person](
# 	[name] [varchar](10) NULL,
# 	[address] [varchar](10) NULL,
# 	[type] [varchar](10) NULL,
# 	[date] [date] NULL
# ) 

# 6. To run our demo
# python -m streamlit run PrintAzureSQLTable.py

#Inspiration:
#https://www.geeksforgeeks.org/a-beginners-guide-to-streamlit/

import streamlit as st
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text

# Connection string
SERVER = st.secrets["server"]
USERNAME = st.secrets["username"]
PASSWORD = st.secrets["password"]
DRIVER = st.secrets["driver"]

DATABASE = '<name-of-database>' #e.g.'sandbox'
TABLE_SCHEMA = '<name-of-schema>' #e.g. 'dbo'
TABLE_NAME = '<name-of-table>' #e.g. 'person'
QUERY = f'SELECT top (100) name, address, type, date from {TABLE_SCHEMA}.{TABLE_NAME};'
DROP_TABLE = f"DROP TABLE {TABLE_SCHEMA}.{TABLE_NAME}"
INIT_TABLE = f"""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TABLE_NAME}' AND schema_id = SCHEMA_ID('{TABLE_SCHEMA}'))
CREATE TABLE {TABLE_SCHEMA}.{TABLE_NAME} (
	[name] [varchar](100) NULL,
	[address] [varchar](100) NULL,
	[type] [varchar](100) NULL,
	[date] [date] NULL
);
--Insert sample data
INSERT INTO {TABLE_SCHEMA}.{TABLE_NAME} (name, address, type, date)
VALUES 
('Alice', 'Street 1', 'Type A', '2024-07-24'),
('Bob', 'Street 2', 'Type B', '2024-07-23'),
('Charlie', 'Street 3', 'Type C', '2024-07-22');"""

# Uses st.cache_resource to only run once.
#@st.cache_resource
def init_connection():
    # Create connection engine
    connection_string = f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
    engine = create_engine(connection_string)
    return engine

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 1 sec (10 min=ttl=600).
#@st.cache_data(ttl=1)
def Select_query(query):
    engine = init_connection()
    with engine.connect() as connection:
        result = pd.read_sql_query(query, connection.connection)
        return result
    
def execute_sql_command(batch_command):
    """
    Executes a batch SQL command.
    """
    with engine.begin() as conn:
        conn.execute(text(batch_command))

 # Initialize connection.   
engine = init_connection()

st.write('# Embed Table Data from Azure SQL Database in Streamlit page')
st.write('This script demonstrate how to retrieve table data from Azure SQL Database and embed is a native text on a Streamlit page. \
        \n\nTo run this demo, ensure you have the following table in your Azure SQL Database:')

st.code(INIT_TABLE, language='sql')

#layouting
col1, col2 = st.columns(2)
with col1:
    st.button(f'Create {TABLE_SCHEMA}.{TABLE_NAME} Demo Table', on_click=execute_sql_command, args=[INIT_TABLE])
with col2:
    st.button(f'Drop {TABLE_SCHEMA}.{TABLE_NAME} Demo Table', on_click=execute_sql_command, args=[DROP_TABLE])

st.write('## View table data from Azure SQL Database')
st.write(f'Shows the current data in the {TABLE_SCHEMA}.{TABLE_NAME} table. Will show an error if table does not exists.')
st.divider()
df = Select_query(QUERY)

# Print results.
for index, row in df.iterrows():
    st.write(f"name column: {row['name']} | address column: {row['address']}")
    
# create a Excel file with the results
# df.to_excel("FileExample.xlsx",sheet_name='Results')