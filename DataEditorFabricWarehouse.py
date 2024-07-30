
# create table [dbo].[product] (
# 	id int identity(1,1),
# 	name varchar(100),
# 	category varchar(100)
# )

import streamlit as st
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
import struct
from itertools import chain, repeat
import pyodbc
import urllib
import random

# Acquire a credential object
def get_token():
    # credential = DefaultAzureCredential() #will automatically use the available credential
    credential = InteractiveBrowserCredential() #forces for interactive login as DefaultAzureCredential would use the available credential associated with the hosting of the app instead of the
    st.session_state['credential'] = credential

# Initialize session state for credential and token if they don't exist
if 'credential' not in st.session_state:
    st.session_state['credential'] = None
if 'token' not in st.session_state:
    st.session_state['token'] = None

#the endpoint url is the same for all sql endpoints on the workspace
SQL_ENDPOINT = "x6eps4xrq2xudenlfv6naeo3i4-jhrvr47hgqiuxicx25egay43ey.msit-datawarehouse.fabric.microsoft.com" 
# database = "LH_LevelUp"
DATABASE = "DW_LevelUp" #also known as the fabric resource name

TABLE_SCHEMA = 'dbo'
TABLE_NAME = 'product'
QUERY = f'select id, name, category from {TABLE_SCHEMA}.{TABLE_NAME};'
DROP_TABLE = f"DROP TABLE {TABLE_SCHEMA}.{TABLE_NAME}"
INIT_TABLE = f"""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TABLE_NAME}' AND schema_id = SCHEMA_ID('{TABLE_SCHEMA}'))
CREATE TABLE {TABLE_SCHEMA}.{TABLE_NAME} (
    id INT,
    name VARCHAR(100),
    category VARCHAR(100)
);
--Insert sample data
INSERT INTO {TABLE_SCHEMA}.{TABLE_NAME} (id, name, category)
VALUES 
(1, 'Product A', 'Category 1'),
(2, 'Product B', 'Category 2'),
(3, 'Product C', 'Category 3');
"""

# Uses st.cache_resource to only run once.
#@st.cache_resource(ttl=3600)
def init_connection():
    """
    Initializes a connection to the database using the provided credentials.

    Returns:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object representing the database connection.
    """
    # Do not use a global variable in streamlit. Streamlit always rerun the code from top to bottom when there is a change in the user interface. 
    # Instead use the built-in dictionary-like session_state
    if 'engineDW' not in st.session_state:
        # Create connection engine
        connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server={SQL_ENDPOINT},1433;Database={DATABASE};Encrypt=Yes;TrustServerCertificate=No"

        # prepare the access token
        #https://debruyn.dev/2023/connect-to-fabric-lakehouses-warehouses-from-python-code/#:~:text=1%20sql_endpoint%20%3D%20%22%22%20%23%20copy%20and%20paste,%3D%20f%22Driver%3D%7B%7BODBC%20Driver%2018%20for%20SQL%20Server%7D%7D%3BServer%3D%20%7Bsql_endpoint%7D%2C1433%3BDatabase%3Df%7Bdatabase%7D%3BEncrypt%3DYes%3BTrustServerCertificate%3DNo%22
        token_object = st.session_state['credential'].get_token("https://database.windows.net//.default") # Retrieve an access token valid to connect to SQL databases
        token_as_bytes = bytes(token_object.token, "UTF-8") # Convert the token to a UTF-8 byte string
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
        attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

        params = urllib.parse.quote_plus(connection_string)
        engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params), connect_args={'attrs_before': attrs_before}, echo=True, future=True)

        st.session_state['engineDW'] = engine

    return st.session_state['engineDW']

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 1 sec (10 min=ttl=600).
@st.cache_data(ttl=2)
def Select_query(query):
    if st.session_state['credential'] is None:
        st.error("Please get the token first.")
        return pd.DataFrame()
    else:
        engine = init_connection()
        with engine.connect() as connection:
            result = pd.read_sql_query(query, connection.connection)
            return result

def execute_sql_command(batch_command):
    """
    Executes a batch SQL command.
    """
    engine = init_connection()
    with engine.begin() as conn:
        conn.execute(text(batch_command))

def insert_added_rows(added_rows):
    if not added_rows:
        return
    insert_commands = [f"INSERT INTO {TABLE_SCHEMA}.{TABLE_NAME} (id, name, category) VALUES ({random.randrange(1, 2147483647, 1)},'{row['name']}', '{row['category']}')" for row in added_rows]
    batch_command = "; ".join(insert_commands)
    execute_sql_command(batch_command)

def delete_deleted_rows(deleted_rows):
    if not deleted_rows:
        return
    delete_commands = []
    
    for row_index in deleted_rows:
        # Get the ID value from database table rather than the index number from the DF.
        productID = df["id"][row_index]

        # Append each delete command to the list
        delete_commands.append(f"DELETE FROM {TABLE_SCHEMA}.{TABLE_NAME} WHERE id = {productID}")
    batch_command = "; ".join(delete_commands)

    execute_sql_command(batch_command)

def update_edited_rows(edited_rows):
    if not edited_rows:
        return
    update_commands = []

    for row_index, row in edited_rows.items():
        # Get the ID value from database table rather than the index number from the DF.
        productID = df["id"][row_index]
        # Construct the SET part of the SQL command dynamically based on edited fields
        set_parts = [f"{column} = '{value}'" for column, value in row.items()]
        set_command = ", ".join(set_parts)

        update_commands.append(f"UPDATE {TABLE_SCHEMA}.{TABLE_NAME} SET {set_command} WHERE id = {productID}")

    batch_command = "; ".join(update_commands)
    print(batch_command)
    execute_sql_command(batch_command)

################################################ Page code Starts here ################################################

 # Initialize connection.   
# engine = init_connection()
st.write('# Data Editor for Fabric Data Warehouse Table')
st.write('This script demonstrates how to use the Streamlit Data Editor to insert, update, and delete data in an Fabric Data Warehouse table.')

with st.expander("Jeffrey's Notes"):
    st.markdown('''
                - Fabric data warehoues and lakehouses are optimized for crunching big analytics workloads with its distributed compute architecture. 
                It is less ideal to use for transactional workloads such as fetching and updating individual rows. Such use case are better served with a traditional database such as Azure SQL db for the best performance.
                - From an architectural perspective modifying the data directly in a data warehouse might not be the best practice as audit trails and data governance might be compromised.
                - I'm using streamlit's caching feature to reduce query latency. If data is not current, you can uncomment the caching decorators (st.cache_resource and st.cache_data).''')
if st.button('Login'):
    get_token()

st.write('''To run this demo, ensure you have the following table in your Fabric Data Warehouse:''')

st.code(INIT_TABLE, language='sql')

#layouting
col1, col2 = st.columns(2)
with col1:
    st.button(f'Create {TABLE_SCHEMA}.{TABLE_NAME} Demo Table', on_click=execute_sql_command, args=[INIT_TABLE])
with col2:
    st.button(f'Drop {TABLE_SCHEMA}.{TABLE_NAME} Demo Table', on_click=execute_sql_command, args=[DROP_TABLE])

st.write('## View table data from Fabric Datawarehouse')
st.write(f'Shows the current data in the {TABLE_SCHEMA}.{TABLE_NAME} table. Will show an error if table does not exists.')
df = Select_query(QUERY)

st.table(df)

st.write('## Insert, update, and delete data in Fabric Datawarehouse')
st.markdown('''
        :blue-background[Note] Product ID column has been disabled because it is automatic incremental identity column.
        ''')
with st.expander("Data Editor Features"):
    st.markdown('''
        **Features:**
        - Toolbar appears when hovering over the table
        - Quick search for data
        - Export data as a CSV file
                
        **Editing Data:**
        - Double-click a cell to insert or edit data.
        - Supports copy/paste from and to Excel.
                
        **Deleting a Row:**
        - Select the row by clicking the leftmost column.
        - Press the Delete key or click the Trash bin icon on toolbar.
        ''')

st.write(f'Data editor for table: {TABLE_SCHEMA}.{TABLE_NAME}')
edited_df = st.data_editor(
    df, 
    column_config={
        "id": "Product ID",
        "name": "Product Name",
        "category": "Product Category"
    },
    disabled=["id"],
    hide_index=True,
    key="MyEditor", 
    num_rows="dynamic"
)

# st.write("Here's the value in Session State:")
# st.write(st.session_state["MyEditor"]) # 👈 Show the value in Session State

def submitPayload():
    print(st.session_state["MyEditor"])
    payloadJson = st.session_state["MyEditor"]

    insert_added_rows(payloadJson['added_rows'])
    delete_deleted_rows(payloadJson['deleted_rows'])
    update_edited_rows(payloadJson['edited_rows'])

submitted = st.button('Submit', on_click=submitPayload, type="primary")

if submitted:
    st.write("Payload submitted")