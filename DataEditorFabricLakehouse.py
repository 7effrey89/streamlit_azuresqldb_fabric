
# create table [dbo].[product] (
# 	id int identity(1,1),
# 	name varchar(100),
# 	category varchar(100)
# )

import streamlit as st
import pandas as pd
from datetime import date, time
from sqlalchemy import create_engine, text
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
import struct
from itertools import chain, repeat
import urllib
import shutil
import os
import time
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from sqlalchemy.exc import SQLAlchemyError  # Assuming SQLAlchemy for database operations
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import numpy as np
from concurrent.futures import ThreadPoolExecutor

# Acquire a credential object
def get_token():
    # credential = DefaultAzureCredential() #will automatically use the available credential
    credential = InteractiveBrowserCredential() #forces for interactive login as DefaultAzureCredential would use the available credential associated with the hosting of the app instead of the
    st.session_state['credential'] = credential

def get_deltalake_conf():
    if 'credential' not in st.session_state:
        get_token()
    if 'datalake_conf' not in st.session_state:
        # Construct storage_options dictionary with retry settings
        datalake_conf = {
            'token': st.session_state['credential'].get_token("https://storage.azure.com/.default").token,
            'timeout': '100000s', 
            'retries': '20', 
            'retry_delay': '2',
        }
        st.session_state['datalake_conf'] = datalake_conf
    return st.session_state['datalake_conf']

# Initialize session state for credential and token if they don't exist
if 'credential' not in st.session_state:
    st.session_state['credential'] = None

#the endpoint url is the same for all sql endpoints on the workspace
#Fabric details
SQL_ENDPOINT = "<your-connectionstring>"  #e.g. 327tgddygq5ejgjqkdcgviwrja-xmh4dzc6lwgunnahc7xybkwxzy.datawarehouse.fabric.microsoft.com" 
ACCOUNT_NAME = "onelake"
WORKSPACE_NAME = "<your-workspace-name>"  #e.g. StreamlitdemoWorkspace
LAKEHOUSE = "<your-lakehouse-name>" #LH_Streamlitdemo
DATA_TABLES_PATH = f"{LAKEHOUSE}.Lakehouse/Tables"

#delta table details
DELTA_TABLE_NAME = "product"
DELTA_TABLE_SCHEMA_DEF = {
    'id': 'int64',
    'name': 'string',
    'category': 'string'
}
DELTA_TABLE_SAMPLE_DATA = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'category': ['car', 'book', 'movie']}
DATABASE = LAKEHOUSE
#full path to the delta table in onelake
FULL_TABLE_PATH = f"{DATA_TABLES_PATH}/{DELTA_TABLE_NAME}"

# Construct the URI for the Delta table
TABLE_URI = f"abfss://{WORKSPACE_NAME}@{ACCOUNT_NAME}.dfs.fabric.microsoft.com/{FULL_TABLE_PATH}"

# Create a sample delta table in Fabric Lakehouse
def init_demo_table():
    df = pd.DataFrame(DELTA_TABLE_SAMPLE_DATA)#.astype(DELTA_TABLE_SCHEMA_DEF)

    DeltaLakeOptions = get_deltalake_conf()
    # Write the DataFrame to a new Delta table
    write_deltalake(table_or_uri=TABLE_URI, 
                    storage_options=DeltaLakeOptions,
                    data=df,
                    mode="overwrite"
                    )

#Inserts added rows to a staging dataframe that is loaded with fabric data    
def add_rows_to_dataframe(df, added_rows):
    for row in added_rows:
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        # Check if 'id' column exists, if not, add it with NaN values
        if 'id' not in df.columns:
            df['id'] = np.nan

        # Generate a random number for each row if 'id' is NaN
        df['id'] = df['id'].apply(lambda x: np.random.randint(0, 100000) if pd.isna(x) else x)

    return df.astype(DELTA_TABLE_SCHEMA_DEF)

#modify rows to a staging dataframe that is loaded with fabric data   
def modify_rows_in_dataframe(df, edited_rows):
    for row_index, modifications in edited_rows.items():
        for column, new_value in modifications.items():
            df.at[int(row_index), column] = new_value
    return df

#deletes rows to a staging dataframe that is loaded with fabric data   
def remove_rows_from_dataframe(df, deleted_rows):
    df.drop(deleted_rows, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# makes a staging dataframe of the delta table in fabric lakehouse combined with the changes from the data editor. Then syncs the changes to onelake.
def submitPayload(df):
    # print(st.session_state["MyEditor"])
    payloadJson = st.session_state["MyEditor"]

    #this is order essential to keep the row_index in sync for next operation in line
    df = modify_rows_in_dataframe(df, payloadJson['edited_rows'])
    df = remove_rows_from_dataframe(df, payloadJson['deleted_rows'])
    df = add_rows_to_dataframe(df, payloadJson['added_rows'])

    # Write the DataFrame to a new Delta table
    DeltaLakeOptions = get_deltalake_conf()
    # Write the DataFrame to a new Delta table
    write_deltalake(table_or_uri=TABLE_URI, 
                    storage_options=DeltaLakeOptions,
                    data=df,
                    mode="overwrite"
                    )

def clean_up():
    #delete folder in onelake
    with get_service_client_token_credential(ACCOUNT_NAME) as service_client:
        file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)
        directory_client = file_system_client.create_directory(FULL_TABLE_PATH)
        directory_client.delete_directory()

#Retrieves a DataLakeServiceClient object using the specified account name.
def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    token_credential = st.session_state['credential']
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    return service_client 

# Uses st.cache_resource to only run once.
@st.cache_resource(ttl=5)
def init_connection():
    """
    Initializes a connection to the database using the provided credentials.

    Returns:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object representing the database connection.
    """
    # Do not use a global variable in streamlit. Streamlit always rerun the code from top to bottom when there is a change in the user interface. 
    # Instead use the built-in dictionary-like session_state
    if 'engineLH' not in st.session_state:
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
        engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params), connect_args={'attrs_before': attrs_before}, echo=True)

        st.session_state['engineLH'] = engine

    return st.session_state['engineLH']

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 1 sec (10 min=ttl=600).
# @st.cache_data(ttl=1)
def Select_query(query):
    if st.session_state['credential'] is None:
        st.error("Please get the token first.")
        return pd.DataFrame()
    else:
        engine = init_connection()
        with engine.connect() as connection:
            result = pd.read_sql_query(query, connection.connection)
            return result
        
#Check if a table exists in the fabric lakehouse
def check_table_exists(table_name):
    result = safe_select_query(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    return not result.empty

#adding retry mechanisme if the query fails
def safe_select_query(query, max_retries=3):
    """Attempt to run Select_query with a specified number of retries on failure."""
    for attempt in range(max_retries):
        try:
            df = Select_query(query)
            return df  # Query succeeded, return the result
        except SQLAlchemyError as e:  # Replace SQLAlchemyError with the specific exception Select_query might raise
            st.error(f"Query failed on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                st.error("Maximum retries reached, unable to fetch data.")
                return None  # or raise an exception
            # Optional: Add a delay between retries with time.sleep(seconds)

################################################ Page code Starts here ################################################
st.write('# Dirty Data Editor for delta table in Fabric Lakehouse/OneLake')
st.write('''
                This script demonstrates:
                1. How create a delta table outside of Microsoft Fabric and sync it to OneLake.
                2. How to use the Streamlit Data Editor to insert, update, and delete data in local delta table and sync it to OneLake.
         ''')
with st.expander("This solution is not recommended for production use"):
    st.markdown('''
                :red[This solution is not recommended for production use]. It is a simple demonstration of how to interact with a delta table in Fabric Lakehouse/OneLake. 
                While this process will enable Create-Read-Update-Delete (CRUD) operations on any delta table in a lakehouse, it will overwrite your transaction log (delta log) in the lakehouse with changes only originating from this editor.
                
                The process of this solution is as follows:
                1. (Optional, for initiation only) Create an delta table locally and upload it to the /Tables folder in Onelake of a Fabric Lakehouse. 
                2. Retrieve the data from a delta table in the Fabric Lakehouse through its SQL Endpoint and save it as a dataframe.
                3. Use the Streamlit Data Editor to insert, update, and delete data in the dataframe and then save it as a delta table locally
                4. Upload the delta table to the /Tables folder in Onelake of a Fabric Lakehouse.
                ''')

with st.expander("Jeffrey's Notes"):
    st.markdown('''
                - Fabric data warehoues and lakehouses are optimized for crunching big analytics workloads with its distributed compute architecture. 
                It is less ideal to use for transactional workloads such as fetching and updating individual rows. Such use case are better served with a traditional database such as Azure SQL db for the best performance.
                - From an architectural perspective modifying the data directly in a data warehouse might not be the best practice as audit trails and data governance might be compromised.
                - I'm using streamlit's caching feature to reduce query latency. If data is not current, you can uncomment the caching decorators (st.cache_resource and st.cache_data).''')
st.write('Step1: Get your Microsoft Azure Authorization Token (default validiaty 1 hour)')
if st.button('Login'):
    get_token()
st.write('Step2: Create Demo Table in Fabric Lakehouse')

st.code(f"#Check below function for how to create the {DELTA_TABLE_NAME} delta table: \n\ninit_demo_table()", language='python')

#layouting
col1, col2 = st.columns(2)
with col1:
    initbtn = st.button(f'Create {DELTA_TABLE_NAME} demo delta table', on_click=init_demo_table)
    if initbtn:
        st.write("Table created")
with col2:
    cleanBtn = st.button('Delete demo table', on_click=clean_up)
    if cleanBtn:
        st.write("Table deleted")

#check if user is logged in and got a token
if 'credential' in st.session_state:

    #check if delta table exists
    status = check_table_exists(DELTA_TABLE_NAME)
    get_deltalake_conf()
    #if table exists load it into a df
    if status:
        # Initialize the DeltaTable with the constructed URI and storage options
        dt = DeltaTable(table_uri=TABLE_URI, storage_options=st.session_state['datalake_conf'])

        # Convert the Delta table to a pandas DataFrame
        df = dt.to_pandas()

        st.write('## Current table data from Fabric Lakehouse')
        st.write(f'Shows the current data in the {DELTA_TABLE_NAME} table. Will show an error if table does not exists.')

        overview = st.dataframe(df)

        st.write('## Data Editor to insert, update, and delete data in Fabric Lakehouse')
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

        st.write(f'### Data Editor')
        col11, col22 = st.columns(2)
        with col11:
            st.write(f'Data editor for delta table: {DELTA_TABLE_NAME}')
            edited_df = st.data_editor(
                df, 
                column_config={
                    "id": "Prod. ID",
                    "name": "Prod. Name",
                    "category": "Prod. Category"
                },
                disabled=["id"],
                hide_index=True,
                key="MyEditor", 
                num_rows="dynamic"
            )
            submitted = st.button('Submit', on_click=submitPayload, args=[df], type="primary")
        with col22:
            st.write("Changes made from the Data Editor:")
            st.write(st.session_state["MyEditor"]) # 👈 Show the value in Session State
