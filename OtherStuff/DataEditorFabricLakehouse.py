
# create table [dbo].[product] (
# 	id int identity(1,1),
# 	name varchar(100),
# 	category varchar(100)
# )
# This sample generates a delta table locally and syncs it to OneLake in a Fabric Lakehouse.
# It then reads the sql endpoint and displays the data in a Streamlit Data Editor.
# The Data Editor allows you to insert, update, and delete data into a local delta table and then sync it back to OneLake.
# This was made utterly complex as there is no need to work locally with a delta table. 
# We can simply just work with the delta tables directly on the lake which also has the added benefit of keeping the transaction log (delta log) intact and not constantly overwritten by the local copy.

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

# Initialize session state for credential and token if they don't exist
if 'credential' not in st.session_state:
    st.session_state['credential'] = None

#the endpoint url is the same for all sql endpoints on the workspace
#Fabric details
SQL_ENDPOINT = "x6eps4xrq2xudenlfv6naeo3i4-jhrvr47hgqiuxicx25egay43ey.msit-datawarehouse.fabric.microsoft.com" 
ACCOUNT_NAME = "onelake"
WORKSPACE_NAME = "jla_levelup"
LAKEHOUSE = "LH_LevelUp"
DATA_TABLES_PATH = f"{LAKEHOUSE}.Lakehouse/Tables"

#delta table details
DELTA_TABLE_NAME = "product"
DELTA_TABLE_SCHEMA_DEF = {
    'id': 'int64',
    'name': 'string',
    'category': 'string'
}
DELTA_TABLE_SAMPLE_DATA = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'category': ['car', 'book', 'movie']}

#table detailS used when querying the delta table throygh sql endpoint
DATABASE = LAKEHOUSE
TABLE_SCHEMA = 'dbo' #default schema for lakehouse tables   
TABLE_NAME = DELTA_TABLE_NAME
QUERY = f'select id, name, category from {TABLE_SCHEMA}.{TABLE_NAME};'

#full path to the delta table in onelake
FULL_TABLE_PATH = f"{DATA_TABLES_PATH}/{DELTA_TABLE_NAME}"

#temp folder to store the delta table
LOCAL_TEMP = f"./temp/{DELTA_TABLE_NAME}"

#Retrieves a DataLakeServiceClient object using the specified account name.
def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    # token_credential = DefaultAzureCredential()
    token_credential = st.session_state['credential']

    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client 

#Deletes a delta table in onelake
def DeleteDeltaTableInOneLake():
    with get_service_client_token_credential(ACCOUNT_NAME) as service_client:
        # Create a file system client
        file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)

        directory_client = create_directory(file_system_client, FULL_TABLE_PATH)

        delete_directory(directory_client)

#internal function to get directory_client to manipulate folders in onelake
def create_directory(file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
    directory_client = file_system_client.create_directory(directory_name)

    return directory_client

#internal function to delete a directory (delta table) in onelake
def delete_directory( directory_client: DataLakeDirectoryClient):
    directory_client.delete_directory()

def upload_file(file_path, file_system_client, data_files_path, folder_path):
    # Construct the Onelake path for the file
    relative_path = os.path.relpath(file_path, folder_path)
    azure_path = os.path.join(data_files_path, relative_path).replace("\\", "/")
    file_client = file_system_client.get_file_client(azure_path)
    
    with open(file_path, "rb") as data:
        print(f"Uploading {file_path} to {azure_path}...")  # Optional: print the upload status
        file_client.upload_data(data, overwrite=True)

def upload_folder(folder_path, file_system_client, data_files_path, max_workers=5):
    #max_workers parameter controls the number of concurrent threads
    files_to_upload = []
    
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            files_to_upload.append(file_path)

    #Uses ThreadPoolExecutor to perform concurrent uploads.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(upload_file, file_path, file_system_client, data_files_path, folder_path)
            for file_path in files_to_upload
        ]
        
        for future in futures:
            future.result()  # Wait for all uploads to complete


#Check if a table exists in the fabric lakehouse
def check_table_exists(table_name):
    result = safe_select_query(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    return not result.empty

def wait_for_table_update(table_name, timeout=60, check_interval=1):
    """
    Wait for the table to be updated with a fixed interval and timeout.

    :param table_name: Name of the table to check.
    :param timeout: Maximum time to wait for the update in seconds.
    :param check_interval: Interval between checks in seconds.
    :return: True if the table was updated within the timeout, False otherwise.
    """
    original_date = table_last_modified(table_name)
    start_time = time.time()

    while time.time() - start_time < timeout:
        time.sleep(check_interval)

        new_date = table_last_modified(table_name)
        
        #print(f"Original date: {original_date}, New date: {new_date}")
        if original_date != new_date:
            return True

    print(f"Timeout reached: Table {table_name} was not updated within {timeout} seconds.")
    return False

def table_last_modified(table_name):
    df = safe_select_query(f"SELECT modify_date FROM sys.tables WHERE name = '{table_name}'")

    #get the modify date of the table as a string
    return df['modify_date'][0]

#Wait for a few seconds before refreshing the UI. Mainly to allow the sql endpoint a few seconds to discover the new table we created in onelake
def waitRefreshUI(waitTime):
    with st.empty():
        for seconds in range(waitTime):
            st.write(f"⏳ Refreshing UI in {waitTime - seconds}!")
            time.sleep(1)
        st.write("✔️ Top table refreshed")

# Create a sample delta table in Fabric Lakehouse
def init_demo_table():
    df = pd.DataFrame(DELTA_TABLE_SAMPLE_DATA).astype(DELTA_TABLE_SCHEMA_DEF)

    Sync_DF_to_Onelake(df)

    waitRefreshUI(5)

#internal function to upload materialize a df as a delta table and then push it to onelake
def Sync_DF_to_Onelake(df):
    with get_service_client_token_credential(ACCOUNT_NAME) as service_client:
        # Create a file system client
        file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)

        # To write a DataFrame to Azure Data Lake, save it to a file first, then upload
        
        OneLakeDestination = FULL_TABLE_PATH

        #https://delta.io/blog/2023-04-01-create-append-delta-lake-table-pandas/
        write_deltalake(LOCAL_TEMP, df, mode="overwrite")

        file_client = file_system_client.get_file_client(OneLakeDestination)
        
        # Upload the saved files to OneLake 
        upload_folder(LOCAL_TEMP, file_system_client, OneLakeDestination)

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

# makes a staging dataframe of the delta table in fabric lakehouse combined with the changes from the data editor. Then syncs the changes to onelake.
def submitPayload(df):
    # print(st.session_state["MyEditor"])
    payloadJson = st.session_state["MyEditor"]

    #this is order essential to keep the row_index in sync for next operation in line
    df = modify_rows_in_dataframe(df, payloadJson['edited_rows'])
    df = remove_rows_from_dataframe(df, payloadJson['deleted_rows'])
    df = add_rows_to_dataframe(df, payloadJson['added_rows'])

    Sync_DF_to_Onelake(df)

#deletes the temp folder and the delta table in onelake
def clean_up():
    # Check if the temp delta table exists
    if os.path.exists(LOCAL_TEMP):
        # Delete the folder
        shutil.rmtree(LOCAL_TEMP)
        print(f"Folder '{LOCAL_TEMP}' has been deleted.")
    else:
        print(f"Folder '{LOCAL_TEMP}' does not exist.")
    #delete table in onelake
    DeleteDeltaTableInOneLake()
    waitRefreshUI(1)

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

# Check if table exists, if not, create it.

# if not status:
#     # auto init the table if it does not exist
#     init_demo_table()

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

    #if table exists load it into a df
    if status:
        df = safe_select_query(QUERY)

        #schema enforcement to ensure the id column is int and not float
        df = df.astype(DELTA_TABLE_SCHEMA_DEF)

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


        st.caption('If the table doesnt reflect your changes it might be due to the sql endpoint not being updated yet. \n\n Please refresh the page to see the changes or increase the wait time.')

        if submitted:
            st.write("Payload submitted - ⏳Waiting for changes to be discoverable at SQL Endpoint")
            with st.empty():
                status = wait_for_table_update(DELTA_TABLE_NAME)
                if status:
                    print("Table changes discovered at SQL Endpoint")

                    df = safe_select_query(QUERY).astype(DELTA_TABLE_SCHEMA_DEF)

                    overview.dataframe(df)
                else:
                    st.write("Table changes not discovered at SQL Endpoint")

                st.write("✔️ Top table refreshed")