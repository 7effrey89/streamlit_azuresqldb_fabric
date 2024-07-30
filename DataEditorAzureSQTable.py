
# This script demonstrates how to use the Streamlit Data Editor to insert, update, and delete data in an Azure SQL Database table.

# To run this demo, ensure you have the following table in your Azure SQL Database:
# create table [dbo].[product] (
# 	id int identity(1,1),
# 	name varchar(100),
# 	category varchar(100)
# )
# Add some table data to it
# INSERT INTO [dbo].[product] (name, category)
# VALUES 
# ('Product A', 'Category 1'),
# ('Product B', 'Category 2'),
# ('Product C', 'Category 3');

import time
import streamlit as st
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text

# Connection string
SERVER = st.secrets["server"]
USERNAME = st.secrets["username"]
PASSWORD = st.secrets["password"]
DRIVER = st.secrets["driver"]

# Database and table details
DATABASE = 'sandbox'
TABLE_SCHEMA = 'dbo'
TABLE_NAME = 'product'
QUERY = f'select id, name, category from {TABLE_SCHEMA}.{TABLE_NAME};'
DROP_TABLE = f"DROP TABLE {TABLE_SCHEMA}.{TABLE_NAME}"
INIT_TABLE = f"""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TABLE_NAME}' AND schema_id = SCHEMA_ID('{TABLE_SCHEMA}'))
CREATE TABLE {TABLE_SCHEMA}.{TABLE_NAME} (
    id INT IDENTITY(1,1),
    name VARCHAR(100),
    category VARCHAR(100)
);
--Insert sample data
INSERT INTO {TABLE_SCHEMA}.{TABLE_NAME} (name, category)
VALUES 
('Product A', 'Category 1'),
('Product B', 'Category 2'),
('Product C', 'Category 3');
"""

# Uses st.cache_resource to only run once.
#@st.cache_resource
def init_connection():
    """
    Initializes a connection to the database using the provided credentials.

    Returns:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object representing the database connection.
    """
    # Create connection engine
    connection_string = f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
    engine = create_engine(connection_string, echo=True)
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
    engine = init_connection()
    with engine.begin() as conn:
        conn.execute(text(batch_command))

# CRUD operations
def insert_added_rows(added_rows):
    if not added_rows:
        return
    insert_commands = [f"INSERT INTO {TABLE_SCHEMA}.{TABLE_NAME} (name, category) VALUES ('{row['name']}', '{row['category']}')" for row in added_rows]
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

st.write('# Data Editor for Azure SQL Database Table')
st.write('This script demonstrates how to use the Streamlit Data Editor to insert, update, and delete data in an Azure SQL Database table. \
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
df = Select_query(QUERY)

st.table(df)

st.write('## Insert, update, and delete data in Azure SQL Database')
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
        "name": "Product Category"
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
    waittime = 5
    with st.empty():
        for seconds in range(waittime):
            st.write(f"✔️ Payload submitted! ({waittime - seconds})")
            time.sleep(1)
        st.write("")


