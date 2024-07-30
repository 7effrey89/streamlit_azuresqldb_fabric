# Create demo table on sql endpoint e.g. Azure SQL Database
# CREATE TABLE [dbo].[person](
# 	[name] [varchar](10) NULL,
# 	[address] [varchar](10) NULL,
# 	[type] [varchar](10) NULL,
# 	[date] [date] NULL
# ) 

#Inspiration:
#https://www.geeksforgeeks.org/a-beginners-guide-to-streamlit/

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

DATABASE = 'sandbox'
TABLE_SCHEMA = 'dbo'
TABLE_NAME = 'person'
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
    """
    Initializes a connection to the database using the provided credentials.

    Returns:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object representing the database connection.
    """
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
    engine = init_connection()
    with engine.begin() as conn:
        conn.execute(text(batch_command))

def CRUD_query(query):
    # To delete a row from the table
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit()  # Commit the transaction to make sure changes are saved

# Function to append inputs from form into dataframe
def submitForm(num_rows):
    """
    Add rows to a DataFrame and write it to an SQL table.

    Parameters:
    - num_rows (int): The number of rows to add to the DataFrame.
    """
    #wipe data from previous entry
    st.session_state.data = None

    #create new data from entry
    for r in range(num_rows):
        row = pd.DataFrame({'name':[st.session_state[f'input_col4{r}']],
                'address':'add' + str(st.session_state[f'input_col3{r}']),
                'type':['hardcoded C'],
                'date': [date.today()]})
        st.session_state.data = pd.concat([st.session_state.data, row])
    
    df = st.session_state.data

    #using sqlalchemy for faster inserts
    #https://stackoverflow.com/questions/63523711/inserting-data-to-sql-server-from-a-python-dataframe-quickly
    #Consider adding the following options to the to_sql call: if_exists='append', method='multi', chunksize=500 The chunk size will depend on how large or numerous your values are. With this I managed to write 20K rows similar to your data in 7 seconds. – 

    # Write DataFrame to SQL table
    df.to_sql(TABLE_NAME, con=engine, schema=TABLE_SCHEMA, if_exists='append', index=False)

# Function to add a row of input widgets
def GenerateFormRows(row):
    """
    Generate form rows for input fields.

    Parameters:
    - row (int): The row number.
    """
    # columns to lay out the inputs
    grid = st.columns(4)
    with grid[0]:
        st.text_input('Name', key=f'input_col1{row}')
    with grid[1]:
        st.number_input('X', step=1, key=f'input_col2{row}', value=2)
    with grid[2]:
        st.number_input('Y', step=2, key=f'input_col3{row}', value=2)
    with grid[3]:
        st.text_input('NameXY', key=f'input_col4{row}',
                        value = st.session_state[f'input_col1{row}'] + str(st.session_state[f'input_col2{row}'] *
                            st.session_state[f'input_col3{row}']),
                        disabled=True)

################################################ Page code Starts here ################################################

 # Initialize connection.   
engine = init_connection()


st.write('# Custom form for submitting data to Azure SQL Database')
st.write('This script demonstrate how to insert data into Azure SQL Database from a variety of widgets in streamlit. \
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

# Print results as table
st.table(df)

st.write('## Input form to insert data into Azure SQL Database')

# Create an empty dataframe on first page load, will skip on page reloads
if 'data' not in st.session_state:
    data = pd.DataFrame({'name':[],'address':[],'type':[],'date':[]})
    st.session_state.data = data

# Show current data
st.write("Shows the dataframe to be inserted into the database generated by the input widgets below.")
st.dataframe(st.session_state.data)

st.write('### Solution using input widgets')

# Number of rows to add
num_rows = st.slider('Number of rows', min_value=1, max_value=10)

# Loop to create rows of input widgets
for r in range(num_rows):
    GenerateFormRows(r)

# Every form must have a submit button.
submitted = st.button('Submit', on_click=submitForm, args=([num_rows]), type="primary")
st.markdown('''
    :blue-background[Note] Remember to click away from the name field for NameXY to re-render.
    ''')

# Just a dummy checkbox
checkbox_val = st.checkbox("Form checkbox",value=True)

# Show form submitted text
if submitted:
    waittime = 5
    with st.empty():
        for seconds in range(waittime):
            st.write(f"✔️ Form submitted, slider: {num_rows} checkbox: {checkbox_val} ({waittime - seconds})")
            time.sleep(1)
        st.write("")
