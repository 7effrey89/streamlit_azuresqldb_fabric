#streamlit example of how to connect to azure sql db using pyodbc

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

# 4. pip install streamlit / pip install --upgrade streamlit
# 4. pip install pyodbc sqlalchemy

# 5. Test that the installation worked..:
# streamlit hello
# If this doesn't work, use the long-form command:
# python -m streamlit hello
#our demo
# python -m streamlit run StreamlitDemo.py

#https://www.geeksforgeeks.org/a-beginners-guide-to-streamlit/
import streamlit as st
import pyodbc
import pandas as pd
from datetime import date
from sqlalchemy import create_engine

# Connection string
server = '<your-sql-server>.database.windows.net'
database = '<your-database>'
username = '<your-username>'
password = '<your-password>'
driver = 'ODBC Driver 17 for SQL Server'

# Create connection engine
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_string)

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

query = "SELECT top (150) name, address, type, date from dbo.Person;"

# rows = run_query(query)

# # Print results.
# for row in rows:
#     st.write(f"name: {row.name} | address: {row.address}")


df = pd.read_sql_query(query, engine)

st.table(df)

# create a Excel file with the results
# df.to_excel("FileExample.xlsx",sheet_name='Results')

# Create an empty dataframe on first page load, will skip on page reloads
if 'data' not in st.session_state:
    data = pd.DataFrame({'name':[],'address':[],'type':[],'date':[]})
    st.session_state.data = data

# Show current data
st.dataframe(st.session_state.data)


st.write('# Solution using input widgets')


# Function to append inputs from form into dataframe
def add_dfForm(num_rows):
    for r in range(num_rows):
        row = pd.DataFrame({'name':[st.session_state[f'input_col4{r}']],
                'address':'add' + str(st.session_state[f'input_col3{r}']),
                'type':['c'],
                'date': [date.today()]})
        st.session_state.data = pd.concat([st.session_state.data, row])
    
    df = st.session_state.data

    # Create connection engine
    # connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
    # engine = create_engine(connection_string, fast_executemany=True)

    #using sqlalchemy for faster inserts
    #https://stackoverflow.com/questions/63523711/inserting-data-to-sql-server-from-a-python-dataframe-quickly
    #Consider adding the following options to the to_sql call: if_exists='append', method='multi', chunksize=500 The chunk size will depend on how large or numerous your values are. With this I managed to write 20K rows similar to your data in 7 seconds. – 

    # Write DataFrame to SQL table
    df.to_sql('Person', engine, if_exists='append', index=False)



def add_row(row):
    # columns to lay out the inputs
    grid = st.columns(4)
    with grid[0]:
        st.text_input('Name', key=f'input_col1{row}', value='jef')
    with grid[1]:
        st.number_input('X', step=1, key=f'input_col2{row}', value=2)
    with grid[2]:
        st.number_input('Y', step=2, key=f'input_col3{row}', value=2)
    with grid[3]:
        st.text_input('NameXY', key=f'input_col4{row}',
                        value = st.session_state[f'input_col1{row}'] + str(st.session_state[f'input_col2{row}'] *
                            st.session_state[f'input_col3{row}']),
                        disabled=True)


num_rows = st.slider('Number of rows', min_value=1, max_value=10)

# Loop to create rows of input widgets
for r in range(num_rows):
    add_row(r)

checkbox_val = st.checkbox("Form checkbox")

# Every form must have a submit button.
submitted = st.button('Submit', on_click=add_dfForm, args=([num_rows]))

if submitted:
    st.write("Form submitted", "slider", num_rows, "checkbox", checkbox_val)
