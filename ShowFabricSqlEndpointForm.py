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

# Uses st.cache_resource to only run once.
# The default lifetime of the token is 1 hour
#@st.cache_resource(ttl=3600)
def init_connection(sql_endpoint, database):
    """
    Initializes a connection to the database using the provided credentials.
    Only runs once and caches the result unless the sql_endpoint or database changes.

    Returns:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object representing the database connection.
    """

    if ('CustomEngine' not in st.session_state or 
        'sqlDB' not in st.session_state or 
        'sqlEndpoint' not in st.session_state or 
        st.session_state['sqlEndpoint'] != sql_endpoint or 
        st.session_state['sqlDB'] != database):
            # Do not use a global variable in streamlit. Streamlit always rerun the code from top to bottom when there is a change in the user interface. 
            # Instead use the built-in dictionary-like session_state
            # Create connection engine
            connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server={sql_endpoint},1433;Database={database};Encrypt=Yes;TrustServerCertificate=No"

            # prepare the access token
            #https://debruyn.dev/2023/connect-to-fabric-lakehouses-warehouses-from-python-code/#:~:text=1%20sql_endpoint%20%3D%20%22%22%20%23%20copy%20and%20paste,%3D%20f%22Driver%3D%7B%7BODBC%20Driver%2018%20for%20SQL%20Server%7D%7D%3BServer%3D%20%7Bsql_endpoint%7D%2C1433%3BDatabase%3Df%7Bdatabase%7D%3BEncrypt%3DYes%3BTrustServerCertificate%3DNo%22
            token_object = st.session_state['credential'].get_token("https://database.windows.net//.default") # Retrieve an access token valid to connect to SQL databases
            token_as_bytes = bytes(token_object.token, "UTF-8") # Convert the token to a UTF-8 byte string
            encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
            token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
            attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

            params = urllib.parse.quote_plus(connection_string)
            engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params), connect_args={'attrs_before': attrs_before}, echo=False)

            st.session_state['CustomEngine'] = engine
            st.session_state['sqlEndpoint'] = sql_endpoint
            st.session_state['sqlDB'] = database

    return st.session_state['CustomEngine']

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 1 sec (10 min=ttl=600).
# @st.cache_data(ttl=10)
def Select_query_exp(server, database, query):
    if st.session_state['credential'] is None:
        st.error("Please get the token first.")
        return pd.DataFrame()
    else:
        engine = init_connection(server, database)
        with engine.connect() as connection:
            result = pd.read_sql_query(query, connection.connection)
            return result

################################################ Page code Starts here ################################################

st.write('# Show table data from any Fabric SQL Analytics Endpoint')
st.write('Here you can test your connection to a SQL Analytics Endpoint in Microsoft Fabric. It will use your windows credentials to authenticate and execute your specified query.')
with st.expander("Jeffrey's Notes"):
    st.markdown('''
                - Fabric data warehoues and lakehouses are optimized for crunching big analytics workloads with its distributed compute architecture. 
                It is less ideal to use for transactional workloads such as fetching and updating individual rows. Such use case are better served with a traditional database such as Azure SQL db for the best performance.
                - From an architectural perspective modifying the data directly in a data warehouse might not be the best practice as audit trails and data governance might be compromised.
                - I'm using streamlit's caching feature to reduce query latency. If data is not current, you can uncomment the caching decorators (st.cache_resource and st.cache_data).''')

if st.button('Login'):
    get_token()

server = st.text_input('SQL Analytics Endpoint (All Fabric items in a workspace shares the same SQL Endpoint/Connectionstring)', key='SQL_ENDPOINT', value='x6eps4xrq2xudenlfv6naeo3i4-d5w5ahlsli3urayctd2h6xygo4.msit-datawarehouse.fabric.microsoft.com')
database = st.text_input('Database (e.g. name of the Fabric Warehouse item)', key='DATABASE', value='jla_fls_warehouse')
# schema = st.text_input('Schema', key='TABLE_SCHEMA', value='dbo')
# table = st.text_input('Table Name', key='TABLE_NAME', value='Address')
query = st.text_input('Query (Fabric is case sensitive)', key='QUERY', value=f'select top (1000) * from sys.tables;')

df = Select_query_exp(server, database, query)
st.dataframe(df)

