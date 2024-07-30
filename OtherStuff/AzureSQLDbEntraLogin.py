#This script demonstrates how to connect to azure sql db using entra to authenticate
# remember setting: azure sql server needs to be able to accept entra login
# user needs to be created in the database, and if it's an web app, the app identity needs to be created instead   

#Step 1: Install Azure SQL DB Drivers
#https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15

# Step 2: Install packages
# pip install -r requirements.txt

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
import logging

# Acquire a credential object
# credential = DefaultAzureCredential() 
#pyodbc sample
#https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart?view=azuresql&tabs=windows%2Csql-inter

#sqlaclhemy sample
#https://stackoverflow.com/questions/53704187/connecting-to-an-azure-database-using-sqlalchemy-in-python

connection_string=f'Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:jlaflsmith.database.windows.net,1433;Database=AdventureWorks2022;Encrypt=yes;TrustServerCertificate=no'

# credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
credential = InteractiveBrowserCredential(additionally_allowed_tenants=['*'] )
token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

cursor = conn.cursor()
cursor.execute(f"select * from sys.tables")
conn.commit()
for row in cursor.fetchall():
    print(row.name)

#Troubleshooting
#https://techcommunity.microsoft.com/t5/azure-database-support-blog/aad-auth-error-login-failed-for-user-lt-token-identified/ba-p/1417535
   
