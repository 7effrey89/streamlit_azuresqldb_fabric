#Basic test to see if the connection is working towards microsoft fabric sql endpoint
from azure.identity import DefaultAzureCredential
import struct
from itertools import chain, repeat
import pyodbc

# Acquire a credential object
credential = DefaultAzureCredential()

#the endpoint url is the same for all sql endpoints on the workspace
sql_endpoint = "x6eps4xrq2xudenlfv6naeo3i4-jhrvr47hgqiuxicx25egay43ey.msit-datawarehouse.fabric.microsoft.com" 
database = "LH_LevelUp"
database = "DW_LevelUp"

connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server={sql_endpoint},1433;Database={database};Encrypt=Yes;TrustServerCertificate=No"

# prepare the access token
token_object = credential.get_token("https://database.windows.net//.default") # Retrieve an access token valid to connect to SQL databases
token_as_bytes = bytes(token_object.token, "UTF-8") # Convert the token to a UTF-8 byte string
encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
cursor = connection.cursor()
cursor.execute("SELECT * FROM sys.tables")
rows = cursor.fetchall()
print(rows)

cursor.close()
connection.close()

#https://debruyn.dev/2023/connect-to-fabric-lakehouses-warehouses-from-python-code/#:~:text=1%20sql_endpoint%20%3D%20%22%22%20%23%20copy%20and%20paste,%3D%20f%22Driver%3D%7B%7BODBC%20Driver%2018%20for%20SQL%20Server%7D%7D%3BServer%3D%20%7Bsql_endpoint%7D%2C1433%3BDatabase%3Df%7Bdatabase%7D%3BEncrypt%3DYes%3BTrustServerCertificate%3DNo%22