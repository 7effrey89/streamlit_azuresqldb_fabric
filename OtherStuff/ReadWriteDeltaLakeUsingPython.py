import pandas as pd
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
# from azure.storage.filedatalake import (
#     DataLakeServiceClient,
#     DataLakeDirectoryClient,
#     FileSystemClient
# )
from deltalake import DeltaTable
from deltalake.writer import write_deltalake


# credential = DefaultAzureCredential() #will automatically use the available credential
credential = InteractiveBrowserCredential() #will prompt you

# the endpoint url is the same for all sql endpoints on the workspace
# Fabric details
ACCOUNT_NAME = "onelake"
WORKSPACE_NAME = "<your-workspace-name>"
LAKEHOUSE = "<your-lakehouse-name>"
DATA_TABLES_PATH = f"{LAKEHOUSE}.Lakehouse/Tables"

#delta table details
DELTA_TABLE_NAME = "<Name-of-existing-delta-table>"

#full path to the delta table in onelake
FULL_TABLE_PATH = f"{DATA_TABLES_PATH}/{DELTA_TABLE_NAME}"

# Construct the URI for the Delta table
table_uri_source = f"abfss://{WORKSPACE_NAME}@{ACCOUNT_NAME}.dfs.fabric.microsoft.com/{FULL_TABLE_PATH}"
table_uri_destination = f"abfss://{WORKSPACE_NAME}@{ACCOUNT_NAME}.dfs.fabric.microsoft.com/{DATA_TABLES_PATH}/{DELTA_TABLE_NAME}_myClone"

# Define the storage options variable
storage_options = {'token': credential.get_token("https://storage.azure.com/.default").token}
# storage_options = {"bearer_token": credential.get_token("https://storage.azure.com/.default").token, "use_fabric_endpoint": "true"} #i see a lot of people using this, but the above also works...

# Load existing delta table from Fabric
dt = DeltaTable(table_uri=table_uri_source, storage_options=storage_options)

# Convert the Delta table to a pandas DataFrame
df = dt.to_pandas()

# If you dont have a table in Fabric, you can create a DataFrame like this to push as a new table in the lakehouse
# df = pd.DataFrame(
#     {
#         "a": [1, 2, 3, 4, 5],
#         "fruits": ["banana", "orange", "mango", "apple", "mandarin"],
#     }
# )

# Print the DataFrame
print(df)

# Write the DataFrame to a new Delta table
write_deltalake(table_or_uri=table_uri_destination, 
                storage_options=storage_options,
                data=df,
                mode="overwrite"
                )
