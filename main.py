import streamlit as st
#Icons
#https://mui.com/material-ui/material-icons/

#this page is to define the navigation and the pages to be displayed. Will by default redirect user to the first page in the list
ShowSqlTbl = st.Page("ShowAzureSQLTable.py", title="Show Data From Azure Sql DB", icon=":material/language:")
InsertSqlTable = st.Page("InsertAzureSQLTable.py", title="Insert Data Into Azure Sql DB", icon=":material/add_circle:")
DataEditAzureSql = st.Page("DataEditorAzureSQTable.py", title="Data Editor for Azure Sql DB", icon=":material/bolt:")
DataEditFabWH = st.Page("DataEditorFabricWarehouse.py", title="Data Editor Fabric Data Warehouse", icon=":material/delete:")
ShowTblFabSqlEpt = st.Page("ShowFabricSqlEndpoint.py", title="Show Data From Fabric Sql Endpoint", icon=":material/storefront:")
DataEditFabLH = st.Page("DataEditorFabricLakehouse.py", title="Dirty Data Editor Fabric Lakehouse", icon=":material/waves:")
ShowFabricSqlEndpointForm = st.Page("ShowFabricSqlEndpointForm.py", title="Explore your Fabric Sql Endpoint", icon=":material/storefront:")

pg = st.navigation({
    "Azure SQL DB" : [ShowSqlTbl, InsertSqlTable, DataEditAzureSql],
    "Microsoft Fabric" : [ShowTblFabSqlEpt, DataEditFabWH, DataEditFabLH]
    })
st.set_page_config(page_title="Streamlit CRUD UI - Azure SQL - Microsoft Fabric", page_icon=":material/edit:")
pg.run()


# To run our demo
#pip install -r requirements.txt
# python -m streamlit run main.py