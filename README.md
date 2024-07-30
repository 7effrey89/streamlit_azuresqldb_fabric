# Custom Data Entry UI for Azure SQL Database and Microsoft Fabric Warehouse and Lakehouse:
![image](https://github.com/user-attachments/assets/7d768b49-1483-4056-ab3e-924366af3f9c)


This demo uses a python framework called streamlit to accelerate the development of browser based solutions.
In this demo you will find pages that enables you:

Azure SQL DB:
- Embed table from Azure SQL DB as native text on a website
- Custom made form that can be dyncamilly controlled to submit one or multiple inputs to a table in Azure SQL DB
- Spreadsheet alike Data Editor to Insert, Update, Delete rows in an Azure SQL DB table
  
Microsoft Fabric:
- Show table data from any SQL Endpoint in Microsoft Fabric (e.g. Warehouse, Lakehouse)
- Spreadsheet alike Data Editor to Insert, Update, Delete rows in an Microsoft Fabric Warehouse
- Spreadsheet alike Data Editor to Insert, Update, Delete rows in an Microsoft Fabric Lakehouse 

# Video demonstration of the solution 
https://youtu.be/bSH6AZi9C8k

## Getting Started:
Open a Terminal in VS Code

Download the code to your machine:

``` git clone https://github.com/7effrey89/streamlit_azuresqldb_fabric.git```

Configure your Azure SQL Database connection
Modify .streamlit/secrets.toml with your details:
``` server = "<azuresql-resource-name>.database.windows.net"
 database = "<database-name>"
 username = "<sql-username>"
 password = "<sql-password>"
```
Modify each of the .py files to target the table you want to interact with:

```
Example: DataEditorAzureSQTable.py:

DATABASE = '<name-of-database>'
TABLE_SCHEMA = '<name-of-schema>'
TABLE_NAME = '<name-of-table>'
```

Run the streamlit app:

``` python -m streamlit run main.py```

## Structure - designed for adoption:
- Each of the .py files represent a webpage in streamlit.
- I've decided to write each page as if it was a standalone python script; making it easy for you to paste into your own solutions. The only dependencies each page has is to the .streamlit/secrets.toml file that contains the global variable of your azure sql db connection details.
- If you are here for only the python logic, you can remove all streamlit related code (typically all with st.xxxxx like replacing st.session_state['xx'] with a variable, st.write/st.table with print('xxx') etc.
- main.py serve as the place to configure the left navigation menu for the streamlit app. Here you can easily remove and add more webpages.
  
## Troubleshooiting
I've included a troubleshooting.txt with all the packages and their listed versions that was installed on my machine - just in case that becomes relevant for you one day...

## Docker
I've also made the solution ready for you to run it on a container. You can modify the solutions to your liking - if you add more packages to the solution, remember to include them in the requirement.txt file so they will be included in the docker image when you build it.

1. : Open terminal in VS Code, and make sure you are located in the root folder of the project e.g.:

``` cd c:\streamlit_azuresqldb_fabric ``` 

2. start docker desktop on your local machine

3. Building Docker Image using the details from the file Dockerfile, and call it streamlit_azuresqldb_fabric

```docker build -f Dockerfile -t streamlit_azuresqldb_fabric . ```

4. Run the docker Image, and redicts traffic from 8501 to 80

```docker run -p 80:8501 streamlit_azuresqldb_fabric```


