# This sets up the container with Python 3.10 installed.
FROM python:3.10-slim

# This copies everything in your current directory to the /app directory in the container.
COPY . /app

# This sets the /app directory as the working directory for any RUN, CMD, ENTRYPOINT, or COPY instructions that follow.
WORKDIR /app

# Update the package list and install unixodbc and the ODBC Driver 17 for SQL Server
RUN apt-get update && \
    apt-get install -y unixodbc unixodbc-dev && \
    apt-get install -y curl gnupg && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# This runs pip install for all the packages listed in your requirements.txt file.
RUN pip install -r requirements.txt

# This tells Docker to listen on port 80 at runtime. Port 80 is the standard port for HTTP.
EXPOSE 80

# This command creates a .streamlit directory in the home directory of the container.
#RUN mkdir ~/.streamlit

# This copies your Streamlit configuration file into the .streamlit directory you just created.
#RUN cp config.toml ~/.streamlit/config.toml

# Similar to the previous step, this copies your Streamlit credentials file into the .streamlit directory.
#RUN cp credentials.toml ~/.streamlit/credentials.toml

# This sets the default command for the container to run the app with Streamlit.
ENTRYPOINT ["streamlit", "run"]

# This command tells Streamlit to run your main.py script when the container starts.
CMD ["main.py"]

#building docker img:
#cd to this project folder

#start docker desktop

#docker build -f Dockerfile -t streamlit_azuresqldb_fabric .

#redicts traffic from 8501 to 80
#docker run -p 80:8501 streamlit_azuresqldb_fabric