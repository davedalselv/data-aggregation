# DataFrame aggregation and saving to Database

Steps:
- Connecting to a psql Database
- Creating the production table and loading with test data (if "setup" argument is set)
- Creating the daily table (if "setup" argument is set)
- Loading data from the production table to a DataFrame
- Transforming the DataFrame by aggregating old and adding new columns
- Saving the new DataFrame in the daily table
- Exporting the new DataFrame to a csv file
- Closing connection

# Setup

Activate virtual environment
```
python -m venv env
source env/bin/activate
```

Install packages
```
pip install -r requirements.txt
```

Setup a psql database.

Create a .env file in the project root folder and add the details of your database to establish a connection.
```
HOST=<your_host>
PORT=<your_port>
DBNAME=<your_dbname>
USER=<your_user>
```

# Usage

To run the project for the first time
```
python aggregator.py setup
```
To update the database (in case the values in the prod table change)
```
python aggregator.py
```

# Additional info

I created a Jupyter file to explore the data and play around with some possible solutions before creating the aggregator.py file.
