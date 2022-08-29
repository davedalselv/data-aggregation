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
