import os
import psycopg2 as ps
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import sys

class Aggregator:

    def __init__ (self):
        self.conn = None
        self.cursor = None
        self.df = None

    def connect(self):
        load_dotenv()

        self.conn = ps.connect(
            host=os.environ['HOST'],
            port=os.environ['PORT'],
            dbname=os.environ['DBNAME'],
            user=os.environ['USER']
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        conn_string = f'postgresql://{os.environ["USER"]}@{os.environ["HOST"]}:{os.environ["PORT"]}/{os.environ["DBNAME"]}'
        db = create_engine(conn_string)
        self.alchemy_conn = db.connect()
        print('Database connection created.')

    def create_table(self, path):
        query = open("sql/" + path).read()
        self.cur.execute(query)
        print(f'Table created by {path}.')

    def load_df(self):
        self.df = pd.read_sql_query('select * from prod', con=self.conn)
        print("DataFrame loaded.")

    def transform_df(self):
        aggregated_df = self.df.copy()
        aggregated_df['FloatingObligation'] = aggregated_df['floatinbligationvolume'] * aggregated_df['hubprice']
        aggregated_df['MerchantRevenue'] = aggregated_df['mwh_del'] * aggregated_df['nodeprice']

        aggregated_df = aggregated_df.resample('D', on='datetime').agg({
            'mwh_del':'sum',
            'nodeprice':'mean',
            'hubprice':'mean',
            'floatinbligationvolume': 'sum',
            'FloatingObligation' : 'sum',
            'MerchantRevenue': 'sum'})

        aggregated_df = aggregated_df.drop(columns=['floatinbligationvolume'])
        aggregated_df.rename(columns = {'mwh_del':'Production', 'nodeprice':'PowerPriceNode', 'hubprice': 'PowerPriceHub'}, inplace = True)
        self.df = aggregated_df
        print("DataFrame transformed.")

    def push_df_to_database(self):
        self.df.to_sql('daily', con=self.alchemy_conn, if_exists='replace')
        print("DataFrame pushed to the Database.")

    def export_to_csv(self):
        self.df.to_csv('transformed_data/daily_data.csv')
        print("DataFrame exported to csv")

    def close_connection(self):
        self.cur.close()
        self.conn.close()
        print('Database connection closed.')


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.connect()

    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        aggregator.create_table("ProdTable.sql")
        aggregator.create_table("DailyTable.sql")

    aggregator.load_df()
    aggregator.transform_df()
    aggregator.push_df_to_database()
    aggregator.export_to_csv()
    aggregator.close_connection()
