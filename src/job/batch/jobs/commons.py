from pymongo import MongoClient
import yaml
from typing import Dict, Any
import psycopg2
import praw
import logging


class Commons:
    """
    This class holds common methods that is required across ETL functionalities, This includes -
    (a). Create psql engine
    (b). Write to psql
    (c). Load data from psql
    (d). Write to Mongo DB
    (e). Read from Mongo DB
    """
    PREFIX = './reddit'
    CLIENT = MongoClient("localhost", 27017)
    MongoDocument = Dict[str, Any]

    def __init__(self,
                 postgres_config: str,
                 postgres_db: str,
                 postgres_table=None
                 ):
        self.postgres_config = f'{self.PREFIX}/{postgres_config}'
        self.mongo_db = self.CLIENT.reddit_data
        self.postgres_db = postgres_db
        self.postgres_table = postgres_table

    def get_col_value_from_subreddit(self, subreddit, col_value):
        return self.mongo_db.reddit_subs.aggregate([
            {"$match": {"subreddit": subreddit}},
            {"$group": {"_id": "$submission_id"}},
            {"$project": {col_value: "$_id", "_id": 0}}
        ])

    @staticmethod
    def reddit_client():
        return praw.Reddit(
            client_id='8R8L1KdzFW5NV79K4WDTXg',  ##os.environ.get('REDDIT_CLIENT_ID'),
            client_secret='kU0WiSIBeYQ0NN_GxG_W-dL_UjNcMQ',  ##os.environ.get('REDDIT_CLIENT_SECRET'),
            user_agent='cats_and_dogs'  # os.environ.get('REDDIT_USER_AGENT')
        )

    @staticmethod
    def logger():
        logging.basicConfig(format='%(asctime)s %(message)s')
        logger = logging.getLogger(logging.__name__)
        logger.setLevel(logging.INFO)
        return logger

    def create_postgres_engine(self):
        """
        Create a Postgres engine based on the provided configuration file.
        """
        with open(self.postgres_config, 'r') as file:
            database_config = yaml.safe_load(file)

        config = database_config[self.postgres_db]
        postgres_connection = psycopg2.connect(
            user=config['username'],
            host=config['host'],
            password=config['password'],
            port=config['port'],
            database=self.postgres_db
        )

        return postgres_connection

    def load_data_to_postgres(self, data, fields):
        """
        Load subreddit data to the specified Postgres database table.
        """
        pg_connection = self.create_postgres_engine()
        pg_cursor = pg_connection.cursor()

        for document in data:
            field_vals = [str(document.get(field)) for field in fields]
            query = f"""
                        INSERT INTO {self.postgres_table} 
                        ({', '.join(fields)}) VALUES ('{field_vals[0]}', TO_TIMESTAMP({field_vals[1]})::date)
                        """
            pg_cursor.execute(query)

        pg_connection.commit()
        pg_cursor.close()
        pg_connection.close()
