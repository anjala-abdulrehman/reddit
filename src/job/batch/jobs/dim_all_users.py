from commons import Commons
from pymongo import MongoClient
from datetime import datetime, timedelta


class Users:
    """
    etl job that takes gets all users between a start date and an end date from Mongo db and writes it to Postgres
    """

    def __init__(self, commons: Commons, start_date, end_date):
        self.commons = commons
        self.start_date = start_date
        self.end_date = end_date
        self.mongo_client = MongoClient("localhost", 27017)
        self.db = self.mongo_client['reddit_data']
        self.subs = self.db['reddit_comments']

    def get_users(self):
        """Aggregates userdata between a start date and end date"""
        query = {'author_created_utc': {"$gte": self.start_date.timestamp(), "$lt": self.end_date.timestamp()}}
        projection = {'_id': 0, 'author_id': 1, 'etl_ts': 1}
        return self.subs.find(query, projection)

    def write_to_postgres(self):
        return self.commons.load_data_to_postgres(
            data=self.get_users(), fields=['author_id', 'etl_ts'])


if __name__ == '__main__':
    commons = Commons(
        postgres_config='/config/db_connection.yaml',
        postgres_db='reddit',
        postgres_table='dim_all_users_stg'
    )

    start_time = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=24, minutes=0, seconds=0)
    stg = Users(commons=commons, start_date=start_time, end_date=end_time)
    stg.write_to_postgres()
