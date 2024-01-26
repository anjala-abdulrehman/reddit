import pandas as pd
import time
from tenacity import retry, stop_after_attempt, wait_fixed
from commons import Commons
from pymongo import MongoClient
import datetime
import itertools
client = MongoClient("localhost", 27017)

db = client.reddit_data
sub_reddit = db.reddit_subs
collection = db.reddit_subs


class Threads:
    """
    Gets data for every thread in a Subreddit, for the sake of simplicity I am loading only comments
    for the top 25 threads. The author fields from this class will be used to understand user activity,
    specifically to create the dim_all_users_tbl
    """
    PATH = '../data/subreddit_public/subreddit_public.csv'

    def __init__(self,
                 num_subs_to_analyze: int,
                 commons: Commons,
                 execution_date: datetime):
        self.commons = commons
        self.num_subs_to_analyze = num_subs_to_analyze
        self.execution_date = execution_date
        self.logger = commons.logger()

    def get_subreddits(self) -> list:
        """
        Loads top sub reddit based on subscriber count, loads <num_subs_to_analyze> subs, in this case
        the number is set to 25
        :return: a list <num_subs_to_analyze> top subs
        """
        trending_subs = pd.read_csv(Threads.PATH, na_values=[None])
        trending_subs = trending_subs.sort_values(by='subscribers_count', ascending=False)
        return trending_subs.head(self.num_subs_to_analyze)['subreddit_name']

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
    def get_thread_in_subreddit(self, subreddit_name) -> list[dict]:
        """
        Returns json data for a thread in the Sub
        :param subreddit_name: name of the sub
        :return: a list of json holding data for the thread
        """
        reddit = self.commons.reddit_client()
        counter = itertools.count(start=1)


        current_date_utc = self.execution_date.strftime('%Y-%m-%d')
        subreddit = reddit.subreddit(subreddit_name)
        data_for_sub = []
        for submission in subreddit.hot(limit=100):
            post_date_utc = datetime.datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d')
            if post_date_utc == current_date_utc:
                try:
                    next(counter)
                    submission_dict = ({
                        'submission_title': submission.title if submission.title else None,
                        'submission_id': str(submission.id) if submission.id else None,
                        'submission_subreddit_id': submission.subreddit_id if submission.subreddit_id else None,
                        'submission_created': submission.created if submission.created else None,
                        'submission_banned_at_utc': submission.banned_at_utc if submission.banned_at_utc else None,
                        'submission_distinguished': submission.distinguished if submission.distinguished else None,
                        'submission_url': submission.url if submission.url else None,
                        'submission_num_comments': submission.num_comments if submission.num_comments else 0,
                        'submission_ups': submission.ups if submission.ups else None,
                        'submission_downs': submission.downs if submission.downs else None,
                        'submission_over_18': submission.over_18 if submission.over_18 else None,
                        'submission_removal_reason': submission.removal_reason if submission.removal_reason else None,
                        'submission_num_reports': submission.num_reports if submission.num_reports else None,
                        'submission_subreddit_type': submission.subreddit_type if submission.subreddit_type else None,
                        'submission_author_name': submission.author.name if submission.author.name else None,
                        'submission.author_id': submission.author.id if submission.author.id else None,
                        'submission_author_link_karma': submission.author.link_karma if submission.author.link_karma else None,
                        'submission_flair': submission.link_flair_text if submission.link_flair_text else None
                    })
                    submission_dict = {k: v for k, v in submission_dict.items() if v is not None}
                    if submission_dict:
                        submission_dict["etl_ts"] = time.time()
                        submission_dict["subreddit"] = subreddit.display_name
                        data_for_sub.append(submission_dict)
                except Exception as e:
                    self.logger.info(f'Loading submission id: {submission.id} failed with exception {e}')
                    continue
        self.logger.info(
            f'Proceeding to load {counter} items for subreddit {subreddit_name} for date {self.execution_date}')
        return data_for_sub

    def load_threads_in_subreddits_to_mongo_db(self) -> None:
        all_subreddits = self.get_subreddits()
        for sub in all_subreddits:

            self.logger.info(f'Fetching data for sub {sub}')
            all_data_for_sub = self.get_thread_in_subreddit(sub)
            if all_data_for_sub:
                collection.insert_many(all_data_for_sub)
        self.logger.info('Data load successful')


def main():
    commons = Commons(
        postgres_config='/config/db_connection.yaml',  # db config defined
        postgres_db='reddit'  # postgres db where data is to be loaded
    )
    threads = Threads(num_subs_to_analyze=25,
                      commons=commons,
                      execution_date=datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                      )
    """
    I chose Mongo db to store this data keeping in mind - 
    (a). future schema evolution, 
    (b). given the nature of the data, such as comments in a post etc
    (c). The volume expected is really high, and Mongo DB with its horizontally scalable nature is a better fit 
    """
    threads.load_threads_in_subreddits_to_mongo_db()


if __name__ == '__main__':
    main()
