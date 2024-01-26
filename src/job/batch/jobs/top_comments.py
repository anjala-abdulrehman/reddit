from datetime import datetime
import prawcore
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import time

from commons import Commons
from top_posts import Threads

from pymongo import MongoClient


class Comments:
    """
    Get comments posted on threads, for the sake of simplicity I am loading only comments for the top 25 threads
    The author fields from this class will be used to understand user activity, specifically to create
    the dim_all_users_tbl
    """
    CLIENT = MongoClient("localhost", 27017)
    DB = CLIENT.reddit_data
    COMMENTS = DB.reddit_comments
    COLLECTION = DB.reddit_comments

    def __init__(self,
                 subreddits: list,
                 execution_date: datetime,
                 commons: Commons):

        self.subreddits = subreddits
        self.commons = commons
        self.logger = commons.logger()
        self.execution_date = execution_date

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(prawcore.exceptions.TooManyRequests))
    def get_comments_for_thread(self, reddit, thread_id, subreddit_name) -> list[dict]:
        """
        Fetches data for a single thread in a subreddit
        :param subreddit_name: subreddit_name being analysed
        :param reddit: Reddit client
        :param thread_id: Single thread for which we are fetching data
        :return: list of comments as a dict
        """
        data_for_thread = []
        target_date = self.execution_date
        submission = reddit.submission(id=thread_id)
        if not submission.comments:
            return data_for_thread

        comments_list_all = submission.comments.list()
        ct = 0
        for comment in comments_list_all:
            try:
                comment_date = datetime.utcfromtimestamp(comment.created_utc)
                if comment_date.date() == target_date.date():
                    ct += 1
                    comment_dict = ({
                        'author_id': comment.id if comment.id else None,
                        'author_author': str(comment.author) if comment.author else None,
                        'author_score': comment.score if comment.score else None,
                        'author_created_utc': comment.created_utc if comment.created_utc else None,
                        'author_is_submitter': comment.is_submitter if comment.is_submitter else None,
                        'author_distinguished': comment.distinguished if comment.distinguished else None,
                        'author_edited': comment.edited if comment.edited else None,
                        'author_replies': len(comment.replies) if comment.replies else None
                    })
                    comment_dict = {k: v for k, v in comment_dict.items() if v is not None}
                    if comment_dict:
                        comment_dict["etl_ts"] = time.time()
                        comment_dict["subreddit"] = subreddit_name
                        data_for_thread.append(comment_dict)
            except Exception as e:
                if "MoreComments" in str(e):
                    self.logger.info(f"Unable to read collapsed comment for thread {thread_id}")
                else:
                    self.logger.info(f'comment with id: {comment.id} load failed with exception {e}')
        self.logger.info(f'got {ct} elements for {subreddit_name}, and thread_id {thread_id}')
        return data_for_thread

    def run(self) -> None:
        """

        :return:
        """

        reddit = self.commons.reddit_client()

        for subreddit in self.subreddits:
            submission_ids = self.commons.get_col_value_from_subreddit(subreddit, col_value='submission_id')

            for submission in submission_ids:
                data_for_subreddit = self.get_comments_for_thread(
                    thread_id=submission['submission_id'],
                    reddit=reddit,
                    subreddit_name=subreddit
                )

                if data_for_subreddit:
                    Comments.COLLECTION.insert_many(data_for_subreddit)


def main():
    commons = Commons(
        postgres_config='config/db_connection.yaml',
        postgres_db='reddit'
    )

    subreddit = Threads(num_subs_to_analyze=25, commons=commons)
    subs_to_analyze = subreddit.get_subreddits()
    comments_for_subs = Comments(
                commons=commons,
                execution_date=datetime.today().replace(hour=0, minute=0, second=0, microsecond=0),
                subreddits=subs_to_analyze)
    comments_for_subs.run()


if __name__ == '__main__':
    main()
