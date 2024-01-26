import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.udf import ScalarFunction, udf
import os
import praw

from src.job.batch.jobs import commons


def create_processed_events_sink_kafka(t_env):
    """ Kafka sink schema"""
    table_name = "reddit_activity_events_kafka"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            subreddit VARCHAR,
            thread_id VARCHAR,
            author_id VARCHAR,
            creation_date VARCHAR,
            url VARCHAR,
            is_violation BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SSL',
            'properties.ssl.truststore.location' = '/var/private/ssl/kafka_truststore.jks',
            'properties.ssl.truststore.password' = '{os.environ.get("KAFKA_PASSWORD")}',
            'properties.ssl.keystore.location' = '/var/private/ssl/kafka_client.jks',
            'properties.ssl.keystore.password' = '{os.environ.get("KAFKA_PASSWORD")}',
            'format' = 'json'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    """ Kafka source schema"""

    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            subreddit VARCHAR,
            thread_id VARCHAR,
            author_id VARCHAR,
            creation_date VARCHAR,
            url VARCHAR,
            is_violation BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SSL',
            'properties.ssl.truststore.location' = '/var/private/ssl/kafka_truststore.jks',
            'properties.ssl.truststore.password' = 'bootcamp',
            'properties.ssl.keystore.location' = '/var/private/ssl/kafka_client.jks',
            'properties.ssl.keystore.password' = 'bootcamp',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


class FlagInappropriateContent(ScalarFunction):
    """
    Flags nsfw post in a non nsfw thread. This data can then be used to take down the post or take action on the
    author of the post
    """

    def __init__(self, submission_id):
        self.submission_id = submission_id

    def eval(self):
        reddit = commons.Commons.reddit_client()

        is_submission_over_18 = reddit.subreddit(id=self.submission_id).over_18

        subreddit_name = reddit.subreddit.display_name
        is_subreddit_over_18 = reddit.subreddit(subreddit_name).over_18

        return is_submission_over_18 and (not is_subreddit_over_18)


class RedditDataGenerator(ScalarFunction):

    def eval(self, subreddit_name):
        submission_data = dict()
        reddit = commons.Commons.reddit_client()
        subreddit = reddit.subreddit(subreddit_name)

        for submission in subreddit.hot(limit=10):
            submission_data = {
                'subreddit': subreddit_name,
                'thread_id': submission.id if submission.id is not None else None,
                'author_id': submission.author.id if submission.author.id is not None else None,
                'creation_date': str(submission.created_utc) if submission.created_utc is not None else None,
                'url': submission.url if submission.created_utc is not None else None
            }

        return json.dumps(submission_data)


reddit_data_generator = udf(RedditDataGenerator(), result_type=DataTypes.STRING())


def nsfw_flag_processing():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Setting parallelism
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)  # create table env
    t_env.create_temporary_function("reddit_data_generator", reddit_data_generator)
    is_thread_over18_in_not_nfsw_sub = udf(FlagInappropriateContent('thread_id'), result_type=DataTypes.BOOLEAN())

    try:
        source_table = create_events_source_kafka(t_env=t_env)
        sink_table = create_processed_events_sink_kafka(t_env)
        t_env.create_temporary_function("is_thread_over18_in_not_nfsw_sub", is_thread_over18_in_not_nfsw_sub)
        t_env.execute_sql(
            f"""
                    INSERT INTO {sink_table}
                    SELECT
                        subreddit,
                        thread_id,
                        author_id,
                        creation_date,
                        url,
                        is_thread_over18_in_not_nfsw_sub(thread_id) as is_violation
                    FROM {source_table}
                    """
        )
    except Exception as e:
        print(f'data load failed with error {e}')


if __name__ == '__main__':
    nsfw_flag_processing()
