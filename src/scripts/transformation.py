from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from os.path import join
import argparse

def get_tweets_data(df):
    df_tweet = df.select(sf.explode(df.data).alias('tweet'))\
            .select('tweet.author_id', 'tweet.conversation_id', 'tweet.created_at',
                    'tweet.id', 'tweet.public_metrics.*', 'tweet.text')
    return df_tweet

def get_users_data(df):
    df_user = df.select(sf.explode(df.includes.users).alias('user'))\
        .select('user.created_at', 'user.id', 'user.name', 'user.username')
    return df_user

def export_json(df, path):
    df.coalesce(1).write.mode("overwrite").json(path)

def twitter_transformation(spark, src, path, process_date):
    try:
        df = spark.read.json(src)
    except Exception as e:
        print(f"Erro ao ler o arquivo JSON: {e}")
        exit(1)

    df_tweet = get_tweets_data(df)
    df_user = get_users_data(df)

    table_path = join(path, "{table_name}", f'process_date={process_date}')

    export_json(df_tweet, table_path.format(table_name='tweet'))
    export_json(df_user, table_path.format(table_name='user'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Spark Labdados Transformation'
    )
    parser.add_argument('--src', required=True)
    parser.add_argument('--path', required=True)
    parser.add_argument('--process_date', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName('twitter_transformation') \
        .master("local") \
        .getOrCreate()

    twitter_transformation(spark, args.src, args.path, args.process_date)
