import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from airflow.models import DAG
#from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago

with DAG(
    'api_labdados_consumer',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:

#    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    query = 'data engineer'
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
#    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
#    end_time = datetime.now().date().strftime(TIMESTAMP_FORMAT)

    to=TwitterOperator(file_path=os.path.join(
        '/opt/airflow/datalake/twitter_datascience',
        'extract_date={{ ds }}',
        'datascience_{{ ds_nodash }}.json'
    ),
    task_id='obtain_data', 
    query=query, 
    start_time='{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}', 
    end_time='{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}')
