import sys
import os
print(sys.path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from pathlib import Path
import json

class TwitterOperator(BaseOperator):

    template_fields = ['query', 'file_path', 'start_time', 'end_time']

    def __init__(self, file_path, query, start_time, end_time, **kwargs):
        self.file_path = file_path
        self.query = query
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):

        query = self.query
        start_time = self.start_time
        end_time = self.end_time

        self.create_parent_folder()

        with open(self.file_path, 'w') as output_file:
            for pg in TwitterHook(query=query, end_time=end_time, start_time=start_time).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write('\n')

if __name__ == "__main__":

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    query = 'data engineer'
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    end_time = datetime.now().date().strftime(TIMESTAMP_FORMAT)

    with DAG(
        'twitter_test',
        start_date=datetime.now(),
        schedule_interval='@daily'
    ) as dag:
        to=TwitterOperator(file_path=os.path.join(
            'datalake/twitter_datascience',
            f'extract_date={datetime.now().date().strftime('%Y-%m-%d')}',
            f'datascience_{datetime.now().date().strftime('%Y-%m-%d')}.json'
        ),task_id='test_run', query=query, start_time=start_time, end_time=end_time)
        ti=TaskInstance(task=to)
        to.execute(ti.task_id)
