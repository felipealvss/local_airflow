from airflow.providers.http.hooks.http import HttpHook
import requests as req
from datetime import datetime, timedelta
import json

class TwitterHook(HttpHook):
    def __init__(self, start_time, end_time, query, conn_id=None):
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time
        self.query = query
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        # Parametros API
        query = self.query
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        start_time = self.start_time
        end_time = self.end_time

        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw

    def connect_to_endpoint(self, url, session):
        res = req.Request("GET", url)
        prep = session.prepare_request(res)
        self.log.info(f'URL: {url}')
        return self.run_and_check(session, prep, {})

    def paginate(self, url_raw, session):
        
        dados_total = []
        limit = 0
        req_limit = 100

        res = self.connect_to_endpoint(url_raw, session)
        dados = res.json()
        dados_total.append(dados)

        while "next_token" in dados.get("meta",{}) and limit < req_limit:
            next_token = dados['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url, session)
            json_response = response.json()
            dados_total.append(json_response)
            limit += 1

        return dados_total

    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()

        return self.paginate(url_raw, session)
    
if __name__ == "__main__":

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    query = 'data engineer'
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    end_time = datetime.now().date().strftime(TIMESTAMP_FORMAT)

    for pg in TwitterHook(query=query, end_time=end_time, start_time=start_time).run():
        print(json.dumps(pg, indent=4, sort_keys=True))

dicas_uso = '''
    Admin/Connections
    Add
    Connection Id = twitter_default
    Connection Type = Http
    Host = https://labdados.com
    Extra = {"Authorization": "Bearer XXXXXXXXXXXXXXXXXXXXXX"}

docker exec -it local_airflow-airflow-webserver-1 /bin/bash
export AIRFLOW_HOME=$(pwd)/local_airflow
python hook/twitter_hook.py
'''