from datetime import datetime, timedelta
import requests as req

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

# Parametros API
query = 'data engineer'
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
end_time = datetime.now().date().strftime(TIMESTAMP_FORMAT)

url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

res = req.get(url_raw)

dados_total = []

if res.status_code == 200:
    print(f'Sucesso na requisição: {res.status_code}')
    dados = res.json()
    dados_total.append(dados)
else:
    print(f'Erro na requisição: {res.status_code}')
# print(json.dumps(dados['data'][0], indent=4, sort_keys=True))

while "next_token" in dados.get("meta",{}):
    next_token = dados['meta']['next_token']
    url = f"{url_raw}&next_token={next_token}"
    response = req.get(url)
    json_response = response.json()
    dados_total.append(json_response)
    # print(json.dumps(json_response, indent=4, sort_keys=True))
