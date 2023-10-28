import json
import requests
import urllib.parse
import pandas as pd
from datetime import datetime, date
from config import configs_work, configs_dw
from utils import Saneamento, sw_work_to_dw, error_handler


def ingestion():
    listaPayload = []
    for key in configs_work:
        page = 1
        payload_list = []
        while True:
            params = urllib.parse.urlencode({'format': 'json', 'page': page})
            response = requests.get(configs_work[key]["endpoint"] + params).json()
            try:
                data = response['results']
                raw_path = configs_work[key]["raw_path"].replace('$id', str(page))
                with open(raw_path, "w") as final:
                    json.dump(data, final)
                payload = {key: raw_path}
                payload_list.append(payload)
            except Exception as exception_error:
                error_handler(exception_error, key, 'ingestion')
            if not response['next']:
                break
            page += 1
        listaPayload.extend(payload_list)
    return listaPayload

def preparation_work(payload):
    if len(payload) > 0:
        for key in payload:
            json_data = json.load(open(payload[key]))
            df = pd.DataFrame.from_records(json_data)
            san = Saneamento(df, configs_work, key)
            san.rename()
            san.normalize_dtype()
            san.normalize_null()
            san.tipagem()
            san.normalize_str()
            san.save_work()
    else:
        print("sem dados novos")
    return True

if __name__ == '__main__':
    listaPayload = ingestion()
    for payload in listaPayload:
        preparation_work(payload)
    sw_work_to_dw(configs_work, configs_dw)