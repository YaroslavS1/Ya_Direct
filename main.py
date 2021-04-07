import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import CONFIG

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', -1)

# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import sys

if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x

class get_stat:
    def __init__(self):
        self.ReportsURL = 'https://api-sandbox.direct.yandex.com/json/v5/reports'

        self.token = CONFIG.token

        self.headers = {
                "Authorization": "Bearer " + self.token,
                "Accept-Language": "ru",
                "processingMode": "auto"
                }

        # Создание тела запроса
        body = {
                "params": {
                    "SelectionCriteria": {},
                    "FieldNames": [
                        "Date",
                        "CampaignName",
                        "Impressions",
                        "Clicks",
                        "Ctr",
                        "Cost",
                        "AvgCpc",
                        "AvgPageviews",
                        "Profit",
                        "Conversions",
                        "Revenue"
                    ],
                    "ReportName": u("ReportLast90_29"),
                    "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                    "DateRangeType": "LAST_90_DAYS",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                }
            }
        self.body = json.dumps(body, indent=4)
    def save_stat(self):
        while True:
            try:
                req = requests.post(self.ReportsURL, self.body, headers=self.headers)
                req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
                if req.status_code == 400:
                    print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(u(self.body)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 200:
                    format(u(req.text))
                    break
                elif req.status_code == 201:
                    print("Отчет успешно поставлен в очередь в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 202:
                    print("Отчет формируется в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 500:
                    print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 502:
                    print("Время формирования отчета превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print("JSON-код запроса: {}".format(self.body))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                else:
                    print("Произошла непредвиденная ошибка")
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(self.body))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break

            except ConnectionError:
                print("Произошла ошибка соединения с сервером API")
                break
            except:
                print("Произошла непредвиденная ошибка")
                break

        file = open("cashe4.csv", "w")
        file.write(req.text)
        file.close()

a = get_stat()
a.save_stat()
f = pd.read_csv("cashe4.csv",header=1, sep='	', index_col=0,)
print(f)