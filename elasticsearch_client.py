import http
import json
import re
from datetime import datetime, timedelta

from config import elasticsearch


client = http.client.HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)


def get_oldest_date_in_indexes():
    response = client.request("GET", "/_cat/indices?bytes=b&format=json&pretty")

    data = json.loads(response)
    oldest_date = datetime.now() - timedelta(1)  # yesterday

    for index in data:
        index_name = index["index"]
        match = re.search("logs-[0-9]{4}[.][0-9]{2}[.][0-9]{2}$", index_name)

        if match is not None:
            row_date = index_name[-10:]
            current_date = datetime.strptime(row_date, '%Y.%m.%d')

            if current_date < oldest_date:
                oldest_date = current_date

    return oldest_date
