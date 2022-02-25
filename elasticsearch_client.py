import http
import json
import re
from datetime import datetime, timedelta
from typing import List

from config import elasticsearch

client = http.client.HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)


def get_oldest_date_in_indexes() -> datetime:
    response = client.request("GET", "/_cat/indices?format=json&pretty")

    data = json.loads(response)
    oldest_date = datetime.now() - timedelta(1)  # yesterday

    for index in data:
        index_name = index["index"]
        # skipping already merged indexes
        match = re.search("logs-[0-9]{4}[.][0-9]{2}[.][0-9]{2}$", index_name)

        if match is not None:
            row_date = index_name[-10:]
            current_date = datetime.strptime(row_date, '%Y.%m.%d')

            if current_date < oldest_date:
                oldest_date = current_date

    return oldest_date


def get_indexes_by_date(current_date: datetime) -> List:
    formatted_date = current_date.strftime("%Y.%m.%d")
    response = client.request("GET", f"_cat/indices/*{formatted_date}?bytes=b&format=json&pretty")

    return json.loads(response)


def get_indexes_by_name(index_name_with_date: str) -> List:
    index_name_without_date = index_name_with_date[0:len(index_name_with_date) - 10]
    response = client.request("GET", f"_cat/indices/{index_name_without_date}*?s=index&bytes=b&format=json&pretty")

    return json.loads(response)
