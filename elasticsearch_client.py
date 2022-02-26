import json
import re
from datetime import datetime, timedelta
from http.client import HTTPConnection
from time import sleep
from typing import List

import config
from config import elasticsearch
from log_config import get_logger

log = get_logger()

def get_oldest_date_in_indexes() -> datetime:
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", "/_cat/indices?format=json&pretty")
    response = connection.getresponse().read()

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

    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", f"_cat/indices/*{formatted_date}?bytes=b&format=json&pretty")
    response = str(connection.getresponse().read().decode())

    return json.loads(response)


def get_indexes_by_name(index_name_with_date: str) -> List:
    index_name_without_date = index_name_with_date[:-10]
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", f"_cat/indices/{index_name_without_date}*?s=index&bytes=b&format=json&pretty")

    response = str(connection.getresponse().read().decode())

    return json.loads(response)


def merge_single_index(index_source: str, index_target: str) -> str:
    body = {
        "source": {
            "index": index_source
        },
        "dest": {
            "index": index_target
        }
    }

    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("POST", "_reindex?wait_for_completion=false", body=json.dumps(body), headers={"Content-Type": "application/json"})
    response = str(connection.getresponse().read().decode())

    return json.loads(response)["task"]


def await_task(task_id: str):
    is_completed = False
    while not is_completed:
        connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
        connection.request("GET", f"/_tasks/{task_id}")
        response = str(connection.getresponse().read().decode())

        is_completed = json.loads(response)["completed"]

        log.debug(f"task_id={task_id} is_completed={is_completed}")

        sleep(config.app["DELAY_IN_SECONDS_BETWEEN_CHECK_MERGE_TASK_IN_ELASTICSEARCH"])


def delete_indexes(indexes: List):
    for index_name in indexes:
        delete_index(index_name)


def delete_index(index_name: str):
    log.debug(f"delete index={index_name}")
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("DELETE", f"/{index_name}")
