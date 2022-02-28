import json
import re
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from time import sleep
from typing import List

import config
from config import elasticsearch
from log_config import get_logger

log = get_logger()


def is_valid_index_name(index_name_with_date: str) -> bool:
    # skipping already merged indexes
    match = re.search("logs-[0-9]{4}[.][0-9]{2}[.][0-9]{2}$", index_name_with_date)

    if match is not None:
        return True

    return False


def get_oldest_date_in_indexes() -> datetime:
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", "/_cat/indices?format=json&pretty")
    response = connection.getresponse().read()

    data = json.loads(response)
    oldest_date = datetime.now(tz=timezone.utc) - timedelta(1)  # yesterday

    for index in data:
        index_name_with_date = index["index"]

        if is_valid_index_name(index_name_with_date):
            row_date = index_name_with_date[-10:]
            current_date = datetime.strptime(row_date, '%Y.%m.%d').replace(tzinfo=timezone.utc)

            if current_date < oldest_date:
                oldest_date = current_date

    return oldest_date


def get_indexes_by_date(current_date: datetime) -> list[dict]:
    formatted_date = current_date.strftime("%Y.%m.%d")

    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", f"_cat/indices/*{formatted_date}?bytes=b&format=json&pretty")
    response = str(connection.getresponse().read().decode())

    return json.loads(response)


def get_indexes_by_name(index_name_with_date: str, date_start: datetime, date_end: datetime) -> list[dict]:
    log.debug(f"index_name_with_date={index_name_with_date}, date_start={date_start}")
    index_name_without_date = index_name_with_date[:-10]
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", f"_cat/indices/{index_name_without_date}*?s=index&bytes=b&format=json&pretty")

    response = str(connection.getresponse().read().decode())

    parsed_response = json.loads(response)

    filtered_response = []
    for index in parsed_response:
        index_name = index["index"]
        if is_valid_index_name(index_name):
            date_index = index_name[-10:]
            parsed_date_index = datetime.strptime(date_index, '%Y.%m.%d').replace(tzinfo=timezone.utc)

            if date_start <= parsed_date_index:
                if date_end >= parsed_date_index:
                    filtered_response.append(index)
                else:
                    log.debug(
                        f"filtered index_name={index_name} because parsed_date_index={parsed_date_index} newer than need data={date_end}")
            else:
                log.debug(
                    f"filtered index_name={index_name} because parsed_date_index={parsed_date_index} older than need data={date_start}")
        else:
            log.debug(f"filtered index_name={index_name} because index already merged")

    log.debug(
        f"found indexes.size={len(filtered_response)} for index_name_with_date={index_name_with_date}, start={date_start}, end={date_end}")

    return filtered_response


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
    connection.request("POST", "_reindex?wait_for_completion=false", body=json.dumps(body),
                       headers={"Content-Type": "application/json"})
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


def delete_indexes(indexes: list[str]):
    for index_name in indexes:
        delete_index(index_name)


def delete_index(index_name: str):
    log.debug(f"delete index={index_name}")
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("DELETE", f"/{index_name}")


def get_tmp_indexes() -> list[str]:
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request("GET", f"_cat/indices/*_tmp?bytes=b&format=json&pretty")
    response = str(connection.getresponse().read().decode())

    parsed_response = json.loads(response)

    tmp_indexes = []
    for index in parsed_response:
        tmp_indexes.append(index["index"])

    return tmp_indexes
