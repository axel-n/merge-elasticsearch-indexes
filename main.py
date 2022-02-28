import logging
from typing import List
import json
import re
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from time import sleep

###############################
# config
###############################
elasticsearch = dict(
    HOST="localhost",
    PORT="9200",
    USERNAME="admin",
    PASSWORD="admin",
    MAX_INDEX_SIZE_IN_GIGABYTES=0.0001,
    MAX_INDEX_DAY_MERGE=3
)
app = dict(
    LOG_LEVEL=logging.INFO,
    DELAY_IN_SECONDS_BETWEEN_CHECK_MERGE_TASK_IN_ELASTICSEARCH=1
)
###############################
# end config
###############################


console_format = "%(asctime)s %(levelname)s %(filename)s.%(funcName)s:%(lineno)s - %(message)s"

console_formatter = logging.Formatter(
    fmt=console_format,
    datefmt="%Y-%m-%d %H:%M:%S"
)

root_logger = logging.getLogger()
root_logger.setLevel(app["LOG_LEVEL"])

console_handler = logging.StreamHandler()
console_handler.setLevel(app["LOG_LEVEL"])
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

base64_token = b64encode((elasticsearch["USERNAME"] + ":" + elasticsearch["PASSWORD"]).encode('ascii')).decode("ascii")
headers = {"Authorization": "Basic " + base64_token}


def get_logger():
    return root_logger


log = get_logger()

ONE_GIGABYTE_IN_BYTES = 1024 * 1024 * 1024
MAX_INDEX_SIZE_IN_BYTES = ONE_GIGABYTE_IN_BYTES * elasticsearch["MAX_INDEX_SIZE_IN_GIGABYTES"]


def is_valid_index_name(index_name_with_date: str) -> bool:
    # skipping already merged indexes
    match = re.search("logs-[0-9]{4}[.][0-9]{2}[.][0-9]{2}$", index_name_with_date)

    if match is not None:
        return True

    return False


def get_oldest_date_in_indexes() -> datetime:
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request(method="GET", url="/_cat/indices?format=json&pretty", headers=headers)
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

    connection = HTTPConnection(host=elasticsearch["HOST"], port=elasticsearch["PORT"], timeout=45)
    connection.request(method="GET", url=f"_cat/indices/*{formatted_date}?bytes=b&format=json&pretty", headers=headers)
    response = str(connection.getresponse().read().decode())

    return json.loads(response)


def get_indexes_by_name(index_name_with_date: str, date_start: datetime, date_end: datetime) -> list[dict]:
    log.debug(f"index_name_with_date={index_name_with_date}, date_start={date_start}")
    index_name_without_date = index_name_with_date[:-10]
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request(method="GET", url=f"_cat/indices/{index_name_without_date}*?s=index&bytes=b&format=json&pretty",
                       headers=headers)

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

    custom_headers = dict()
    custom_headers["Content-Type"] = "application/json"
    custom_headers["Authorization"] = "Basic " + base64_token
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request(method="POST", url="_reindex?wait_for_completion=false", body=json.dumps(body),
                       headers=custom_headers)
    response = str(connection.getresponse().read().decode())

    return json.loads(response)["task"]


def await_task(task_id: str):
    is_completed = False
    while not is_completed:
        connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
        connection.request(method="GET", url=f"/_tasks/{task_id}", headers=headers)
        response = str(connection.getresponse().read().decode())

        is_completed = json.loads(response)["completed"]

        log.debug(f"task_id={task_id} is_completed={is_completed}")

        sleep(app["DELAY_IN_SECONDS_BETWEEN_CHECK_MERGE_TASK_IN_ELASTICSEARCH"])


def delete_indexes(indexes: list[str]):
    for index_name in indexes:
        delete_index(index_name)


def delete_index(index_name: str):
    log.debug(f"delete index={index_name}")
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request(method="DELETE", url=f"/{index_name}", headers=headers)


def get_tmp_indexes() -> list[str]:
    connection = HTTPConnection(elasticsearch["HOST"], elasticsearch["PORT"], timeout=45)
    connection.request(method="GET", url=f"_cat/indices/*_tmp?bytes=b&format=json&pretty", headers=headers)
    response = str(connection.getresponse().read().decode())

    parsed_response = json.loads(response)

    tmp_indexes = []
    for index in parsed_response:
        tmp_indexes.append(index["index"])

    return tmp_indexes


def get_indexes_for_merge(indexes_by_current_index_name: list[dict]) -> tuple[list[str], float, int, bool]:
    indexes_for_merge = list()

    current_size_in_bytes_indexes_for_merge = 0
    current_count_days_for_merge = 0

    is_reach_limits = False

    for index in indexes_by_current_index_name:
        index_size = int(index["pri.store.size"])
        index_name = index["index"]

        if current_count_days_for_merge <= elasticsearch["MAX_INDEX_DAY_MERGE"]:
            new_index_size = current_size_in_bytes_indexes_for_merge + index_size
            if new_index_size <= MAX_INDEX_SIZE_IN_BYTES:

                indexes_for_merge.append(index_name)
                current_size_in_bytes_indexes_for_merge += index_size
                current_count_days_for_merge += 1
            else:
                log.debug(f"skipping index={index_name} because size={new_index_size}bytes to large")
                is_reach_limits = True
                break
        else:
            log.debug(f"skipping index={index_name} because days to large")
            is_reach_limits = True
            break

    size_in_gigabytes_indexes_for_merge = \
        float("{:.4f}".format(current_size_in_bytes_indexes_for_merge / ONE_GIGABYTE_IN_BYTES))

    return indexes_for_merge, size_in_gigabytes_indexes_for_merge, current_count_days_for_merge, is_reach_limits


def rename_tmp_index_to_final_name(index_temp_for_marge, index_with_date_end):
    date_end = index_with_date_end[-10:]
    index_final_name = index_temp_for_marge[0:len(index_temp_for_marge) - 4] + "-" + date_end
    task_id = merge_single_index(index_temp_for_marge, index_final_name)
    await_task(task_id)


def merge_indexes(indexes_for_merge: List):
    index_with_date_start = indexes_for_merge[0]
    index_with_date_end = indexes_for_merge[len(indexes_for_merge) - 1]
    index_temp_for_merge = index_with_date_start + "_tmp"

    for i, index_with_date in enumerate(indexes_for_merge):
        task_id = merge_single_index(index_with_date, index_temp_for_merge)
        await_task(task_id)

    log.debug(f"merged indexes count={len(indexes_for_merge)} to tmp index={index_temp_for_merge}")
    delete_indexes(indexes_for_merge)

    rename_tmp_index_to_final_name(index_temp_for_merge, index_with_date_end)
    delete_index(index_temp_for_merge)


def run():
    old_tmp_indexes = get_tmp_indexes()

    if len(old_tmp_indexes) >= 1:
        log.info(f"found previous not finished merged indexes={old_tmp_indexes}. delete it")
        delete_indexes(old_tmp_indexes)

    oldest_date = get_oldest_date_in_indexes()
    log.info(f"staring from oldest_date={str(oldest_date)}")

    today = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = (today - timedelta(days=1)).replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)

    current_date = oldest_date

    while current_date <= yesterday:
        indexes_by_date = get_indexes_by_date(current_date)

        for index in indexes_by_date:
            index_with_date = index["index"]
            log.debug(f"start working with index={index_with_date}, all indexes.count={len(indexes_by_date)} " +
                      f"by date={index_with_date[-10:]}")
            indexes_by_current_index_name = get_indexes_by_name(index_with_date, current_date, yesterday)

            if len(indexes_by_current_index_name) <= 1:
                log.debug(f"not found enough indexes for merge. "
                          f"indexes_by_current_index_name=size={len(indexes_by_current_index_name)}")
                continue

            indexes_for_merge = get_indexes_for_merge(indexes_by_current_index_name)

            if not indexes_for_merge[3]:
                log.debug(f"not reached limits. skipping indexes. "
                          f"size={indexes_for_merge[1]}gb, count={indexes_for_merge[2]}")
                continue

            if len(indexes_for_merge[0]) < 1:
                log.debug(f"not found indexes for merge. skipping index={index_with_date}. "
                          f"size={indexes_for_merge[1]}gb, count={indexes_for_merge[2]}")
                continue

            log.info(
                f"preparing indexes for merge. size={indexes_for_merge[1]}gb, count={indexes_for_merge[2]}. indexes={indexes_for_merge[0]}")
            merge_indexes(indexes_for_merge[0])
            log.info(
                f"merged indexes. size={indexes_for_merge[1]}gb, count={indexes_for_merge[2]}, indexes={indexes_for_merge[0]}")

        current_date = current_date + timedelta(1)
        log.debug(f"increment current_date={current_date}")


if __name__ == '__main__':
    run()
