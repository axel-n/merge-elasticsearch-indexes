from datetime import datetime, timedelta
from typing import List, Any

import config
from elasticsearch_client import get_oldest_date_in_indexes, get_indexes_by_date, get_indexes_by_name, \
    merge_single_index, await_task, delete_indexes, delete_index
from log_config import get_logger

log = get_logger()

ONE_GIGABYTE_IN_BYTES = 1024 * 1024 * 1024
MAX_INDEX_SIZE_IN_BYTES = ONE_GIGABYTE_IN_BYTES * config.elasticsearch["MAX_INDEX_SIZE_IN_GIGABYTES"]


def get_indexes_for_merge(indexes_by_current_index_name: List) -> tuple[list[Any], float, int]:
    indexes_for_merge = list()

    current_size_in_bytes_indexes_for_merge = 0
    current_count_days_for_merge = 0

    for index in indexes_by_current_index_name:
        index_size = int(index["pri.store.size"])
        index_name = index["index"]

        if current_count_days_for_merge <= config.elasticsearch["MAX_INDEX_DAY_MERGE"]:
            if current_size_in_bytes_indexes_for_merge + index_size <= MAX_INDEX_SIZE_IN_BYTES:

                log.debug(f"index={index_name} preparing for merging")
                indexes_for_merge.append(index_name)
                current_size_in_bytes_indexes_for_merge += index_size
                current_count_days_for_merge += 1
            else:
                log.debug(f"skipping index={index_name} because size to large")
        else:
            log.debug(f"skipping index={index_name} because days to large")

    size_in_gigabytes_indexes_for_merge = \
        float("{:.4f}".format(current_size_in_bytes_indexes_for_merge / ONE_GIGABYTE_IN_BYTES))

    return indexes_for_merge, size_in_gigabytes_indexes_for_merge, current_count_days_for_merge


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
    oldest_date = get_oldest_date_in_indexes()
    log.info(f"staring from oldest_date={str(oldest_date)[0:10]}")

    yesterday = datetime.now() - timedelta(1)

    current_date = oldest_date

    while current_date <= yesterday:
        indexes_by_date = get_indexes_by_date(current_date)

        for index in indexes_by_date:
            index_with_date = index["index"]
            log.info(f"start working with index={index_with_date}, all indexes.count={len(indexes_by_date)} " +
                     f"by date={index_with_date[-10:]}")
            indexes_by_current_index_name = get_indexes_by_name(index_with_date, current_date)

            indexes_for_merge = get_indexes_for_merge(indexes_by_current_index_name)

            if indexes_for_merge[2] > 1:
                log.info(f"preparing indexes size={indexes_for_merge[1]}gb, count={indexes_for_merge[2]} for merge")
                merge_indexes(indexes_for_merge[0])
                log.info(f"merged indexes with size={indexes_for_merge[1]}gb, count={indexes_for_merge[2]}")

        current_date = current_date + timedelta(1)
        log.debug(f"increment current_date={current_date}")


if __name__ == '__main__':
    run()
