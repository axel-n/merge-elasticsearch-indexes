import logging
from datetime import datetime, timedelta

from elasticsearch_client import get_oldest_date_in_indexes, get_indexes_by_date, get_indexes_by_name
from log_config import get_logger

logger = get_logger()




def run():
    oldest_date = get_oldest_date_in_indexes()
    logger.info(f"staring from oldest_date={oldest_date}")

    yesterday = datetime.now() - timedelta(1)

    current_date = oldest_date

    while current_date <= yesterday:
        indexes_by_date = get_indexes_by_date(current_date)

        for index_with_date in indexes_by_date:
            indexes_by_current_index_name = get_indexes_by_name(index_with_date)




if __name__ == '__main__':
    run()
