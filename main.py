import logging

# TODO use filename from config
# filename='merge_elasticsearch_indexes.log', filemode='w',
from elasticsearch_client import get_oldest_date_in_indexes
from log_config import get_logger

logger = get_logger()


def run():
    oldest_date = get_oldest_date_in_indexes()
    logger.info(f"oldest_date={oldest_date}")


if __name__ == '__main__':
    run()
