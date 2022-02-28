import logging

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
