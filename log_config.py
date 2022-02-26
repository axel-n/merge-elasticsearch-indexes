import logging

import config

console_format = "%(asctime)s %(levelname)s %(filename)s.%(funcName)s:%(lineno)s - %(message)s"

console_formatter = logging.Formatter(
    fmt=console_format,
    datefmt="%Y-%m-%d %H:%M:%S"
)

# TODO use filename from config

root_logger = logging.getLogger()
root_logger.setLevel(config.app["LOG_LEVEL"])

console_handler = logging.StreamHandler()
console_handler.setLevel(config.app["LOG_LEVEL"])
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)


def get_logger():
    return root_logger
