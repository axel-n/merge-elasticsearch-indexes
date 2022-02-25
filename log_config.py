import logging

import config

console_format = "%(asctime)s %(levelname)s %(filename)s.%(funcName)s:%(lineno)s - %(message)s"

console_formatter = logging.Formatter(
    fmt=console_format,
    datefmt="%Y-%m-%d %H:%M:%S"
)

root_logger = logging.getLogger()
root_logger.setLevel(config.app["LOG_LEVEL"])

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)


def get_logger():
    return root_logger
