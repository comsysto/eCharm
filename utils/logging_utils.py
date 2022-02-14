import logging
from logging.handlers import RotatingFileHandler


def setup_logger(name, info_only=True):
    if info_only:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

        loghdl = RotatingFileHandler(name + '.log')
        loghdl.setLevel(logging.DEBUG)
        loghdl.setFormatter(formatter)
        logger.addHandler(loghdl)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


log = setup_logger(name='appLog')
