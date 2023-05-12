import configparser
import os
import pathlib


def reject_if(test: bool, error_message: str = ""):
    if test:
        raise RuntimeError(error_message)


def init_config():
    config: configparser = configparser.RawConfigParser()
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))
    return config


current_dir = os.path.join(pathlib.Path(__file__).parent.parent.resolve())
config: configparser = init_config()
