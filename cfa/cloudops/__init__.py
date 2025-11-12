import datetime
import logging
import os
import sys

from ._cloudclient import CloudClient
from ._containerappclient import ContainerAppClient
from .automation import run_experiment, run_tasks
from .helpers import get_log_level

__all__ = [CloudClient, ContainerAppClient, run_experiment, run_tasks]

run_time = datetime.datetime.now()
now_string = f"{run_time:%Y-%m-%d_%H:%M:%S%z}"
FORMAT = "[%(levelname)s] %(asctime)s: %(message)s"

log_status = os.getenv("LOG_OUTPUT")
logfile = os.path.join("logs", f"{now_string}.log")


# Efficient handler selection
def get_handlers(log_status, logfile):
    if log_status is None:
        return [logging.StreamHandler(sys.stdout)]
    status = log_status.lower()
    if status.startswith("both"):
        if not os.path.exists("logs"):
            os.mkdir("logs")
        return [logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)]
    if status.startswith("file"):
        if not os.path.exists("logs"):
            os.mkdir("logs")
        return [logging.FileHandler(logfile)]
    if status.startswith("std"):
        return [logging.StreamHandler(sys.stdout)]
    print(f"Did not recognize {log_status}. Setting to stdout.")
    return [logging.StreamHandler(sys.stdout)]


handlers = get_handlers(log_status, logfile)
level = get_log_level()
logging.basicConfig(
    level=level,
    format=FORMAT,
    datefmt="%Y-%m-%d_%H:%M:%S%z",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

if log_status is None:
    logger.info("Logging output set to stdout only.")
elif log_status.lower().startswith("both"):
    logger.info("Logging output set to both stdout and file.")
elif log_status.lower().startswith("file"):
    logger.info("Logging output set to file only.")
elif log_status.lower().startswith("std"):
    logger.info("Logging output set to stdout only.")
else:
    logger.info(f"Did not recognize {log_status}. Setting to stdout.")

logger.info("Logging configuration complete.")
logger.debug(f"logging set to {level} to output: {log_status}.")
