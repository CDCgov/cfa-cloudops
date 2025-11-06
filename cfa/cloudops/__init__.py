import datetime
import logging
import os
import sys

from ._cloudclient import CloudClient
from ._containerappclient import ContainerAppClient
from .automation import run_experiment, run_tasks
from .helpers import get_log_level

__all__ = [CloudClient, ContainerAppClient, run_experiment, run_tasks]

logger = logging.getLogger(__name__)
run_time = datetime.datetime.now()
now_string = f"{run_time:%Y-%m-%d_%H:%M:%S%z}"
# Logging
if not os.path.exists("logs"):
    os.mkdir("logs")
logfile = os.path.join("logs", f"{now_string}.log")
FORMAT = "[%(levelname)s] %(asctime)s: %(message)s"

log_status = os.getenv("LOG_OUTPUT")
if log_status is None:
    handler = [logging.StreamHandler(sys.stdout)]
    logger.info("Logging output set to stdout only.")
elif log_status.lower().startswith("both"):
    handler = [logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)]
    logger.info("Logging output set to both stdout and file.")
elif log_status.lower().startswith("file"):
    handler = [logging.FileHandler(logfile)]
    logger.info("Logging output set to file only.")
elif log_status.lower().startswith("std"):
    handler = [logging.StreamHandler(sys.stdout)]
    logger.info("Logging output set to stdout only.")
else:
    print(f"Did not recognize {log_status}. Setting to stdout.")
    handler = [logging.StreamHandler(sys.stdout)]


logging.basicConfig(
    level=get_log_level(),
    format=FORMAT,
    datefmt="%Y-%m-%d_%H:%M:%S%z",
    handlers=handler,
)
logger.info("Logging configuration complete.")
logging.debug(f"logging set to {get_log_level()} to output: {log_status}.")
