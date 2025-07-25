import logging
import os

logger = logging.getLogger(__name__)


def get_log_level() -> int:
    """
    Gets the LOG_LEVEL from the environment.

    If it could not find one, set it to None.

    If one was found, but not expected, set it to DEBUG
    """
    log_level = os.getenv("LOG_LEVEL")

    if log_level is None:
        return logging.CRITICAL + 1

    match log_level.lower():
        case "none":
            return logging.CRITICAL + 1
        case "debug":
            logger.info("Log level set to DEBUG")
            return logging.DEBUG
        case "info":
            logger.info("Log level set to INFO")
            return logging.INFO
        case "warning" | "warn":
            logger.info("Log level set to WARNING")
            return logging.WARNING
        case "error":
            logger.info("Log level set to ERROR")
            return logging.ERROR
        case "critical":
            logger.info("Log level set to CRITICAL")
            return logging.CRITICAL
        case ll:
            logger.warning(
                f"Did not recognize log level string {ll}. Using DEBUG"
            )
            return logging.DEBUG
