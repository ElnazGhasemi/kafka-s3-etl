import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def get_daily_logger():
    log_filename = os.path.join('logs', datetime.now().strftime('log_%Y_%m_%d.log'))
    handler = TimedRotatingFileHandler(
        log_filename, when='midnight', interval=1, backupCount=7, encoding="utf-8"
    )
    handler.suffix = "%Y_%m_%d"
    # Include filename, function name, and line number in log format
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(funcName)s:%(lineno)d]: %(message)s'))

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()  # Avoid duplicate logs
    logger.addHandler(handler)
    return logger

# Example of how to use this logger
if __name__ == "__main__":
    logger = get_daily_logger()
    logger.info("This is my log message!")
    logger.error("This is an error!xx")
