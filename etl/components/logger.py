import os

import logging
from dotenv import load_dotenv

load_dotenv()

def logger():
    logging.basicConfig(
        # filename='logs.log',
        level=os.environ.get("LOG_LEVEL"),
        format = "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(message)s",
        datefmt = "%d-%m-%Y %I:%M:%S"
    )
    logger = logging.getLogger(__file__)
    return logger
