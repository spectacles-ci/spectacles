from pathlib import Path
import logging
import os

LOG_DIRECTORY = Path("logs")
LOG_FILEPATH = Path(LOG_DIRECTORY / "logs.txt")

LOG_DIRECTORY.mkdir(exist_ok=True)

logger = logging.getLogger("Fonz")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(LOG_FILEPATH)
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

GLOBAL_LOGGER = logger
