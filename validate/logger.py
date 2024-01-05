import logging
import logging.config
from logging.handlers import RotatingFileHandler

import yaml

with open("config_log.yaml", "r") as f:
  config = yaml.safe_load(f.read())
  logging.config.dictConfig(config)

logger = logging.getLogger("mainLogger")
handler: RotatingFileHandler = logging.getHandlerByName("file")

try:
  handler.doRollover()
except PermissionError:
  # log file can still be in use due to notebook
  logger.warning("doRollover failed - Log file still in use, probably by an active notebook.")
