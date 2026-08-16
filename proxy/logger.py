import logging
import sys

from config import settings

logger = logging.getLogger(__name__)
logger_level = getattr(logging, settings.logging.level.upper(), None)
logger.setLevel(logger_level)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
