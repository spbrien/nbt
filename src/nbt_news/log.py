import sys
import logging

# Logging
# -----------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# -----------------------------------------------------