import sys
import logging

# Logging
# -----------------------------------------------------
logging.basicConfig(
    level=logging.ERROR,
    # level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M',
    handlers=[logging.StreamHandler(sys.stdout)]
    # filename='./news_analysis.log',
    # filemode='a'
)
# -----------------------------------------------------