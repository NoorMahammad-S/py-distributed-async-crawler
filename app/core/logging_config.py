import logging
import sys


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='{"timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
