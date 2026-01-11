import logging
import os
from decouple import config


TMDB_API_KEY = config("TMDB_API_KEY")


def get_logger(name: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("tmdb_analysis.log"), logging.StreamHandler()],
    )
    return logging.getLogger(name)
