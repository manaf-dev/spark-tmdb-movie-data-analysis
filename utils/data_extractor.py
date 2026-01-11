import time

import requests
from config import TMDB_API_KEY, get_logger

from utils.data_schema import API_MOVIES_SCHEMA

logger = get_logger(__name__)


def fetch_movies_from_api(spark, movie_ids, max_retries=3, backoff_factor=1.5):
    """
    Fetch movies from TMDB API.

    Args:
        spark: SparkSession object
        movie_ids: List of TMDB movie IDs

    Returns:
        Spark DataFrame with raw movie data
    """
    base_url = "https://api.themoviedb.org/3/movie/"
    headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}

    movies = []
    logger.info("Fetching movies from TMDB API")

    for movie_id in movie_ids:
        url = f"{base_url}{movie_id}?append_to_response=credits"
        attempts = 0

        while attempts <= max_retries:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    movie_data = response.json()
                    movies.append(movie_data)
                    break

                if response.status_code in (429, 500, 502, 503, 504):
                    wait_time = backoff_factor**attempts
                    logger.warning(
                        f"Retry {attempts + 1}/{max_retries} for movie with ID {movie_id}. "
                        f"Waiting {wait_time:.1f} seconds..."
                    )
                    time.sleep(wait_time)
                    attempts += 1
                else:
                    logger.error(
                        f"Failed to fetch movie with ID {movie_id}: (Status code {response.status_code})"
                    )
                    break

            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor**attempts
                logger.warning(
                    f"Error fetching movie with ID {movie_id}: {e}. "
                    f"Retrying in {wait_time:.1f}s... (Attempt {attempts + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                attempts += 1
        else:
            logger.error(f"Exceeded max retries for movie with ID {movie_id}")

    logger.info(f"Successfully fetched {len(movies)} movies")

    return spark.createDataFrame(movies, schema=API_MOVIES_SCHEMA)
