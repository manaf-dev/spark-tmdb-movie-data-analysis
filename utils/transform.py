from functools import reduce
from operator import add

from config import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, expr, lit, size, to_date, when
from pyspark.sql.types import DoubleType

logger = get_logger(__name__)


def clean_movie_data(df: DataFrame) -> DataFrame:
    logger.info("Starting data cleaning with PySpark...")
    movies_df = df

    # Drop irrelevant columns
    columns_to_drop = [
        "adult",
        "imdb_id",
        "original_title",
        "video",
        "homepage",
        "backdrop_path",
    ]
    movies_df = movies_df.drop(*[c for c in columns_to_drop if c in movies_df.columns])

    # Extract nested data

    # belongs_to_collection.name
    if "belongs_to_collection" in movies_df.columns:
        movies_df = movies_df.withColumn(
            "belongs_to_collection", col("belongs_to_collection.name")
        )

    # genres
    if "genres" in movies_df.columns:
        movies_df = movies_df.withColumn("genres", concat_ws("|", col("genres.name")))

    # spoken_languages
    if "spoken_languages" in movies_df.columns:
        movies_df = movies_df.withColumn(
            "spoken_languages", concat_ws("|", col("spoken_languages.english_name"))
        )

    # production_countries
    if "production_countries" in movies_df.columns:
        movies_df = movies_df.withColumn(
            "production_countries", concat_ws("|", col("production_countries.name"))
        )

    # production_companies
    if "production_companies" in movies_df.columns:
        movies_df = movies_df.withColumn(
            "production_companies", concat_ws("|", col("production_companies.name"))
        )

    # Credits extraction
    if "credits" in movies_df.columns:
        # Cast names
        movies_df = movies_df.withColumn(
            "cast", concat_ws("|", col("credits.cast.name"))
        )

        # Cast size
        movies_df = movies_df.withColumn("cast_size", size(col("credits.cast")))

        # Directors only
        movies_df = movies_df.withColumn(
            "directors",
            expr("concat_ws('|', filter(credits.crew, x -> x.job = 'Director').name)"),
        )

        # Crew size
        movies_df = movies_df.withColumn("crew_size", size(col("credits.crew")))

        # Drop credits
        movies_df = movies_df.drop("credits")

    # Type conversions
    for c in ["budget", "revenue", "runtime"]:
        if c in movies_df.columns:
            movies_df = movies_df.withColumn(c, col(c).cast(DoubleType()))

    if "release_date" in movies_df.columns:
        movies_df = movies_df.withColumn("release_date", to_date(col("release_date")))

    # Replace unrealistic values
    for c in ["budget", "revenue", "runtime"]:
        if c in movies_df.columns:
            movies_df = movies_df.withColumn(
                c, when(col(c) <= 0, None).otherwise(col(c))
            )

    # Convert to millions
    movies_df = movies_df.withColumn("budget_musd", col("budget") / 1_000_000)
    movies_df = movies_df.withColumn("revenue_musd", col("revenue") / 1_000_000)
    movies_df = movies_df.drop("budget", "revenue")

    # Vote average cleanup
    movies_df = movies_df.withColumn(
        "vote_average",
        when(col("vote_count") == 0, None).otherwise(col("vote_average")),
    )

    # Deduplication & validity
    movies_df = movies_df.dropDuplicates(["id"]).dropna(subset=["id", "title"])

    # Keep rows with ≥ 10 non-null values
    non_na_count = reduce(
        add,
        [when(col(c).isNotNull(), lit(1)).otherwise(lit(0)) for c in movies_df.columns],
    )
    movies_df = movies_df.withColumn("non_na_count", non_na_count)
    movies_df = movies_df.filter(col("non_na_count") >= 10).drop("non_na_count")

    # Released movies only
    if "status" in movies_df.columns:
        movies_df = movies_df.filter(col("status") == "Released").drop("status")

    # Profit & ROI
    movies_df = movies_df.withColumn("profit", col("revenue_musd") - col("budget_musd"))
    movies_df = movies_df.withColumn("roi", col("revenue_musd") / col("budget_musd"))

    column_order = [
        "id",
        "title",
        "tagline",
        "release_date",
        "genres",
        "belongs_to_collection",
        "original_language",
        "budget_musd",
        "revenue_musd",
        "profit",
        "roi",
        "production_companies",
        "production_countries",
        "vote_count",
        "vote_average",
        "popularity",
        "runtime",
        "overview",
        "spoken_languages",
        "poster_path",
        "cast",
        "cast_size",
        "directors",
        "crew_size",
    ]
    # Select only the columns that actually exist in the DataFrame
    final_columns = [c for c in column_order if c in movies_df.columns]
    movies_df = movies_df.select(final_columns)

    logger.info(
        f"Cleaned data: {movies_df.count()} movies, {len(movies_df.columns)} columns"
    )

    return movies_df
