from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, concat_ws, expr, size, to_date, when
from pyspark.sql.functions import filter as spark_filter
from pyspark.sql.types import DoubleType


def clean_movie_data(df: DataFrame) -> DataFrame:
    columns = set(df.columns)

    # Drop irrelevant columns
    drop_cols = {
        "adult",
        "imdb_id",
        "original_title",
        "video",
        "homepage",
        "backdrop_path",
    }
    movies_df = df.drop(*drop_cols.intersection(columns))

    # Flatten nested fields
    select_exprs = ["*"]

    if "belongs_to_collection" in columns:
        select_exprs.append(
            col("belongs_to_collection.name").alias("belongs_to_collection")
        )

    if "genres" in columns:
        select_exprs.append(concat_ws("|", col("genres.name")).alias("genres"))

    if "spoken_languages" in columns:
        select_exprs.append(
            concat_ws("|", col("spoken_languages.english_name")).alias(
                "spoken_languages"
            )
        )

    if "production_countries" in columns:
        select_exprs.append(
            concat_ws("|", col("production_countries.name")).alias(
                "production_countries"
            )
        )

    if "production_companies" in columns:
        select_exprs.append(
            concat_ws("|", col("production_companies.name")).alias(
                "production_companies"
            )
        )

    if "credits" in columns:
        select_exprs.extend(
            [
                concat_ws("|", col("credits.cast.name")).alias("cast"),
                size(col("credits.cast")).alias("cast_size"),
                expr(
                    "concat_ws('|', filter(credits.crew, x -> x.job = 'Director').name)"
                ).alias("directors"),
                size(col("credits.crew")).alias("crew_size"),
            ]
        )

    movies_df = movies_df.select(*select_exprs)

    if "credits" in columns:
        movies_df = movies_df.drop("credits")

    # Type conversions
    for c in {"budget", "revenue", "runtime"} & columns:
        movies_df = movies_df.withColumn(c, when(col(c) > 0, col(c).cast(DoubleType())))

    if "release_date" in columns:
        movies_df = movies_df.withColumn("release_date", to_date(col("release_date")))

    # Monetary normalization
    movies_df = (
        movies_df.withColumn("budget_musd", col("budget") / 1_000_000)
        .withColumn("revenue_musd", col("revenue") / 1_000_000)
        .drop("budget", "revenue")
    )

    # Vote cleanup
    if {"vote_average", "vote_count"} <= columns:
        movies_df = movies_df.withColumn(
            "vote_average", when(col("vote_count") > 0, col("vote_average"))
        )

    # Deduplication
    movies_df = movies_df.dropDuplicates(["id"]).dropna(subset=["id", "title"])

    # Non-null threshold
    non_null_cols = [col(c) for c in movies_df.columns]

    movies_df = (
        movies_df.withColumn(
            "non_na_count",
            size(spark_filter(array(*non_null_cols), lambda x: x.isNotNull())),
        )
        .filter(col("non_na_count") >= 10)
        .drop("non_na_count")
    )

    # Released only
    if "status" in columns:
        movies_df = movies_df.filter(col("status") == "Released").drop("status")

    # Profit & ROI
    movies_df = movies_df.withColumn(
        "profit", col("revenue_musd") - col("budget_musd")
    ).withColumn(
        "roi",
        when(
            col("budget_musd").isNotNull() & (col("budget_musd") > 0),
            col("revenue_musd") / col("budget_musd"),
        ),
    )

    # Column ordering
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

    final_columns = [c for c in column_order if c in movies_df.columns]
    return movies_df.select(final_columns)
