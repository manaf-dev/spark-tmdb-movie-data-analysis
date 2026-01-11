from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, explode, lit, mean, split
from pyspark.sql.functions import sum as _sum


def rank_movies(
    df: DataFrame,
    metric: str,
    top_n: int = 5,
    ascending: bool = False,
    min_budget: float | None = None,
    min_votes: int | None = None,
) -> DataFrame:
    data = df

    if min_budget is not None:
        data = data.filter(col("budget_musd") >= min_budget)

    if min_votes is not None:
        data = data.filter(col("vote_count") >= min_votes)

    order_col = col(metric).asc() if ascending else col(metric).desc()

    return data.orderBy(order_col).limit(top_n)


def analyze_franchise_vs_standalone(df: DataFrame) -> DataFrame:
    franchise = df.filter(col("belongs_to_collection").isNotNull())
    standalone = df.filter(col("belongs_to_collection").isNull())

    franchise_metrics = franchise.select(
        mean("revenue_musd").alias("Mean Revenue (M USD)"),
        mean("roi").alias("Mean ROI"),
        mean("budget_musd").alias("Mean Budget (M USD)"),
        mean("popularity").alias("Mean Popularity"),
        mean("vote_average").alias("Mean Rating"),
        count("*").alias("Movie Count"),
    )

    standalone_metrics = standalone.select(
        mean("revenue_musd").alias("Mean Revenue (M USD)"),
        mean("roi").alias("Mean ROI"),
        mean("budget_musd").alias("Mean Budget (M USD)"),
        mean("popularity").alias("Mean Popularity"),
        mean("vote_average").alias("Mean Rating"),
        count("*").alias("Movie Count"),
    )

    # Convert wide → long comparison table
    comparison = franchise_metrics.withColumn("Type", lit("Franchise")).unionByName(
        standalone_metrics.withColumn("Type", lit("Standalone"))
    )

    return comparison


def get_successful_franchises(df: DataFrame) -> DataFrame:
    franchise_movies = df.filter(col("belongs_to_collection").isNotNull())

    performance = (
        franchise_movies.groupBy("belongs_to_collection")
        .agg(
            count("id").alias("Total Movies"),
            _sum("budget_musd").alias("Total Budget"),
            mean("budget_musd").alias("Mean Budget"),
            _sum("revenue_musd").alias("Total Revenue"),
            mean("revenue_musd").alias("Mean Revenue"),
            mean("vote_average").alias("Mean Rating"),
        )
        .orderBy(col("Total Movies").desc(), col("Total Revenue").desc())
    )

    return performance


def get_successful_directors(df: DataFrame) -> DataFrame:
    directors_df = df.select(
        "id",
        "revenue_musd",
        "vote_average",
        explode(split(col("directors"), "\\|")).alias("director"),
    ).filter(col("director").isNotNull() & (col("director") != ""))

    performance = (
        directors_df.groupBy("director")
        .agg(
            count("id").alias("Total Movies"),
            _sum("revenue_musd").alias("Total Revenue"),
            mean("vote_average").alias("Mean Rating"),
        )
        .orderBy(col("Total Movies").desc(), col("Total Revenue").desc())
    )

    return performance


def search_movies(
    df: DataFrame,
    cast_member: str | None = None,
    director: str | None = None,
    genres: list[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = False,
) -> DataFrame:
    data = df

    if cast_member:
        data = data.filter(col("cast").rlike(f"(?i){cast_member}"))

    if director:
        data = data.filter(col("directors").rlike(f"(?i){director}"))

    if genres:
        for genre in genres:
            data = data.filter(col("genres").rlike(f"(?i){genre}"))

    if sort_by:
        order_col = col(sort_by).asc() if ascending else col(sort_by).desc()
        data = data.orderBy(order_col)

    return data
