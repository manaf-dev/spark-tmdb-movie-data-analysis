"""
Visualization utilities for movie data analysis.
"""

import matplotlib.pyplot as plt
from pyspark.sql.functions import avg, col, count, explode, split, when, year


def plot_revenue_vs_budget(df):
    """
    Create scatter plot of revenue vs budget with trend line.

    Args:
        df: Movie PySpark DataFrame with 'budget_musd' and 'revenue_musd' columns
    """
    plt.figure(figsize=(10, 6))

    # Get data without missing values and convert to Pandas
    plot_data = df.select("budget_musd", "revenue_musd").dropna().toPandas()

    # Scatter plot
    plt.scatter(
        plot_data["budget_musd"],
        plot_data["revenue_musd"],
        alpha=0.6,
        s=100,
        edgecolors="black",
        linewidth=0.5,
    )

    plt.xlabel("Budget (Million USD)", fontsize=12)
    plt.ylabel("Revenue (Million USD)", fontsize=12)
    plt.title("Revenue vs Budget Trends", fontsize=14, fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def plot_roi_by_genre(df):
    """
    Create bar chart of average ROI by genre.

    Args:
        df: Movie PySpark DataFrame with 'genres' and 'roi' columns
    """
    plt.figure(figsize=(10, 6))

    # Explode genres and calculate mean roi
    genre_df = df.withColumn("Genre", explode(split(col("genres"), "\|")))
    genre_roi = (
        genre_df.groupBy("Genre")
        .agg(avg("roi").alias("roi"))
        .sort("roi", ascending=False)
        .toPandas()
    )

    # Create bar chart
    plt.bar(genre_roi["Genre"], genre_roi["roi"], edgecolor="black")

    plt.xlabel("Genre", fontsize=12)
    plt.ylabel("Average ROI", fontsize=12)
    plt.title("ROI Distribution by Genre", fontsize=14, fontweight="bold")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()


def plot_franchise_comparison(df):
    """
    Create multi-panel comparison of franchise vs standalone performance.

    Args:
        df: Movie PySpark DataFrame with franchise indicators
    """
    # Group by franchise vs standalone and calculate metrics
    stats_df = (
        df.withColumn(
            "type",
            when(col("belongs_to_collection").isNotNull(), "Franchise").otherwise(
                "Standalone"
            ),
        )
        .groupBy("type")
        .agg(
            avg("revenue_musd").alias("Revenue"),
            avg("roi").alias("ROI"),
            avg("budget_musd").alias("Budget"),
            avg("vote_average").alias("Rating"),
        )
        .toPandas()
        .set_index("type")
    )

    # Prepare metrics, handling cases where one type might be missing
    metrics = {
        "Revenue": [
            stats_df.loc["Franchise"]["Revenue"] if "Franchise" in stats_df.index else 0,
            stats_df.loc["Standalone"]["Revenue"]
            if "Standalone" in stats_df.index
            else 0,
        ],
        "ROI": [
            stats_df.loc["Franchise"]["ROI"] if "Franchise" in stats_df.index else 0,
            stats_df.loc["Standalone"]["ROI"] if "Standalone" in stats_df.index else 0,
        ],
        "Budget": [
            stats_df.loc["Franchise"]["Budget"] if "Franchise" in stats_df.index else 0,
            stats_df.loc["Standalone"]["Budget"]
            if "Standalone" in stats_df.index
            else 0,
        ],
        "Rating": [
            stats_df.loc["Franchise"]["Rating"] if "Franchise" in stats_df.index else 0,
            stats_df.loc["Standalone"]["Rating"]
            if "Standalone" in stats_df.index
            else 0,
        ],
    }

    # Create subplots
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle(
        "Franchise vs Standalone Movie Performance", fontsize=14, fontweight="bold"
    )

    categories = ["Franchise", "Standalone"]
    colors = ["#2E86AB", "#A23B72"]

    # Revenue comparison
    axs[0, 0].bar(categories, metrics["Revenue"], color=colors, edgecolor="black")
    axs[0, 0].set_title("Average Revenue (M USD)")
    axs[0, 0].set_ylabel("Million USD")
    axs[0, 0].grid(True, axis="y", alpha=0.3)

    # ROI comparison
    axs[0, 1].bar(categories, metrics["ROI"], color=colors, edgecolor="black")
    axs[0, 1].set_title("Average ROI")
    axs[0, 1].set_ylabel("ROI Multiplier")
    axs[0, 1].grid(True, axis="y", alpha=0.3)

    # Budget comparison
    axs[1, 0].bar(categories, metrics["Budget"], color=colors, edgecolor="black")
    axs[1, 0].set_title("Average Budget (M USD)")
    axs[1, 0].set_ylabel("Million USD")
    axs[1, 0].grid(True, axis="y", alpha=0.3)

    # Rating comparison
    axs[1, 1].bar(categories, metrics["Rating"], color=colors, edgecolor="black")
    axs[1, 1].set_title("Average Rating")
    axs[1, 1].set_ylabel("Rating (out of 10)")
    axs[1, 1].set_ylim(0, 10)
    axs[1, 1].grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    plt.show()


def plot_popularity_vs_rating(df):
    """
    Create scatter plot of popularity vs rating.

    Args:
        df: Movie PySpark DataFrame
    """
    plt.figure(figsize=(10, 6))

    # Select data and convert to Pandas
    plot_data = df.select("vote_average", "popularity").toPandas()

    plt.scatter(
        plot_data["vote_average"],
        plot_data["popularity"],
        alpha=0.6,
        s=100,
        edgecolors="black",
        linewidth=0.5,
    )

    plt.xlabel("Rating", fontsize=12)
    plt.ylabel("Popularity", fontsize=12)
    plt.title("Popularity vs. Rating", fontsize=14, fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def plot_yearly_trends(df):
    """
    Plot yearly trends in box office performance.

    Args:
        df: Movie PySpark DataFrame
    """
    # Extract year from release date, group by year, and calculate metrics
    yearly_stats = (
        df.withColumn("release_year", year(col("release_date")))
        .groupBy("release_year")
        .agg(
            count("*").alias("Movie Count"),
            avg("revenue_musd").alias("Mean Revenue"),
            avg("budget_musd").alias("Mean Budget"),
            avg("roi").alias("Mean ROI"),
        )
        .sort("release_year")
        .toPandas()
        .set_index("release_year")
    )

    # Create subplots
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle("Yearly Box Office Performance Trends", fontsize=16, fontweight="bold")

    # Movie Count per Year
    axs[0, 0].plot(yearly_stats.index, yearly_stats["Movie Count"], marker="o")
    axs[0, 0].set_title("Number of Movies Released per Year")
    axs[0, 0].set_ylabel("Count")

    # Average Revenue per Year
    axs[0, 1].plot(
        yearly_stats.index, yearly_stats["Mean Revenue"], marker="o", color="green"
    )
    axs[0, 1].set_title("Average Revenue per Year (M USD)")
    axs[0, 1].set_ylabel("Revenue (M USD)")

    # Average Budget per Year
    axs[1, 0].plot(
        yearly_stats.index, yearly_stats["Mean Budget"], marker="o", color="orange"
    )
    axs[1, 0].set_title("Average Budget per Year (M USD)")
    axs[1, 0].set_ylabel("Budget (M USD)")

    # Average ROI per Year
    axs[1, 1].plot(yearly_stats.index, yearly_stats["Mean ROI"], marker="o", color="purple")
    axs[1, 1].set_title("Average ROI per Year")
    axs[1, 1].set_ylabel("ROI")

    for ax in axs.flat:
        ax.set_xlabel("Year")
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()