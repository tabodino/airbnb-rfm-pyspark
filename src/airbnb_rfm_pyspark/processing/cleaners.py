from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config.settings import Settings
from src.airbnb_rfm_pyspark.core.logger import logger


def clean_price_column(df: DataFrame, price_col: str = "price") -> DataFrame:
    """
    Cleans a price column by removing currency symbols and converting to float.

    Steps:
    1. Remove currency symbols from the price column.
    2. Cast the cleaned price column to float.
    3. Filter outliers.
    """
    config = Settings().rfm

    logger.info(
        f"Schema before clean_price_column with price_col='{price_col}': {df.columns}"
    )

    df_cleaned = df.withColumn(
        "price_numeric_str",
        F.regexp_replace(F.col(price_col), r"[^0-9.]", ""),
    )

    df_cleaned = df_cleaned.filter(F.col("price_numeric_str") != "")

    df_cleaned = df_cleaned.withColumn(
        "price_cleaned",
        F.expr("try_cast(price_numeric_str as float)"),
    ).filter(
        F.col("price_cleaned").isNotNull()
        & (F.col("price_cleaned") >= config.min_price_filter)
        & (F.col("price_cleaned") <= config.max_price_filter)
    )

    initial_count = df.count()
    cleaned_count = df_cleaned.count()
    removed = initial_count - cleaned_count

    logger.info(
        f"Price cleaning: {initial_count:,} -> {cleaned_count:,} rows "
        f"(removed {removed:,} invalid prices)"
    )

    return df_cleaned


def normalize_date_column(
    df: DataFrame, date_col: str = "date", date_format: str = "yyyy-MM-dd"
) -> DataFrame:
    """
    Normalizes a date column to a standard format.
    """
    df_normalized = df.withColumn(date_col, F.try_to_date(F.col(date_col), date_format))

    null_dates_count = df_normalized.filter(F.col(date_col).isNull()).count()
    if null_dates_count > 0:
        logger.warning(f"Found {null_dates_count:,} null dates, filtering them out")
        df_normalized = df_normalized.filter(F.col(date_col).isNotNull())

    return df_normalized


def filter_valid_neighbourhoods(
    df: DataFrame, col: str = "neighbourhood_cleansed"
) -> DataFrame:
    """
    Filters out rows with invalid or missing neighbourhoods.
    """
    initial_count = df.count()
    df_filtered = df.filter(
        (F.col(col).isNotNull()) & (F.col(col) != "") & (F.col(col) != "Unknown")
    )

    filtered_count = df_filtered.count()
    removed = initial_count - filtered_count

    logger.info(
        f"Neighbourhood filtering: {initial_count:,} -> {filtered_count:,} rows "
        f"(removed {removed:,} invalid neighbourhoods)"
    )

    return df_filtered


def deduplicate_reviews(df: DataFrame) -> DataFrame:
    """
    Deduplicates reviews based on 'listing_id' and 'date', keeping the latest review.
    """
    initial_count = df.count()

    df_deduplicated = df.dropDuplicates(["listing_id", "reviewer_id", "date"])

    deduplicated_count = df_deduplicated.count()
    removed = initial_count - deduplicated_count

    if removed > 0:
        logger.warning(
            f"Found {removed:,} duplicate reviews, removed them "
            f"(kept {deduplicated_count:,} unique rows)"
        )

    return df_deduplicated


def optimize_dataframe_partitions(
    df: DataFrame, target_rows_per_partition: int = 100000
) -> DataFrame:
    """
    Optimizes the number of partitions of a DataFrame.

    If the current number of partitions is greater than the target, it will
    reduce the number of partitions using coalesce. If it's less, it will
    increase the number of partitions using repartition.
    """
    current_partitions = df.rdd.getNumPartitions()
    total_rows = df.count()
    optimal_partitions = max(1, total_rows // target_rows_per_partition)

    if current_partitions > optimal_partitions:
        logger.info(
            f"Coalescing from {current_partitions} to {optimal_partitions} "
            f"partitions for {total_rows:,} rows"
        )
        return df.coalesce(optimal_partitions)

    if current_partitions < optimal_partitions:
        logger.info(
            f"Repartitioning from {current_partitions} to {optimal_partitions} "
            f"partitions for {total_rows:,} rows"
        )
        return df.repartition(optimal_partitions)

    logger.info(
        f"Partition count already optimal: {current_partitions} partitions "
        f"for {total_rows:,} rows"
    )
    return df
