from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pandas as pd
from config.settings import Settings
from src.airbnb_rfm_pyspark.core.logger import logger, log_execution_time
from src.airbnb_rfm_pyspark.processing.cleaners import (
    clean_price_column,
    normalize_date_column,
    filter_valid_neighbourhoods,
    deduplicate_reviews,
    optimize_dataframe_partitions,
)


class RFMEngine:
    """RFM process Pipeline."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = Settings().rfm

    @log_execution_time
    def load_data(self) -> Tuple[DataFrame, DataFrame]:
        """Load listings and reviews data into Spark DataFrames."""
        logger.info("Loading raw datasets...")

        df_reviews = (
            self.spark.read.csv(
                str(Settings().data.reviews_path.resolve()),
                header=True,
                inferSchema=True,
                sep=",",
                quote='"',
                escape="\\",
                multiLine=True,
            )
            .select("listing_id", "reviewer_id", "date")
            .withColumn("listing_id", F.col("listing_id").cast("string"))
        )

        df_listings = (
            self.spark.read.csv(
                str(Settings().data.listings_path.resolve()),
                header=True,
                inferSchema=True,
                sep=",",
                quote='"',
                escape="\\",
                multiLine=True,
            )
            .select(
                "id",
                "price",
                "neighbourhood_cleansed",
            )
            .withColumn("id", F.col("id").cast("string"))
        )

        logger.info(
            f"Loaded {df_reviews.count():,} reviews "
            f"and {df_listings.count():,} listings"
        )

        # Optimization: Repartition after gz decompression
        df_reviews = optimize_dataframe_partitions(df_reviews)
        df_listings = optimize_dataframe_partitions(df_listings)

        return df_reviews, df_listings

    @log_execution_time
    def clean_data(
        self, df_reviews: DataFrame, df_listings: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """Clean reviews and listings DataFrames."""
        logger.info("Cleaning reviews dataset...")

        df_reviews_cleaned = deduplicate_reviews(df_reviews)

        df_reviews_cleaned = normalize_date_column(df_reviews_cleaned, date_col="date")

        logger.info("Cleaning listings dataset...")
        df_listings_cleaned = clean_price_column(
            filter_valid_neighbourhoods(df_listings), price_col="price"
        )

        df_listings_cleaned = df_listings_cleaned.select(
            F.col("id").alias("listing_id"),
            "price_cleaned",
            F.col("neighbourhood_cleansed").alias("neighbourhood"),
        )

        return df_reviews_cleaned, df_listings_cleaned

    @log_execution_time
    def compute_rfm(self, df_reviews: DataFrame, df_listings: DataFrame) -> DataFrame:
        """
        Compute RFM metrics for each customer.

        Args:
            df_reviews: Cleaned reviews DataFrame.
            df_listings: Cleaned listings DataFrame.
        Returns:
            DataFrame with RFM metrics per customer.
        """
        logger.info("Computing RFM metrics...")

        # Broadcast due to light dataframe size
        df_joined = df_reviews.join(
            F.broadcast(df_listings),  # Broadcast join (avoid shuffle)
            on="listing_id",
            how="inner",
        )

        # Compute RFM metrics per customer
        reference_date = F.lit(self.config.reference_date)

        rfm_agg = df_joined.groupBy("reviewer_id").agg(
            # Recency: days since last review
            F.datediff(reference_date, F.max("date")).alias("recency"),
            # Frequency: total number of reviews
            F.count("*").alias("frequency"),
            # Monetary: sum of prices * avg nights per booking
            F.sum(F.col("price_cleaned") * self.config.avg_nights_per_booking).alias(
                "monetary"
            ),
            # Context: Keep the most popular neighbourhood
            F.first("neighbourhood").alias("favourite_neighbourhood"),
        )

        logger.info(f"Computed RFM for {rfm_agg.count():,} unique reviewers")

        return rfm_agg

    def segment_customers(self, df_rfm: DataFrame) -> DataFrame:
        """
        Apply business segmentation into 6 segments.

        Strategy: Cascade of WHEN/OTHERWISE based on config thresholds.
        Order: From most to least valuable.
        """
        logger.info("Applying customer segmentation...")

        rules = self.config.segment_rules

        # Creation of segment column
        df_segmented = df_rfm.withColumn(
            "segment",
            # Champions: Very recent + Frequent + High LTV
            F.when(
                (F.col("recency") <= rules["Champions"][0])
                & (F.col("frequency") >= rules["Champions"][1])
                & (F.col("monetary") >= rules["Champions"][2]),
                "Champions",
            )
            # Loyal: Regular customers with good lifetime value (LTV)
            .when(
                (F.col("recency") <= rules["Loyal"][0])
                & (F.col("frequency") >= rules["Loyal"][1])
                & (F.col("monetary") >= rules["Loyal"][2]),
                "Loyal",
            )
            # Potential: recent but infrequent customers (to be converted)
            .when(
                (F.col("recency") <= rules["Potential"][0])
                & (F.col("frequency") >= rules["Potential"][1])
                & (F.col("monetary") >= rules["Potential"][2]),
                "Potential",
            )
            # At Risk: Former high‑value customers (win-back)
            .when(
                (F.col("recency") <= rules["At Risk"][0])
                & (F.col("frequency") >= rules["At Risk"][1])
                & (F.col("monetary") >= rules["At Risk"][2]),
                "At Risk",
            )
            # Hibernating: Very old but moderate
            .when(
                (F.col("recency") <= rules["Hibernating"][0])
                & (F.col("frequency") >= rules["Hibernating"][1])
                & (F.col("monetary") >= rules["Hibernating"][2]),
                "Hibernating",
            )
            # Lost: All remaining customers (low value + inactive)
            .otherwise("Lost"),
        )

        # Log distribution of segments
        segment_counts = (
            df_segmented.groupBy("segment")
            .count()
            .orderBy(F.col("count").desc())
            .collect()
        )

        logger.info("Segment distribution:")
        for row in segment_counts:
            logger.info(f"  {row['segment']:<15} {row['count']:>8,} clients")

        return df_segmented

    @log_execution_time
    def calculate_neighbourhood_metrics(self, df_rfm: DataFrame) -> pd.DataFrame:
        """
        Calculates neighbourhood‑level metrics for geographic analysis.

        Returns:
            Pandas DataFrame with: neighbourhood, total_revenue, avg_customer_value, customer_count
        """
        logger.info("Computing neighbourhood metrics...")

        neighbourhood_stats = (
            df_rfm.groupBy("favourite_neighbourhood")
            .agg(
                F.sum("monetary").alias("total_revenue"),
                F.avg("monetary").alias("avg_customer_value"),
                F.count("*").alias("customer_count"),
            )
            .orderBy(F.col("total_revenue").desc())
            .limit(20)  # Top 20 neighbourhoods
        )

        return neighbourhood_stats.toPandas()

    @log_execution_time
    def process(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Full pipeline: Load -> Clean -> RFM -> Segment.

        Returns:
            Tuple[rfm_data, neighbourhood_data] en Pandas pour Streamlit
        """
        # 1. Load data
        df_reviews, df_listings = self.load_data()

        # 2. Cleaning
        df_reviews_clean, df_listings_clean = self.clean_data(df_reviews, df_listings)

        # 3. Compute RFM
        df_rfm = self.compute_rfm(df_reviews_clean, df_listings_clean)

        # 4. Segmentation
        df_segmented = self.segment_customers(df_rfm)

        # 5. Geographic aggregations
        df_neighbourhood = self.calculate_neighbourhood_metrics(df_segmented)

        # 6. Conversion to Pandas (cached for Spark)
        df_segmented.cache()  # Important: prevents recomputation during collect
        df_pandas = df_segmented.toPandas()

        logger.success(
            f"RFM processing complete: {len(df_pandas):,} customers segmented"
        )

        return df_pandas, df_neighbourhood
