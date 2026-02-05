from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pandas as pd
from config.settings import Settings
from src.airbnb_rfm_pyspark.core.logger import logger, log_execution_time
from src.airbnb_rfm_pyspark.core.data_quality import DataQualityChecker
from src.airbnb_rfm_pyspark.core.pipeline_monitor import PipelineMonitor, SimpleAlerter
from src.airbnb_rfm_pyspark.processing.cleaners import (
    clean_price_column,
    normalize_date_column,
    filter_valid_neighbourhoods,
    deduplicate_reviews,
    optimize_dataframe_partitions,
)


class RFMEngine:
    """RFM process Pipeline."""

    def __init__(self, spark: SparkSession, enable_monitoring: bool = True):
        self.spark = spark
        self.config = Settings().rfm

        # Initialize monitor
        self.monitor = PipelineMonitor("airbnb_rfm") if enable_monitoring else None

    @log_execution_time
    def load_data(self) -> Tuple[DataFrame, DataFrame]:
        """Load listings and reviews data with validation."""

        if self.monitor:
            with self.monitor.stage("load_data"):
                return self._load_data_impl()
        else:
            return self._load_data_impl()

    def _load_data_impl(self) -> Tuple[DataFrame, DataFrame]:
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

        # Record input volumes
        reviews_count = df_reviews.count()
        listings_count = df_listings.count()

        logger.info(f"Loaded {reviews_count:,} reviews and {listings_count:,} listings")

        if self.monitor:
            self.monitor.record_input_rows("reviews", reviews_count)
            self.monitor.record_input_rows("listings", listings_count)

        # Optimization: Repartition after gz decompression
        df_reviews = optimize_dataframe_partitions(df_reviews)
        df_listings = optimize_dataframe_partitions(df_listings)

        return df_reviews, df_listings

    @log_execution_time
    def clean_data(
        self, df_reviews: DataFrame, df_listings: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """Clean reviews and listings with quality checks."""

        if self.monitor:
            with self.monitor.stage("clean_data"):
                return self._clean_data_impl(df_reviews, df_listings)
        else:
            return self._clean_data_impl(df_reviews, df_listings)

    def _clean_data_impl(
        self, df_reviews: DataFrame, df_listings: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:

        logger.info("Cleaning reviews dataset...")

        # Track initial counts
        initial_reviews = df_reviews.count()
        initial_listings = df_listings.count()

        # =========================================================================
        # REVIEWS CLEANING + VALIDATION
        # =========================================================================

        # 1. Deduplicate
        df_reviews_cleaned = deduplicate_reviews(df_reviews)

        # 2. Select necessary columns FIRST
        df_reviews_cleaned = df_reviews_cleaned.select(
            "listing_id", "reviewer_id", "date"
        )

        # 3. THEN normalize dates
        df_reviews_cleaned = normalize_date_column(df_reviews_cleaned, date_col="date")

        # Data Quality Checks (WARNING level - log but continue)
        logger.info("🔍 Running data quality checks on reviews...")
        checker_reviews = DataQualityChecker(fail_on_error=False)

        checker_reviews.add_schema_check(["listing_id", "reviewer_id", "date"])
        checker_reviews.add_completeness_check(
            "listing_id", min_completeness=1.0, severity="ERROR"
        )
        checker_reviews.add_completeness_check("date", min_completeness=0.95)

        results_reviews = checker_reviews.run_checks(df_reviews_cleaned)

        # =========================================================================
        # LISTINGS CLEANING + VALIDATION
        # =========================================================================

        logger.info("Cleaning listings dataset...")

        df_listings_cleaned = filter_valid_neighbourhoods(df_listings)
        df_listings_cleaned = clean_price_column(df_listings_cleaned, price_col="price")

        # Select only necessary columns
        df_listings_cleaned = df_listings_cleaned.select(
            F.col("id").alias("listing_id"),
            "price_cleaned",
            F.col("neighbourhood_cleansed").alias("neighbourhood"),
        )

        # Data Quality Checks
        logger.info("🔍 Running data quality checks on listings...")
        checker_listings = DataQualityChecker(fail_on_error=False)

        checker_listings.add_schema_check(
            ["listing_id", "price_cleaned", "neighbourhood"]
        )
        checker_listings.add_completeness_check(
            "listing_id", min_completeness=1.0, severity="ERROR"
        )
        checker_listings.add_completeness_check("price_cleaned", min_completeness=0.8)
        checker_listings.add_range_check(
            "price_cleaned", min_value=10, max_value=10000, max_outliers_pct=0.05
        )

        results_listings = checker_listings.run_checks(df_listings_cleaned)

        # =========================================================================
        # MONITORING
        # =========================================================================

        final_reviews = df_reviews_cleaned.count()
        final_listings = df_listings_cleaned.count()

        if self.monitor:
            filtered_reviews = initial_reviews - final_reviews
            filtered_listings = initial_listings - final_listings

            self.monitor.record_filtered_rows(filtered_reviews + filtered_listings)

            # Aggregate quality results
            total_errors = sum(
                1
                for r in results_reviews + results_listings
                if not r.passed and r.severity == "ERROR"
            )
            total_warnings = sum(
                1
                for r in results_reviews + results_listings
                if not r.passed and r.severity == "WARNING"
            )

            all_passed = total_errors == 0
            self.monitor.record_data_quality(all_passed, total_warnings, total_errors)

        return df_reviews_cleaned, df_listings_cleaned

    @log_execution_time
    def compute_rfm(self, df_reviews: DataFrame, df_listings: DataFrame) -> DataFrame:
        """Compute RFM metrics with monitoring."""

        if self.monitor:
            with self.monitor.stage("compute_rfm"):
                return self._compute_rfm_impl(df_reviews, df_listings)
        else:
            return self._compute_rfm_impl(df_reviews, df_listings)

    def _compute_rfm_impl(
        self, df_reviews: DataFrame, df_listings: DataFrame
    ) -> DataFrame:
        logger.info("Computing RFM metrics...")

        # Optimized Join
        df_joined = df_reviews.join(
            F.broadcast(df_listings),
            on="listing_id",
            how="inner",
        )

        # RFM Compute
        reference_date = F.lit(self.config.reference_date)

        rfm_agg = df_joined.groupBy("reviewer_id").agg(
            F.datediff(reference_date, F.max("date")).alias("recency"),
            F.count("*").alias("frequency"),
            F.sum(F.col("price_cleaned") * self.config.avg_nights_per_booking).alias(
                "monetary"
            ),
            F.first("neighbourhood").alias("favourite_neighbourhood"),
        )

        logger.info(f"Computed RFM for {rfm_agg.count():,} unique reviewers")

        return rfm_agg

    def segment_customers(self, df_rfm: DataFrame) -> DataFrame:
        """Apply customer segmentation with monitoring."""

        if self.monitor:
            with self.monitor.stage("segment_customers"):
                return self._segment_customers_impl(df_rfm)
        else:
            return self._segment_customers_impl(df_rfm)

    def _segment_customers_impl(self, df_rfm: DataFrame) -> DataFrame:
        logger.info("Applying customer segmentation...")

        rules = self.config.segment_rules

        df_segmented = df_rfm.withColumn(
            "segment",
            F.when(
                (F.col("recency") <= rules["Champions"][0])
                & (F.col("frequency") >= rules["Champions"][1])
                & (F.col("monetary") >= rules["Champions"][2]),
                "Champions",
            )
            .when(
                (F.col("recency") <= rules["Loyal"][0])
                & (F.col("frequency") >= rules["Loyal"][1])
                & (F.col("monetary") >= rules["Loyal"][2]),
                "Loyal",
            )
            .when(
                (F.col("recency") <= rules["Potential"][0])
                & (F.col("frequency") >= rules["Potential"][1])
                & (F.col("monetary") >= rules["Potential"][2]),
                "Potential",
            )
            .when(
                (F.col("recency") <= rules["At Risk"][0])
                & (F.col("frequency") >= rules["At Risk"][1])
                & (F.col("monetary") >= rules["At Risk"][2]),
                "At Risk",
            )
            .when(
                (F.col("recency") <= rules["Hibernating"][0])
                & (F.col("frequency") >= rules["Hibernating"][1])
                & (F.col("monetary") >= rules["Hibernating"][2]),
                "Hibernating",
            )
            .otherwise("Lost"),
        )

        # Log & record segment distribution
        segment_counts = (
            df_segmented.groupBy("segment")
            .count()
            .orderBy(F.col("count").desc())
            .collect()
        )

        logger.info("Segment distribution:")
        segments_dict = {}
        for row in segment_counts:
            logger.info(f"  {row['segment']:<15} {row['count']:>8,} clients")
            segments_dict[row["segment"]] = row["count"]

        if self.monitor:
            self.monitor.record_segments(segments_dict)

        return df_segmented

    @log_execution_time
    def calculate_neighbourhood_metrics(self, df_rfm: DataFrame) -> pd.DataFrame:
        """Calculate neighbourhood metrics."""
        logger.info("Computing neighbourhood metrics...")

        neighbourhood_stats = (
            df_rfm.groupBy("favourite_neighbourhood")
            .agg(
                F.sum("monetary").alias("total_revenue"),
                F.avg("monetary").alias("avg_customer_value"),
                F.count("*").alias("customer_count"),
            )
            .orderBy(F.col("total_revenue").desc())
            .limit(20)
        )

        return neighbourhood_stats.toPandas()

    @log_execution_time
    def process(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Pipeline complet avec monitoring intégré.

        Pattern: Observability + Data Quality
        """
        try:
            # 1. Load
            df_reviews, df_listings = self.load_data()

            # 2. Clean + Validate
            df_reviews_clean, df_listings_clean = self.clean_data(
                df_reviews, df_listings
            )

            # 3. Compute RFM
            df_rfm = self.compute_rfm(df_reviews_clean, df_listings_clean)

            # 4. Segment
            df_segmented = self.segment_customers(df_rfm)

            # 5. Neighbourhood metrics
            df_neighbourhood = self.calculate_neighbourhood_metrics(df_segmented)

            # 6. Convert to Pandas
            df_segmented.cache()
            df_pandas = df_segmented.toPandas()

            # Record final output
            if self.monitor:
                self.monitor.record_output_rows(len(df_pandas))
                self.monitor.mark_success()

            logger.success(
                f"RFM processing complete: {len(df_pandas):,} customers segmented"
            )

            return df_pandas, df_neighbourhood

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            if self.monitor:
                self.monitor.mark_failed(str(e))
            raise

        finally:
            # Always save metrics
            if self.monitor:
                self.monitor.save_metrics()

                # Check alerts
                metrics = self.monitor.get_metrics()
                SimpleAlerter.check_alerts(metrics)
