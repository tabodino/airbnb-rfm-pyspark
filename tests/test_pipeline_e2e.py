import pytest
from config.settings import Settings
from src.airbnb_rfm_pyspark.core.spark_session import get_spark, stop_spark
from src.airbnb_rfm_pyspark.processing.rfm_engine import RFMEngine
from src.airbnb_rfm_pyspark.data.ingestion import download_datasets


@pytest.fixture(scope="module")
def spark():
    """Fixture to provide a Spark session for testing."""
    spark_session = get_spark()
    yield spark_session
    stop_spark()


@pytest.fixture(scope="module")
def ensure_data():
    """Fixture to ensure datasets are available for testing."""
    settings = Settings()

    if (
        not settings.data.reviews_path.exists()
        or not settings.data.listings_path.exists()
    ):
        print("Data not found, downloading...")
        download_datasets()

    assert settings.data.reviews_path.exists(), "Reviews data missing"
    assert settings.data.listings_path.exists(), "Listings data missing"


class TestPipelineE2E:
    """End-to-end test for the RFM pipeline using Spark."""

    def test_pipeline_execution(self, spark, ensure_data):
        """Critical Test: Run the entire pipeline and check results."""
        engine = RFMEngine(spark, enable_monitoring=True)

        # Execute the complete pipeline
        df_rfm, df_neighbourhood = engine.process()

        # Basic assertions to verify outputs
        assert df_rfm is not None, "RFM DataFrame should not be None"
        assert len(df_rfm) > 0, "RFM DataFrame should have rows"
        assert (
            df_neighbourhood is not None
        ), "Neighbourhood DataFrame should not be None"

    def test_output_schema(self, spark, ensure_data):
        """Test that the output DataFrames have the expected schema."""
        engine = RFMEngine(spark, enable_monitoring=False)
        df_rfm, _ = engine.process()

        # Check RFM DataFrame schema
        expected_columns = {
            "reviewer_id",
            "recency",
            "frequency",
            "monetary",
            "favourite_neighbourhood",
            "segment",
        }

        actual_columns = set(df_rfm.columns)

        assert expected_columns.issubset(
            actual_columns
        ), f"Missing columns: {expected_columns - actual_columns}"

    def test_rfm_metrics_validity(self, spark, ensure_data):
        """Test that RFM metrics are calculated correctly."""
        engine = RFMEngine(spark, enable_monitoring=False)
        df_rfm, _ = engine.process()

        # Recency (days)
        assert df_rfm["recency"].min() >= 0, "Recency cannot be negative"
        assert df_rfm["recency"].max() < 10000, "Recency suspiciously high"

        # Frequency
        assert df_rfm["frequency"].min() >= 1, "Frequency must be at least 1"
        assert df_rfm["frequency"].max() < 1000, "Frequency suspiciously high"

        # Monetary
        assert df_rfm["monetary"].min() >= 0, "Monetary cannot be negative"
        assert df_rfm["monetary"].max() < 1000000, "Monetary suspiciously high"

    def test_segments_present(self, spark, ensure_data):
        """Test that all expected segments are present in the output."""
        engine = RFMEngine(spark, enable_monitoring=False)
        df_rfm, _ = engine.process()

        expected_segments = {
            "Champions",
            "Loyal",
            "Potential",
            "At Risk",
            "Hibernating",
            "Lost",
        }
        actual_segments = set(df_rfm["segment"].unique())

        # At least 3 segments should be present to ensure meaningful segmentation
        assert len(actual_segments) >= 3, f"Too few segments: {actual_segments}"

        # All present segments should be valid
        invalid = actual_segments - expected_segments
        assert not invalid, f"Invalid segments found: {invalid}"

    def test_segment_distribution_reasonable(self, spark, ensure_data):
        """Check that the segment distribution is reasonable."""
        engine = RFMEngine(spark, enable_monitoring=False)
        df_rfm, _ = engine.process()

        total = len(df_rfm)
        segment_counts = df_rfm["segment"].value_counts()

        # No segment should contain more than 80% of customers (bad segmentation)
        for segment, count in segment_counts.items():
            pct = count / total
            assert pct < 0.8, f"Segment '{segment}' too large: {pct:.1%}"

        # At least 100 customers analyzed to ensure meaningful results
        # (dataset should be sufficient)
        assert total >= 100, f"Too few customers: {total}"

    def test_neighbourhood_metrics(self, spark, ensure_data):
        """Check that the neighbourhood metrics are valid."""
        engine = RFMEngine(spark, enable_monitoring=False)
        _, df_neighbourhood = engine.process()

        assert len(df_neighbourhood) > 0, "No neighbourhood metrics"
        assert len(df_neighbourhood) <= 20, "Too many neighbourhoods (should be top 20)"

        expected_cols = {
            "favourite_neighbourhood",
            "total_revenue",
            "avg_customer_value",
            "customer_count",
        }
        actual_cols = set(df_neighbourhood.columns)

        assert expected_cols.issubset(
            actual_cols
        ), f"Missing columns: {expected_cols - actual_cols}"

        # Positives Values
        assert (
            df_neighbourhood["total_revenue"].min() >= 0
        ), "Revenue cannot be negative"
        assert (
            df_neighbourhood["customer_count"].min() >= 1
        ), "Customer count must be at least 1"

    def test_data_quality_checks_executed(self, spark, ensure_data):
        """Check that data quality checks are executed."""
        engine = RFMEngine(spark, enable_monitoring=True)
        df_rfm, _ = engine.process()

        # Check if monitor is initialized
        assert engine.monitor is not None, "Monitor not initialized"

        # Check if metrics were collected
        metrics = engine.monitor.get_metrics()
        assert metrics.status in [
            "SUCCESS",
            "PARTIAL",
        ], f"Unexpected status: {metrics.status}"

        # Check data quality is validated
        assert hasattr(metrics, "data_quality_passed"), "Data quality not checked"

    def test_monitoring_metrics_collected(self, spark, ensure_data):
        """Check that monitoring metrics are collected."""
        engine = RFMEngine(spark, enable_monitoring=True)
        df_rfm, _ = engine.process()

        metrics = engine.monitor.get_metrics()

        # Timing metrics
        assert metrics.total_duration_seconds > 0, "Duration not measured"
        assert len(metrics.stage_durations) >= 4, "Not all stages measured"

        # Volume metrics
        assert metrics.output_rows > 0, "Output rows not recorded"
        assert len(metrics.input_rows) >= 2, "Input rows not recorded"

        # Segment metrics
        assert len(metrics.segments) > 0, "Segments not recorded"

    def test_pipeline_performance(self, spark, ensure_data):
        """Check that the pipeline executes in a reasonable amount of time."""
        import time

        engine = RFMEngine(spark, enable_monitoring=False)

        start = time.time()
        df_rfm, _ = engine.process()
        duration = time.time() - start

        # In local, should take less than 60 seconds
        assert duration < 60, f"Pipeline too slow: {duration:.2f}s (expected <60s)"

        print(f"Pipeline executed in {duration:.2f}s")


class TestDataIngestion:
    """Test ingestion module"""

    def test_download_datasets_success(self):
        """Check download success."""
        from config.settings import Settings

        settings = Settings()

        if settings.data.reviews_path.exists() and settings.data.listings_path.exists():
            pytest.skip("Datasets already present")

        result = download_datasets()

        assert result is True, "Download failed"
        assert settings.data.reviews_path.exists(), "Reviews not downloaded"
        assert settings.data.listings_path.exists(), "Listings not downloaded"


class TestDataCleaners:
    """Test of cleaning functions."""

    def test_clean_price_column(self, spark):
        from src.airbnb_rfm_pyspark.processing.cleaners import clean_price_column

        # Test data
        data = [
            (1, "$1,200.50"),
            (2, "$50.00"),
            (3, "$5.00"),  # Should be filtered (< 10)
            (4, "$20000.00"),  # Should be filtered (> 10000)
            (5, None),  # Should be filtered (NULL)
        ]

        df = spark.createDataFrame(data, ["id", "price"])

        result = clean_price_column(df, "price")

        # Check results
        cleaned_count = result.count()
        assert cleaned_count == 2, f"Expected 2 rows, got {cleaned_count}"

        # Check values
        prices = [row["price_cleaned"] for row in result.collect()]
        assert 1200.5 in prices, "Price 1200.5 missing"
        assert 50.0 in prices, "Price 50.0 missing"

    def test_normalize_date_column(self, spark):
        """Test Date normalization."""
        from src.airbnb_rfm_pyspark.processing.cleaners import normalize_date_column
        from pyspark.sql.types import DateType

        data = [
            (1, "2023-01-15"),
            (2, "2023-06-20"),
            (3, "invalid"),  # Should be NULL and filtered
        ]

        df = spark.createDataFrame(data, ["id", "date"])

        result = normalize_date_column(df, "date")

        # Check if invalid dates are filtered
        assert result.count() == 2, "Invalid dates not filtered"

        # Check type
        assert result.schema["date"].dataType == DateType(), "Date type not converted"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
