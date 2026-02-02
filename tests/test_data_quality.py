import pytest
from pyspark.sql import SparkSession
from src.airbnb_rfm_pyspark.core.data_quality import DataQualityChecker


@pytest.fixture(scope="module")
def spark():
    """Spark session for tests."""
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


class TestDataQualityChecker:
    """Tests of DataQualityChecker."""

    def test_schema_check_success(self, spark):
        """Test: Schema check with all columns present."""
        df = spark.createDataFrame(
            [(1, "A", 100), (2, "B", 200)], ["id", "name", "value"]
        )

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_schema_check(["id", "name"])

        results = checker.run_checks(df)

        assert len(results) == 1
        assert results[0].passed is True

    def test_schema_check_failure(self, spark):
        """Test: Schema check with missing column."""
        df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_schema_check(["id", "name", "missing_col"])

        results = checker.run_checks(df)

        assert len(results) == 1
        assert results[0].passed is False

    def test_completeness_check_success(self, spark):
        """Test: Completeness check with a 100% non-null."""
        df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "name"])

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_completeness_check("name", min_completeness=0.9)

        results = checker.run_checks(df)

        assert results[0].passed is True

    def test_completeness_check_failure(self, spark):
        """Test: Completeness check with 50% of NULL."""
        df = spark.createDataFrame([(1, "A"), (2, None), (3, None)], ["id", "name"])

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_completeness_check("name", min_completeness=0.5)

        results = checker.run_checks(df)

        # 33% completeness < 50% required
        assert results[0].passed is False

    def test_range_check_success(self, spark):
        """Test: Range check with valid values."""
        df = spark.createDataFrame([(1, 25), (2, 30), (3, 45)], ["id", "age"])

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_range_check("age", min_value=18, max_value=65, max_outliers_pct=0.1)

        results = checker.run_checks(df)

        assert results[0].passed is True

    def test_range_check_with_outliers(self, spark):
        """Test: Range check with outliers."""
        df = spark.createDataFrame(
            [(1, 25), (2, 150), (3, 200)], ["id", "age"]  # 2 outliers sur 3
        )

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_range_check("age", min_value=18, max_value=65, max_outliers_pct=0.5)

        results = checker.run_checks(df)

        # 66% outliers > 50% max -> fail
        assert results[0].passed is False

    def test_uniqueness_check_success(self, spark):
        """Test: Uniqueness check without duplicates."""
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_uniqueness_check("id", max_duplicates_pct=0.01)

        results = checker.run_checks(df)

        assert results[0].passed is True

    def test_uniqueness_check_with_duplicates(self, spark):
        """Test: Uniqueness check with duplicates."""
        df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])  # 1 duplicate

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_uniqueness_check("id", max_duplicates_pct=0.2)

        results = checker.run_checks(df)

        # 33% duplicates > 20% max -> fail
        assert results[0].passed is False

    def test_fail_on_error_raises_exception(self, spark):
        """Test: fail_on_error=True raise exception."""
        df = spark.createDataFrame([(1,)], ["id"])

        checker = DataQualityChecker(fail_on_error=True)
        checker.add_schema_check(["id", "missing"])

        with pytest.raises(RuntimeError, match="Data quality checks failed"):
            checker.run_checks(df)

    def test_multiple_checks(self, spark):
        """Test: Mutliple checks."""
        df = spark.createDataFrame(
            [(1, "A", 25), (2, "B", 30), (3, None, 35)], ["id", "name", "age"]
        )

        checker = DataQualityChecker(fail_on_error=False)
        checker.add_schema_check(["id", "name", "age"])
        checker.add_completeness_check("name", 0.8)  # 66% -> fail
        checker.add_range_check("age", 18, 65)

        results = checker.run_checks(df)

        assert len(results) == 3
        assert results[0].passed is True  # Schema OK
        assert results[1].passed is False  # Completeness fail
        assert results[2].passed is True  # Range OK


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
