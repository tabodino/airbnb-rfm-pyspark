from dataclasses import dataclass
from typing import Dict, List, Callable, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.airbnb_rfm_pyspark.core.logger import logger


@dataclass
class DataQualityRule:
    """
    A data quality rule to be applied on a DataFrame.
    """

    name: str
    description: str
    check_function: Callable[[DataFrame], bool]
    severity: str = "ERROR"
    metric_name: Optional[str] = None


@dataclass
class DataQualityResult:
    """
    Result of a data quality check.
    """

    rule_name: str
    passed: bool
    severity: str
    message: str
    metric_value: Optional[float] = None


class DataQualityChecker:
    """
    Class to perform data quality checks on a DataFrame.
    """

    def __init__(self, fail_on_error: bool = True):
        self.fail_on_error = fail_on_error
        self.rules: List[DataQualityRule] = []
        self.results: List[DataQualityResult] = []

    def add_rule(self, rule: DataQualityRule):
        self.rules.append(rule)

    # =========================================================================
    # SCHEMA VALIDATION
    # =========================================================================

    def add_schema_check(
        self, expected_columns: List[str], allow_extra: bool = True
    ) -> "DataQualityChecker":
        """Add a schema validation rule to check for expected columns."""

        def check(df: DataFrame) -> bool:
            actual_cols = set(df.columns)
            expected_cols = set(expected_columns)

            missing = expected_cols - actual_cols

            if missing:
                logger.error(f"Missing columns: {missing}")
                return False

            if not allow_extra:
                extra = actual_cols - expected_cols
                if extra:
                    logger.warning(f"Unexpected extra columns: {extra}")

            logger.info(
                f"Schema check passed: {len(expected_cols)} required columns present"
            )
            return True

        self.rules.append(
            DataQualityRule(
                name="schema_validation",
                description=f"Check {len(expected_columns)} required columns",
                check_function=check,
                severity="ERROR",
            )
        )
        return self

    # =========================================================================
    # COMPLETENESS CHECKS
    # =========================================================================

    def add_completeness_check(
        self, column: str, min_completeness: float = 0.9, severity: str = "WARNING"
    ) -> "DataQualityChecker":
        """Add a completeness check for a specific column."""

        def check(df: DataFrame) -> bool:
            total_count = df.count()
            non_null_count = df.filter(F.col(column).isNotNull()).count()
            completeness = non_null_count / total_count if total_count > 0 else 0.0

            # Store metric for monitoring
            self._store_metric(f"completeness_{column}", completeness)

            if completeness < min_completeness:
                logger.warning(
                    f"Column '{column}': {completeness:.1%} complete "
                    f"(expected ≥{min_completeness:.1%})"
                )
                return False

            logger.info(f"Column '{column}': {completeness:.1%} complete")
            return True

        self.rules.append(
            DataQualityRule(
                name=f"completeness_check_{column}",
                description=f"Check completeness of column '{column}'",
                check_function=check,
                severity=severity,
                metric_name=f"completeness_{column}",
            )
        )
        return self

    # =========================================================================
    # UNIQUENESS CHECKS
    # =========================================================================

    def add_uniqueness_check(
        self, column: str, max_duplicates_pct: float = 0.01
    ) -> "DataQualityChecker":
        """Add a uniqueness check for a specific column."""

        def check(df: DataFrame) -> bool:
            total = df.count()
            distinct = df.select(column).distinct().count()
            duplicates = total - distinct
            dup_pct = duplicates / total if total > 0 else 0

            self._store_metric(f"duplicates_{column}_pct", dup_pct)

            if dup_pct > max_duplicates_pct:
                logger.warning(
                    f"Column '{column}': {dup_pct:.2%} duplicates "
                    f"(expected ≤{max_duplicates_pct:.2%})"
                )
                return False

            logger.info(f"Column '{column}': {dup_pct:.2%} duplicates (acceptable)")
            return True

        self.rules.append(
            DataQualityRule(
                name=f"uniqueness_{column}",
                description=f"Check {column} duplicates ≤{max_duplicates_pct:.1%}",
                check_function=check,
                severity="WARNING",
                metric_name=f"duplicates_{column}_pct",
            )
        )
        return self

    # =========================================================================
    # RANGE CHECKS
    # =========================================================================

    def add_range_check(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        max_outliers_pct: float = 0.05,
    ) -> "DataQualityChecker":
        """Add a range check for a specific column."""

        def check(df: DataFrame) -> bool:
            total = df.count()

            # Filter values within range
            filtered = df
            if min_value is not None:
                filtered = filtered.filter(F.col(column) >= min_value)
            if max_value is not None:
                filtered = filtered.filter(F.col(column) <= max_value)

            valid_count = filtered.count()
            outliers = total - valid_count
            outliers_pct = outliers / total if total > 0 else 0

            self._store_metric(f"outliers_{column}_pct", outliers_pct)

            if outliers_pct > max_outliers_pct:
                logger.warning(
                    f"Column '{column}': {outliers_pct:.2%} outliers "
                    f"(expected ≤{max_outliers_pct:.2%})"
                )
                return False

            logger.info(
                f"Column '{column}': {outliers_pct:.2%} outliers "
                f"(range: {min_value} - {max_value})"
            )
            return True

        self.rules.append(
            DataQualityRule(
                name=f"range_{column}",
                description=f"Check {column} in range [{min_value}, {max_value}]",
                check_function=check,
                severity="WARNING",
                metric_name=f"outliers_{column}_pct",
            )
        )
        return self

    # =========================================================================
    # FRESHNESS CHECKS
    # =========================================================================

    def add_freshness_check(
        self, date_column: str, max_age_days: int = 30
    ) -> "DataQualityChecker":
        """Add a freshness check based on a date column."""

        def check(df: DataFrame) -> bool:
            from datetime import datetime

            max_date = df.agg(F.max(date_column)).collect()[0][0]

            if max_date is None:
                logger.warning(f"No date found in '{date_column}'")
                return False

            # Convert in date if string
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date, "%Y-%m-%d")

            age_days = (datetime.now() - max_date).days

            self._store_metric(f"freshness_{date_column}_days", age_days)

            if age_days > max_age_days:
                logger.warning(
                    f"Data is {age_days} days old (expected ≤{max_age_days} days)"
                )
                return False

            logger.info(f"Data freshness: {age_days} days old")
            return True

        self.rules.append(
            DataQualityRule(
                name=f"freshness_{date_column}",
                description=f"Check data not older than {max_age_days} days",
                check_function=check,
                severity="WARNING",
                metric_name=f"freshness_{date_column}_days",
            )
        )
        return self

    # =========================================================================
    # EXECUTION
    # =========================================================================

    def run_checks(self, df: DataFrame) -> List[DataQualityResult]:
        """Execute all data quality checks on the provided DataFrame."""
        logger.info(f"Running {len(self.rules)} data quality checks...")

        self.results = []
        errors = []
        warnings = []

        for rule in self.rules:
            try:
                passed = rule.check_function(df)

                result = DataQualityResult(
                    rule_name=rule.name,
                    passed=passed,
                    severity=rule.severity,
                    message=rule.description,
                )
                self.results.append(result)

                if not passed:
                    if rule.severity == "ERROR":
                        errors.append(rule.name)
                    elif rule.severity == "WARNING":
                        warnings.append(rule.name)

            except Exception as e:
                logger.error(f"Check '{rule.name}' failed with exception: {e}")
                errors.append(rule.name)

                self.results.append(
                    DataQualityResult(
                        rule_name=rule.name,
                        passed=False,
                        severity="ERROR",
                        message=f"Exception: {e}",
                    )
                )

        # Log summary
        passed_count = sum(1 for r in self.results if r.passed)
        total = len(self.results)

        logger.info(f"\n{'='*60}")
        logger.info(f"DATA QUALITY SUMMARY: {passed_count}/{total} checks passed")

        if errors:
            logger.error(f"Errors: {len(errors)} - {errors}")
        if warnings:
            logger.warning(f"Warnings: {len(warnings)} - {warnings}")
        if passed_count == total:
            logger.success("All checks passed!")

        logger.info(f"{'='*60}\n")

        # Fail-fast if errors
        if errors and self.fail_on_error:
            raise RuntimeError(
                f"Data quality checks failed: {len(errors)} errors. "
                f"Failed checks: {errors}"
            )

        return self.results

    # =========================================================================
    # METRICS
    # =========================================================================

    def _store_metric(self, name: str, value: float):
        """Store metrics for monitoring."""
        logger.info(f"Metric: {name} = {value:.4f}")

    def get_metrics(self) -> Dict[str, float]:
        """Retrun all collected metrics."""
        return {
            r.rule_name: r.metric_value
            for r in self.results
            if r.metric_value is not None
        }


# =============================================================================
# HELPERS FOR EXECUTION
# =============================================================================


def validate_reviews_data(
    df: DataFrame, fail_on_error: bool = False
) -> List[DataQualityResult]:
    """Standard Validation for reviews dataset."""
    checker = DataQualityChecker(fail_on_error=fail_on_error)

    return (
        checker.add_schema_check(["listing_id", "reviewer_id", "date"])
        .add_completeness_check("listing_id", min_completeness=1.0, severity="ERROR")
        .add_completeness_check("date", min_completeness=0.95)
        .add_uniqueness_check("id", max_duplicates_pct=0.001)  # 0.1% max
        .run_checks(df)
    )


def validate_listings_data(
    df: DataFrame, fail_on_error: bool = False
) -> List[DataQualityResult]:
    """Standard Validation for listings dataset."""
    checker = DataQualityChecker(fail_on_error=fail_on_error)

    return (
        checker.add_schema_check(["id", "price", "neighbourhood_cleansed"])
        .add_completeness_check("id", min_completeness=1.0, severity="ERROR")
        .add_completeness_check("price", min_completeness=0.8)
        .add_range_check(
            "price_cleaned", min_value=10, max_value=10000, max_outliers_pct=0.05
        )
        .run_checks(df)
    )
