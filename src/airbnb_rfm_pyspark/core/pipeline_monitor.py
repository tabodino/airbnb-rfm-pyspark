import time
import json
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, List
from contextlib import contextmanager
from src.airbnb_rfm_pyspark.core.logger import logger


@dataclass
class PipelineMetrics:
    """Metrics pipeline data."""

    run_id: str
    timestamp: str
    status: str  # SUCCESS, FAILED, PARTIAL

    # Timing
    total_duration_seconds: float
    stage_durations: Dict[str, float]

    # Volume
    input_rows: Dict[str, int]
    output_rows: int
    rows_filtered: int

    # Quality
    data_quality_passed: bool
    data_quality_warnings: int
    data_quality_errors: int

    # Business
    segments: Dict[str, int]

    # Errors
    errors: List[str]


class PipelineMonitor:
    """Monitor pipeline data."""

    def __init__(self, pipeline_name: str = "airbnb_rfm"):
        self.pipeline_name = pipeline_name
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = time.time()

        # Metrics storage
        self.stage_durations: Dict[str, float] = {}
        self.input_rows: Dict[str, int] = {}
        self.output_rows: int = 0
        self.rows_filtered: int = 0

        self.data_quality_passed: bool = True
        self.data_quality_warnings: int = 0
        self.data_quality_errors: int = 0

        self.segments: Dict[str, int] = {}
        self.errors: List[str] = []

        self.status: str = "RUNNING"

        logger.info(f"Pipeline started: {self.pipeline_name} (run_id: {self.run_id})")

    @contextmanager
    def stage(self, stage_name: str):
        """Context manager for measuring stage duration."""

        logger.info(f"Stage: {stage_name}")
        start = time.time()

        try:
            yield
            duration = time.time() - start
            self.stage_durations[stage_name] = duration
            logger.info(f"Stage '{stage_name}' completed in {duration:.2f}s")

        except Exception as e:
            duration = time.time() - start
            self.stage_durations[stage_name] = duration
            self.errors.append(f"{stage_name}: {str(e)}")
            logger.error(f"Stage '{stage_name}' failed after {duration:.2f}s: {e}")
            raise

    def record_input_rows(self, source: str, count: int):
        """Save number of input rows."""
        self.input_rows[source] = count
        logger.info(f"Input: {source} = {count:,} rows")

    def record_output_rows(self, count: int):
        """Save number of output rows."""
        self.output_rows = count
        logger.info(f"Output: {count:,} rows")

    def record_filtered_rows(self, count: int):
        """Save number of filtered rows."""
        self.rows_filtered = count
        logger.info(f"Filtered: {count:,} rows")

    def record_data_quality(self, passed: bool, warnings: int = 0, errors: int = 0):
        """Save data quality results."""
        self.data_quality_passed = passed
        self.data_quality_warnings = warnings
        self.data_quality_errors = errors

        if passed:
            logger.info(f"Data quality: PASSED (warnings: {warnings})")
        else:
            logger.warning(
                f"Data quality: FAILED (errors: {errors}, warnings: {warnings})"
            )

    def record_segments(self, segments: Dict[str, int]):
        """Save RFM segment distribution."""
        self.segments = segments
        logger.info(f"Segments: {segments}")

    def mark_success(self):
        """Set pipeline as successful."""
        self.status = "SUCCESS"
        logger.success("Pipeline completed successfully")

    def mark_failed(self, error: str):
        """Set pipeline as failed."""
        self.status = "FAILED"
        self.errors.append(error)
        logger.error(f"Pipeline failed: {error}")

    def mark_partial(self):
        """Set pipeline as partially successful."""
        self.status = "PARTIAL"
        logger.warning("Pipeline completed with warnings")

    def get_metrics(self) -> PipelineMetrics:
        """Return full metrics."""
        total_duration = time.time() - self.start_time

        return PipelineMetrics(
            run_id=self.run_id,
            timestamp=datetime.now().isoformat(),
            status=self.status,
            total_duration_seconds=total_duration,
            stage_durations=self.stage_durations,
            input_rows=self.input_rows,
            output_rows=self.output_rows,
            rows_filtered=self.rows_filtered,
            data_quality_passed=self.data_quality_passed,
            data_quality_warnings=self.data_quality_warnings,
            data_quality_errors=self.data_quality_errors,
            segments=self.segments,
            errors=self.errors,
        )

    def save_metrics(self, output_dir: str = "logs/metrics"):
        """
        Save metrics to JSON file.

        Pattern: Time-series metrics for long-term monitoring.
        Useful to detect performance/quality degradations.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        metrics = self.get_metrics()
        filename = f"pipeline_metrics_{self.run_id}.json"
        filepath = output_path / filename

        with open(filepath, "w") as f:
            json.dump(asdict(metrics), f, indent=2)

        logger.info(f"Metrics saved: {filepath}")

        # Log summary
        self._log_summary(metrics)

    def _log_summary(self, metrics: PipelineMetrics):
        """Executetion summary log."""
        logger.info("\n" + "=" * 70)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Run ID: {metrics.run_id}")
        logger.info(f"Status: {metrics.status}")
        logger.info(f"Duration: {metrics.total_duration_seconds:.2f}s")
        logger.info("\nStage Timings:")
        for stage, duration in metrics.stage_durations.items():
            pct = (
                (duration / metrics.total_duration_seconds * 100)
                if metrics.total_duration_seconds > 0
                else 0
            )
            logger.info(f"  {stage:<20} {duration:>6.2f}s ({pct:>5.1f}%)")

        logger.info("\nData Volume:")
        for source, count in metrics.input_rows.items():
            logger.info(f"  Input {source:<15} {count:>10,} rows")
        logger.info(f"  Output (final):      {metrics.output_rows:>10,} rows")
        logger.info(f"  Filtered:            {metrics.rows_filtered:>10,} rows")

        if metrics.segments:
            logger.info("\nSegments:")
            for segment, count in metrics.segments.items():
                pct = (
                    (count / metrics.output_rows * 100)
                    if metrics.output_rows > 0
                    else 0
                )
                logger.info(f"  {segment:<15} {count:>8,} ({pct:>5.1f}%)")

        if metrics.errors:
            logger.info(f"\nErrors ({len(metrics.errors)}):")
            for error in metrics.errors:
                logger.error(f"  - {error}")

        logger.info("=" * 70 + "\n")


# =============================================================================
# ALERTING SIMPLE
# =============================================================================


class SimpleAlerter:
    """Simple threshold-based alerting."""

    @staticmethod
    def check_alerts(metrics: PipelineMetrics):
        """Check if alerts should be triggered."""
        alerts = []

        # Alert 1: Duration too long
        if metrics.total_duration_seconds > 300:  # 5 minutes
            alerts.append(
                f"SLOW: Pipeline took {metrics.total_duration_seconds:.0f}s (>5min)"
            )

        # Alert 2: Too many rows filtered
        if metrics.input_rows and metrics.output_rows:
            total_input = sum(metrics.input_rows.values())
            filter_pct = (
                (metrics.rows_filtered / total_input * 100) if total_input > 0 else 0
            )

            if filter_pct > 20:  # >20% filtered
                alerts.append(f"HIGH FILTER RATE: {filter_pct:.1f}% rows filtered")

        # Alert 3: Data quality issues
        if not metrics.data_quality_passed:
            alerts.append(f"DATA QUALITY: {metrics.data_quality_errors} errors")

        # Alert 4: Pipeline failed
        if metrics.status == "FAILED":
            alerts.append(f"PIPELINE FAILED: {metrics.errors}")

        # Log alerts
        if alerts:
            logger.warning("\nALERTS TRIGGERED:")
            for alert in alerts:
                logger.warning(f"   {alert}")

            # In production: Send to Slack/Email
            # send_slack_alert(alerts)

        return alerts
