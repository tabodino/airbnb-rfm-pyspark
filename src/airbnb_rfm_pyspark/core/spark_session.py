from pyspark.sql import SparkSession
from config.settings import Settings
from src.airbnb_rfm_pyspark.core.logger import logger


class SparkSessionManager:
    """Manages the Spark in a Singleton session."""

    _instance: SparkSession | None = None

    @classmethod
    def get_session(cls) -> SparkSession:
        """Get the singleton Spark session instance."""
        if cls._instance is None:
            logger.info("Initializing Spark Session...")
            config = Settings().spark

            cls._instance = (
                SparkSession.builder.appName(config.app_name)
                .master(config.master)
                .config("spark.driver.memory", config.driver_memory)
                .config("spark.executor.memory", config.executor_memory)
                .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
                .config(
                    "spark.sql.adaptive.enabled", "true"
                )  # Adaptive Query Execution
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )

            cls._instance.sparkContext.setLogLevel(config.log_level)

            logger.success(
                f"Spark Session initialized: {config.master} "
                f"with {config.driver_memory} driver memory"
            )

        return cls._instance

    @classmethod
    def stop_session(cls):
        """Stop the Spark session if it exists."""
        if cls._instance is not None:
            logger.info("Stopping Spark Session...")
            cls._instance.stop()
            cls._instance = None
            logger.success("Spark Session stopped")


def get_spark() -> SparkSession:
    """Convenience function to get the Singleton Spark session."""
    return SparkSessionManager.get_session()


def stop_spark():
    """Convenience function to stop the Spark session."""
    return SparkSessionManager.stop_session()
