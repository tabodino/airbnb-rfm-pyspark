from pathlib import Path
from typing import Dict
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SparkConfig(BaseSettings):
    """Configuration for Spark session optimized in local mode"""

    model_config: SettingsConfigDict = SettingsConfigDict(
        env_prefix="SPARK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Airbnb RFM Segmentation"
    master: str = "local[*]"
    driver_memory: str = "4g"
    executor_memory: str = "2g"
    shuffle_partitions: int = 128
    log_level: str = "WARN"


class DataConfig(BaseSettings):
    """Configuration for data paths and URLs"""

    model_config = SettingsConfigDict(
        env_prefix="DATA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    reviews_url: str = (
        "https://data.insideairbnb.com/france/pyr%C3%A9n%C3%A9es-atlantiques/pays-basque/2025-09-21/data/listings.csv.gz"
    )
    listings_url: str = (
        "https://data.insideairbnb.com/france/pyr%C3%A9n%C3%A9es-atlantiques/pays-basque/2025-09-21/data/reviews.csv.gz"
    )

    data_dir: Path = Path("data")
    chunk_size: int = 8192
    timeout: int = 30
    max_retries: int = 3

    @property
    def reviews_path(self) -> Path:
        """Path to reviews data file."""
        return self.data_dir / "reviews.csv.gz"

    @property
    def listings_path(self) -> Path:
        """Path to listings data file."""
        return self.data_dir / "listings.csv.gz"


class RFMConfig(BaseSettings):
    """Configuration for RFM parameters"""

    model_config = SettingsConfigDict(
        env_prefix="RFM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    reference_date: str = "2025-09-12"
    avg_nights_per_booking: int = 3
    min_price_filter: float = 10.0
    max_price_filter: float = 10000.0

    segment_rules: Dict[str, tuple[int, int, float]] = {
        "Champions": (90, 5, 500),  # Very recent + Frequent + High LTV
        "Loyal": (180, 3, 300),  # Moderately regular
        "Potential": (365, 1, 50),  # Recent but infrequent
        "At Risk": (730, 2, 100),  # Former high‑value customers
        "Hibernating": (1000, 1, 10),  # Inactive for more than 2 years
        "Lost": (1000, 0, 0),  # Very old + low value
    }


class Settings(BaseSettings):
    """Application settings"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment
    environment: str = Field(
        default="development",
        pattern="^(development|production)$",
        description="Application environment",
    )
    debug: bool = True

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Application logging level",
    )
    log_format: str = Field(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    # Sub-configurations
    spark: SparkConfig = SparkConfig()
    data: DataConfig = DataConfig()
    rfm: RFMConfig = RFMConfig()


# Singleton instance
settings = Settings()
