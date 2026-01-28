from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.airbnb_rfm_pyspark.core.logger import logger, log_execution_time
from config.settings import Settings


class DataIngestion:
    def __init__(self):
        self.data_config = Settings().data
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.data_config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update(
            {"User-Agent": "Mozilla/5.0 (compatible; AirbnbRFMBot/1.0)"}
        )

        return session

    def _download_file(
        self, name: str, url: str, filepath: Path
    ) -> Tuple[str, bool, str]:
        """Download a file from a URL to a specified path."""

        if filepath.exists():
            logger.info(f"{name} already exists at {filepath}, skipping download.")
            return name, True, str(filepath)

        try:
            logger.info(f"Starting download of {name} from {url}")
            start_time = time.perf_counter()

            response = self.session.get(
                url, stream=True, timeout=self.data_config.timeout
            )
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(
                    chunk_size=self.data_config.chunk_size
                ):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Calculate download progress
                        if downloaded % (10 * 1024 * 1024) == 0:
                            progress = (
                                (downloaded / total_size * 100) if total_size else 0
                            )
                            logger.debug(
                                f"{name}: Downloaded {downloaded / 1e6:.1f}MB "
                                f"{progress:.1f}%"
                            )

            elapsed = time.perf_counter() - start_time
            size_mb = filepath.stat().st_size / 1e6
            speed_mbps = size_mb / elapsed if elapsed > 0 else 0

            logger.success(
                f"{name}: Downloaded {size_mb:.1f}MB in {elapsed:.2f}s "
                f"({speed_mbps:.1f}MB/s)"
            )
            return (name, True, f"Downloaded to {size_mb:.1f}MB")

        except requests.exceptions.Timeout:
            logger.error(f"{name}: Download timeout after {self.data_config.timeout}s")
            return (name, False, "Timeout")
        except requests.exceptions.RequestException as e:
            logger.error(f"{name}: Download failed - {e}")
            return (name, False, str(e))
        except IOError as e:
            logger.error(f"{name}: File write error - {e}")
            return (name, False, f"I/O Error: {e}")

    @log_execution_time
    def download_all(self) -> Dict[str, bool]:
        """Download all required data files concurrently."""

        self.data_config.data_dir.mkdir(parents=True, exist_ok=True)

        download_tasks = [
            (
                "reviews",
                str(self.data_config.reviews_url),
                self.data_config.reviews_path,
            ),
            (
                "listings",
                str(self.data_config.listings_url),
                self.data_config.listings_path,
            ),
        ]

        results = {}
        # Parallel download using ThreadPoolExecutor
        with ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="Downloader"
        ) as executor:
            future_to_name = {
                executor.submit(self._download_file, name, url, path): name
                for name, url, path in download_tasks
            }
            # Waiting result (non-blocking, streaming)
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                _, success, message = future.result()
                results[name] = success

                if not success:
                    logger.warning(f"Download incomplete for {name}: {message}")

        # Final check
        all_success = all(results.values())
        if all_success:
            logger.success("All datasets downloaded successfully")
        else:
            failed = [name for name, status in results.items() if not status]
            logger.error(f"Failed downloads: {', '.join(failed)}")

        return results


@log_execution_time
def download_datasets() -> bool:
    """Convenience function to download all datasets."""
    ingestion = DataIngestion()
    results = ingestion.download_all()
    return all(results.values())


if __name__ == "__main__":
    success = download_datasets()
    exit(0 if success else 1)
