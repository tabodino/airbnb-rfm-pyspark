import sys
from loguru import logger
from config.settings import Settings


logger.remove()  # Remove default logger

logger.add(
    sys.stdout,
    format=Settings().log_format,
    level=Settings().log_level,
    colorize=True,
    backtrace=True,
    diagnose=True,
)

if Settings().environment == "production":
    logger.add(
        "logs/app_{time:YYYY-MM-DD}.log",
        rotation="500 MB",
        retention="30 days",
        compression="zip",
        format=Settings().log_format,
        level=Settings().log_level,
    )


def log_execution_time(func):
    """Decorator to log the execution time of a function."""

    import time
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Execution time of {func.__name__}: {execution_time:.4f} seconds")
        return result

    return wrapper
