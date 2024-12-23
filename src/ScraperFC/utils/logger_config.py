from loguru import logger
import sys
from pathlib import Path
import os

def setup_logging(name=__name__, log_level="INFO", log_file="football_ml.log"):
    """
    Configure logging using loguru with both file and console outputs.
    Console: INFO and above
    File: DEBUG and above
    Format: project, lvl, dd/mm/yy:time->message
    
    Args:
        name: Logger name (usually __name__)
        log_level: Logging level for the logger (default: "INFO")
        log_file: Name of the log file (default: football_ml.log)
    
    Returns:
        logger: Configured logger instance
    """
    # Remove any existing handlers
    logger.remove()

    # Use home directory for logs
    home_dir = os.path.expanduser("~")
    log_dir = os.path.join(home_dir, "logs")
    
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Full path to log file
    log_path = os.path.join(log_dir, log_file)

    # Define log formats with project name
    console_format = "<green>{time:DD/MM/YY:HH:mm:ss}</green> | <level>{level.icon}</level> <blue>[ScraperFC]</blue> <level>{level: <8}</level> | <cyan>{file}:{line}</cyan> - <level>{message}</level>"
    file_format = "{time:DD/MM/YY:HH:mm:ss} | [ScraperFC] {level: <8} | {file}:{line} - {message}"
    
    # Add console handler
    logger.add(
        sys.stderr,
        format=console_format,
        level=log_level,
        colorize=True,
        backtrace=True,
        diagnose=True
    )
    
    # Add file handler
    logger.add(
        log_path,
        format=file_format,
        level="DEBUG",
        rotation="500 MB",
        compression="zip",
        mode="a"  # Append to log file
    )
    
    return logger
