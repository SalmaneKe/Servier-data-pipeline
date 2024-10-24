import logging


def setup_logging():
    """Configure the logging settings."""
    logging.basicConfig(
        level=logging.INFO,  # Default level is INFO; change to DEBUG for more details
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler("pipeline.log"),  # Log to a file
        ],
    )
