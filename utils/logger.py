import logging

def get_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(message)s",
        handlers=[
            logging.FileHandler("logs/etl_pipeline.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)
