import logging
from utils.logger import get_logger
from utils.config_reader import load_config

# Import extractors, transformations, loaders dynamically
from extractors.csv_extractor import CSVExtractor
from transformations.basic_cleaning import BasicCleaning
from loaders.csv_loader import CSVLoader

logger = get_logger()

class ETLPipeline:
    def __init__(self, config):
        self.config = config

    def run(self):
        try:
            logger.info("ETL Pipeline Started")

            # 1. Extract
            extractor = CSVExtractor(self.config["extract"])
            data = extractor.extract()
            logger.info(f"Extracted {len(data)} records")

            # 2. Transform
            transformer = BasicCleaning(self.config["transform"])
            data = transformer.transform(data)
            logger.info("Transformations applied")

            # 3. Load
            loader = CSVLoader(self.config["load"])
            loader.load(data)
            logger.info("Data successfully loaded")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    config = load_config("config.yaml")
    pipeline = ETLPipeline(config)
    pipeline.run()
