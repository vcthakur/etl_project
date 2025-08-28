import pandas as pd
import logging

class CSVExtractor:
    def __init__(self, config):
        self.file_path = config["path"]

    def extract(self):
        logging.info(f"Reading CSV from {self.file_path}")
        return pd.read_csv(self.file_path)
