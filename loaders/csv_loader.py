import logging

class CSVLoader:
    def __init__(self, config):
        self.output_path = config["path"]

    def load(self, df):
        logging.info(f"Saving results to {self.output_path}")
        df.to_csv(self.output_path, index=False)
