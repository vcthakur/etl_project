import logging
import pandas as pd

class BasicCleaning:
    def __init__(self, config: dict):
        # allow configurable options later
        self.dropna_columns = config.get("dropna_columns", ["id"])
        self.strip_whitespace = config.get("strip_whitespace", True)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Applying basic cleaning...")
        if df.empty:
            return df

        cleaned_df = df.copy()

        # Drop nulls only for specific columns
        if self.dropna_columns:
            cleaned_df = cleaned_df.dropna(subset=self.dropna_columns)

        # Strip whitespace in string columns
        if self.strip_whitespace:
            for col in cleaned_df.select_dtypes(include="object").columns:
                cleaned_df[col] = cleaned_df[col].str.strip()

        return cleaned_df
