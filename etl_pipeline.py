
import importlib
import pandas as pd
from Config.config import config
from Config import etl_config


def load_class(module_path: str, class_name: str):
    """Dynamically import and return a class from a module."""
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def run_etl(config: dict):
    # 1. Extract
    extractor_class = load_class(config["extractor"]["module"], config["extractor"]["class"])
    extractor = extractor_class(config["extractor"].get("params", {}))
    df = extractor.extract()

    # 2. Transform
    for transform_cfg in config.get("transformations", []):
        transformer_class = load_class(transform_cfg["module"], transform_cfg["class"])
        transformer = transformer_class(transform_cfg.get("params", {}))
        df = transformer.transform(df)

    # 3. Load
    loader_class = load_class(config["loader"]["module"], config["loader"]["class"])
    loader = loader_class(config["loader"].get("params", {}))
    loader.load(df)

if __name__ == "__main__":
    # Example pipeline config
    pipeline_config = {
        "extractor": etl_config.get_extractor_config("CSV"),
        "transformations": etl_config.get_transformations_config(),
        "loader": etl_config.get_loader_config("SQL")
    }

    run_etl(pipeline_config)
