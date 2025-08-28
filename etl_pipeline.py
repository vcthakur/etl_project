import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")



import importlib
import pandas as pd

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
        "extractor": {
            "module": "extractors.api_extractor",
            "class": "APIExtractor",
            "params": {
                "url": "http://127.0.0.1:5001/api/orders",
                "method": "GET"
            }
        },
        "transformations": [
            {
                "module": "transformations.basic_cleaning",
                "class": "BasicCleaning",
                "params": {}
            }
        ],
        "loader": {
            "module": "loaders.csv_loader",
            "class": "CSVLoader",
            "params": {
                "path": "data/output.csv"
            }
        }
    }

    run_etl(pipeline_config)
