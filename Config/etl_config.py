# etl_config.py
# Configuration for ETL process including extractors, loaders, and transformations.
from Config.config import config
from Config import etl_config

EXTRACTORS = {
    "CSV": {
        "module": "extractors.csv_extractor",
        "class": "CSVExtractor",
        "params": {
            # Add required CSV params here, e.g., 
             "path": "data/1kproduct.csv"  # Example path
        }
    },
    "SQL": {
        "module": "extractors.sql_extractor",
        "class": "SQLExtractor",
        "params": {
            # Add required JSON params here
            "SQLALCHEMY_DATABASE_URI": config.SQLALCHEMY_DATABASE_URI,
            "query": "SELECT * FROM addresses"  # Example query
        }
    }
}

LOADERS = {
    "SQL": {
        "module": "loaders.sql_loader",
        "class": "SQLLoader",
        "params": {
            # Add required SQL params here, e.g., 
             "SQLALCHEMY_DATABASE_URI": config.SQLALCHEMY_DATABASE_URI, 
             "table_name": "products"  # Example table name
        }
    },
    "CSV": {
        "module": "loaders.csv_loader",
        "class": "CSVLoader",
        "params": {
            # Add required CSV loader params here
             "path": "data/addrload.csv"  # Example path
        }
    }
}

TRANSFORMATIONS = {
    "basic_cleaning": {
        "module": "transformations.basic_cleaning",
        "class": "BasicCleaning",
        "params": {
            # Add transformation params here
        }
    }
}


def get_extractor_config(extractor_type):
    return EXTRACTORS.get(extractor_type, None)

def get_loader_config(loader_type):
    return LOADERS.get(loader_type, None)

def get_transformations_config():
    # Customize as needed; returning list of dicts
    return list(TRANSFORMATIONS.values())

