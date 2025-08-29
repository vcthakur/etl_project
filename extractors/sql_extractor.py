# extractors/sql_extractor.py
from Config.config import config
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any
import logging

# ------------------------------
# SQLAlchemy ORM Base class
# ------------------------------
Base = declarative_base()

class SQLExtractor:
    """
    Extracts data from a SQL database using SQLAlchemy.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the SQLExtractor with database connection parameters and a SQL query.

        Args:
            params (Dict[str, Any]): A dictionary containing the following keys:
                - `SQLALCHEMY_DATABASE_URI` (str): The database connection URI.
                - `query` (str): The SQL query to execute.
        """
        self.database_uri = params.get("SQLALCHEMY_DATABASE_URI")
        self.query = params.get("query")

        if not self.database_uri:
            raise ValueError("Database URI must be provided in params.")
        if not self.query:
            raise ValueError("SQL query must be provided in params.")

        self.engine = create_engine(
            self.database_uri,
            echo=False,
            future=True,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                "timeout": 30,
                "connect_timeout": 30
            }
        )
        self.Session = sessionmaker(bind=self.engine)  # Create a session factory
        logging.info(f"SQLExtractor initialized with URI: {self.database_uri[:20]}... and query: {self.query[:50]}...") # Log with truncated URI and query


    def extract(self) -> pd.DataFrame:
        """
        Executes the SQL query and returns the result as a Pandas DataFrame.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the results of the query.
        """
        logging.info(f"Executing SQL query: {self.query[:50]}...") # Log with truncated query
        try:
            with self.Session() as session: # Use context manager for session
                df = pd.read_sql_query(self.query, session.bind)
                logging.info(f"Query executed successfully. Extracted {len(df)} rows.")
                return df
        except Exception as e:
            logging.error(f"Error executing SQL query: {e}")
            raise

