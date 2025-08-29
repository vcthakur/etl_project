# loaders/sql_loader.py
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean  # Import necessary data types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any
import logging

Base = declarative_base()

class SQLLoader:
    """
    Loads data from a Pandas DataFrame into a SQL database table using SQLAlchemy.
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the SQLLoader with database connection parameters and table information.

        Args:
            params (Dict[str, Any]): A dictionary containing the following keys:
                - `SQLALCHEMY_DATABASE_URI` (str): The database connection URI.
                - `table_name` (str): The name of the table to load data into.
                - `dtype` (dict, optional): A dictionary specifying the SQLAlchemy data types for each column in the table.
                    If not provided, data types will be inferred from the DataFrame.
        """
        self.database_uri = params.get("SQLALCHEMY_DATABASE_URI")
        self.table_name = params.get("table_name")
        self.dtype = params.get("dtype")  # Optional: Specify column data types

        if not self.database_uri:
            raise ValueError("Database URI must be provided in params.")
        if not self.table_name:
            raise ValueError("Table name must be provided in params.")

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
        self.Session = sessionmaker(bind=self.engine)
        self.Base = Base # Assign Base to self.Base
        logging.info(f"SQLLoader initialized for table: {self.table_name} with URI: {self.database_uri[:20]}...") # Log with truncated URI

    def load(self, df: pd.DataFrame):
        """
        Loads data from the given Pandas DataFrame into the specified SQL table.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to load.
        """
        logging.info(f"Loading data into table: {self.table_name}. DataFrame shape: {df.shape}")

        try:
            # Define the table dynamically using SQLAlchemy ORM
            class DataTable(self.Base):
                __tablename__ = self.table_name

                # Dynamically create columns based on DataFrame columns and specified dtypes
                id = Column(Integer, primary_key=True)  # Add an auto-incrementing primary key column

                def __init__(self, **kwargs):
                    super().__init__(**kwargs)

            # Add columns based on DataFrame
            for column_name, dtype in df.dtypes.items():
                if column_name not in DataTable.__table__.columns: # Prevents duplicate columns
                    if self.dtype and column_name in self.dtype:
                        column_type = self.dtype[column_name]
                    else:
                        # Infer SQLAlchemy data type from Pandas dtype
                        if dtype == 'int64':
                            column_type = Integer
                        elif dtype == 'float64':
                            column_type = Float
                        elif dtype == 'bool':
                            column_type = Boolean
                        else:
                            column_type = String # Default to String for other types

                    # Add the new column to the DataTable class
                    new_column = Column(column_name, column_type)
                    setattr(DataTable, column_name, new_column)

            self.Base.metadata.create_all(self.engine)  # Create the table in the database

            with self.Session() as session:
                # Iterate over the DataFrame rows and insert data into the table
                for _, row in df.iterrows():
                    try:
                        data = row.to_dict()
                        data.pop('id', None)
                        record = DataTable(**data)
                        session.add(record)
                    except Exception as record_error:
                        logging.warning(f"Skipping row due to error creating record: {record_error}")
                        session.rollback()
                        continue # Skip problematic row

                session.commit()
            logging.info("Data loading completed successfully.")

        except Exception as e:
            logging.error(f"Error loading data into SQL table: {e}")
            raise

