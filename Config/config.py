# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

class Config:
    # Flask settings
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")

    # SQLAlchemy settings (SQL Server example with pyodbc driver)
    DB_USER = os.getenv("DB_USER", "sa")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "YourStrongPassword123")
    DB_SERVER = os.getenv("DB_SERVER", "localhost")
    DB_NAME = os.getenv("DB_NAME", "retailshopdb")
    DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    SQLALCHEMY_DATABASE_URI = (
        f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_SERVER}/{DB_NAME}?driver={DB_DRIVER.replace(' ', '+')}"
        f"&TrustServerCertificate=yes&Encrypt=yes&Connection+Timeout=30"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

# Global config instance
config = Config()
