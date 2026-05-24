import os
from dataclasses import dataclass
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str

@dataclass
class PipelineConfig:
    database: DatabaseConfig
    api_endpoint: str
    api_key: str
    input_path: str
    output_path: str
    batch_size: int = 1000

def load_config() -> PipelineConfig:
    """Loads configuration from environment variables."""
    
    # 1. Populate the DatabaseConfig dataclass
    db_config = DatabaseConfig(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", 5432)),
        database=os.environ.get("DB_NAME", "postgres"),
        username=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "1344")
    )

    # 2. Populate and return the PipelineConfig dataclass
    return PipelineConfig(
        database=db_config,
        api_endpoint=os.environ.get("API_ENDPOINT", ""),
        api_key=os.environ.get("API_KEY", ""),
        input_path=os.environ.get("INPUT_PATH", "data/raw/product_catalog.csv"),
        output_path=os.environ.get("OUTPUT_PATH", "data/processed/"),
        batch_size=1000  # Default batch size as per requirements
    )

# Quick test to ensure it works
if __name__ == "__main__":
    config = load_config()
    print(f"Loaded config for database host: {config.database.host}")