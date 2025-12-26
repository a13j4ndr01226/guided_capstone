import os
from dotenv import load_dotenv
from pathlib import Path

# Resolve project root → config/.env
ENV_PATH = Path(__file__).resolve().parents[1] / "config" / ".env"

load_dotenv(dotenv_path=ENV_PATH)

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
AZURE_BLOB_SAS = os.getenv("AZURE_BLOB_SAS")
