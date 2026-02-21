from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    alphavantage_key: str
    fred_key: str
    db_path: str = "data.sqlite"

def get_settings() -> Settings:
    av = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    fred = os.getenv("FRED_API_KEY", "").strip()
    if not av:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY in .env")
    if not fred:
        raise RuntimeError("Missing FRED_API_KEY in .env")
    return Settings(alphavantage_key=av, fred_key=fred)
