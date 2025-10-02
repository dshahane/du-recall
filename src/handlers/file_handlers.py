import abc
import pandas as pd
from typing import List, Dict, Any, Union
from urllib.parse import urlparse

# Define a type alias for the core data format used throughout the pipeline
ProcessedData = List[Dict[str, Any]]


# --- Core Handler Interface ---

class FileHandler(abc.ABC):
    """Abstract Base Class for all file and source handlers."""

    def __init__(self, source_identifier: str):
        self.source_identifier = source_identifier

    @abc.abstractmethod
    def parse(self) -> ProcessedData:
        """Reads and parses the source into the standard ProcessedData format."""
        pass

    def validate(self, data: ProcessedData) -> bool:
        """Performs basic structural validation on the parsed data."""
        if not data:
            print("[Validation] Data is empty.")
            return False
        # Ensure every record has an 'id' and 'raw_text' for the pipeline
        if not all('id' in record and 'raw_text' in record for record in data):
            print("[Validation] Missing 'id' or 'raw_text' in some records.")
            return False
        return True

    def get_metadata(self) -> Dict[str, Any]:
        """Returns metadata about the source."""
        return {"source_type": self.__class__.__name__, "path_or_url": self.source_identifier}


# --- Concrete Handler Implementations ---

class CSVFileHandler(FileHandler):
    """Handles local CSV files."""

    def parse(self) -> ProcessedData:
        print(f"  [Handler] Reading CSV file at: {self.source_identifier}")
        try:
            # Assumes CSV has columns like 'id', 'raw_text', etc.
            df = pd.read_csv(self.source_identifier)
            # Ensure an 'id' column exists, or create a synthetic one
            if 'id' not in df.columns:
                df['id'] = range(1, len(df) + 1)

            # Convert to standard Python list of dictionaries
            return df.to_dict('records')
        except Exception as e:
            print(f"  [Handler Error] Failed to read CSV: {e}")
            return []


class TrustfallWebHandler(FileHandler):
    """
    Handles data ingestion from a web URL using a mock Trustfall implementation.
    The real implementation would use Trustfall's Query Engine.
    """

    # --- MOCK Trustfall Components ---
    MOCK_TRUSTFALL_SCHEMA = """
        schema Product {
            id: Int!
            title: String!
            price: Float!
            review_count: Int!
            raw_text: String
        }
    """
    MOCK_TRUSTFALL_QUERY = """
        {
            Product @filter(op: "=", value: ["$url"]) {
                id
                title
                price
                review_count
                raw_text
            }
        }
    """

    def _mock_trustfall_execute(self, url: str) -> ProcessedData:
        """Simulates Trustfall querying a web page (e.g., using a Wasm adapter)."""
        print(f"  [Handler] Executing mock Trustfall query on URL: {url}")

        # Mock structured data extracted from the web page based on the query
        if "ecommerce-report" in url:
            return [
                {
                    "id": 101,
                    "title": "Q1 Inventory Summary",
                    "price": 0.0,
                    "review_count": 50,
                    "author_name": "Web Scraper",
                    "in_stock": True,
                    "raw_text": "Q1 saw a massive spike in smart device sales across North America. Inventory velocity is high."
                },
                {
                    "id": 102,
                    "title": "Q2 Key Takeaways",
                    "price": 0.0,
                    "review_count": 12,
                    "author_name": "Web Scraper",
                    "in_stock": False,
                    "raw_text": "Minor delays in EU shipping chain impacted Q2 revenue targets slightly."
                }
            ]
        return []

    def parse(self) -> ProcessedData:
        """Parses the URL using the mock Trustfall executor."""
        return self._mock_trustfall_execute(self.source_identifier)


# --- Handler Factory ---

def is_url(source_identifier: str) -> bool:
    """Checks if the source identifier is a valid URL."""
    try:
        result = urlparse(source_identifier)
        return result.scheme in ['http', 'https']
    except:
        return False


def get_handler(source_identifier: str) -> FileHandler:
    """Factory function to return the correct handler based on the source identifier."""
    if is_url(source_identifier):
        return TrustfallWebHandler(source_identifier)

    # Simple check for file extension can be added here if needed
    # For now, treat non-URLs as file paths (e.g., CSV, Excel)
    elif source_identifier.lower().endswith(('.csv', '.txt')):
        return CSVFileHandler(source_identifier)

    # Default handler for local files (can be expanded to ExcelHandler, JSONHandler, etc.)
    else:
        # For simplicity, if it's a file path, we'll assume it's CSV-like for this demo
        return CSVFileHandler(source_identifier)
