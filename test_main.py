import os
import tempfile
from src.pipeline import flow

# Create a mock CSV file for testing file ingestion
MOCK_CSV_CONTENT = """
id,raw_text,author_name,in_stock
1,The new smartphone has a great camera and long battery life.,Jane Doe,True
2,A quick memo about Q3 inventory status.,John Smith,False
3,Detailed specifications for the upcoming Q1 product launch.,Alice,True
4,Short note about supply chain delay.,Bob,False
"""

def create_mock_file(content: str) -> str:
    """Creates a temporary file with the given content and returns its path."""
    # Use tempfile to ensure cleanup (though the FastAPI app handles this better)
    temp_dir = tempfile.gettempdir()
    temp_filepath = os.path.join(temp_dir, "mock_data.csv")
    with open(temp_filepath, "w") as f:
        f.write(MOCK_CSV_CONTENT)
    return temp_filepath

def test_pipeline():
    """Runs the full data pipeline for both file and URL sources."""
    print("\n==============================================")
    print("STARTING END-TO-END PIPELINE TEST")
    print("==============================================")

    # --- Test Case 1: File Ingestion (Simulates CSV upload) ---
    print("\n--- Running Test Case 1: FILE INGESTION ---")
    file_path = create_mock_file(MOCK_CSV_CONTENT)
    try:
        flow.data_pipeline_flow(source_identifier=file_path)
    except Exception as e:
        print(f"Test Case 1 FAILED with error: {e}")
    finally:
        # Clean up the mock file
        os.remove(file_path)
        print(f"Cleaned up mock file: {file_path}")

    # --- Test Case 2: URL Ingestion (Simulates web scraping/Trustfall) ---
    print("\n--- Running Test Case 2: URL INGESTION (Trustfall Mock) ---")
    url_source = "https://www.example.com/ecommerce-report-2025"
    try:
        flow.data_pipeline_flow(source_identifier=url_source)
    except Exception as e:
        print(f"Test Case 2 FAILED with error: {e}")

    print("\n==============================================")
    print("END OF END-TO-END PIPELINE TEST")
    print("==============================================")

if __name__ == "__main__":
    test_pipeline()
