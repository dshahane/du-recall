import tomli
import tempfile
import os
import shutil
from typing import Optional, Dict, Any

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from prefect.deployments import run_deployment

# Import the Prefect flow function
from pipeline import flow

# --- Configuration Loading ---

# Read the TOML configuration file
try:
    with open("config.toml", "rb") as f:
        config = tomli.load(f)
except FileNotFoundError:
    raise RuntimeError("config.toml not found. Cannot start application.")

app = FastAPI(title="DU-Recall Config-Driven Data Pipeline Service")


# Define a Pydantic model for URL ingestion requests
class UrlIngestRequest(BaseModel):
    """Schema for pipelines that expect a URL input."""
    url: str
    metadata: Dict[str, Any] = {}


# --- Prefect Deployment Helper ---

def trigger_prefect_flow(source_identifier: str, title: str):
    """Triggers the Prefect flow with the given source identifier."""
    print(f"Triggering Prefect Flow: '{title}' for source: {source_identifier}")
    try:
        # In a real deployed Prefect system, you would typically run a deployment
        # For this local demo, we call the flow function directly, which respects the DaskRunner setup.
        flow.data_pipeline_flow(source_identifier=source_identifier)
        return {"status": "Flow triggered successfully", "source": source_identifier}
    except Exception as e:
        # In production, Prefect handles most exceptions, but we catch deployment errors here
        raise HTTPException(status_code=500, detail=f"Failed to trigger flow: {str(e)}")


# --- Dynamic Endpoint Creation ---

for pipeline_config in config.get("pipeline", []):
    endpoint = pipeline_config["endpoint"]
    title = pipeline_config["title"]
    source_type = pipeline_config["source_type"]

    if source_type == "file":
        # Endpoint for FILE UPLOAD pipelines

        # Use a closure to capture variables for the function definition
        def create_file_endpoint(title, endpoint):

            async def file_endpoint(file: UploadFile = File(...),
                                    metadata: Optional[str] = Form(None)):
                """Ingest data via file upload and run the pipeline."""
                temp_dir = tempfile.mkdtemp()
                temp_file_path = os.path.join(temp_dir, file.filename)

                try:
                    # Save the uploaded file to a temporary location
                    with open(temp_file_path, "wb") as buffer:
                        shutil.copyfileobj(file.file, buffer)

                    # Trigger the Prefect flow with the temporary file path
                    result = trigger_prefect_flow(source_identifier=temp_file_path, title=title)
                    return result

                finally:
                    # Clean up the temporary directory and file
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)

            file_endpoint.__name__ = f"{endpoint}_file_handler"
            file_endpoint.__doc__ = f"**{title}**: Accepts a file upload to start the processing pipeline."
            app.post(f"/{endpoint}", name=endpoint, summary=title)(file_endpoint)


        create_file_endpoint(title, endpoint)

    elif source_type == "url":
        # Endpoint for URL INGESTION pipelines

        # Use a closure to capture variables for the function definition
        def create_url_endpoint(title, endpoint):

            async def url_endpoint(request: UrlIngestRequest):
                """Ingest data via URL and run the pipeline."""
                url = request.url
                # Trigger the Prefect flow with the URL as the source identifier
                result = trigger_prefect_flow(source_identifier=url, title=title)
                return result

            url_endpoint.__name__ = f"{endpoint}_url_handler"
            url_endpoint.__doc__ = f"**{title}**: Accepts a URL to start the processing pipeline."
            app.post(f"/{endpoint}", name=endpoint, summary=title)(url_endpoint)


        create_url_endpoint(title, endpoint)

    else:
        print(f"Warning: Unknown source_type '{source_type}' for endpoint '{endpoint}'. Skipping.")
