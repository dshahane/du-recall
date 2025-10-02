## ⚙️ Configuration-Driven Dcoument Understanding Pipeline Service

This project delivers a **robust, scalable, and extensible data ingestion and processing service** built on modern Python libraries. It uses a **configuration-first approach**, allowing new data pipelines and API endpoints to be defined simply by updating a `config.toml` file.

---

### ✨ Architecture & Core Technologies

The service is composed of several specialized components working together:

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Orchestration** | **Prefect** | Manages the workflow, defining and executing the data pipeline tasks. |
| **Compute** | **Dask** | Provides parallel and distributed computing for handling complex data analysis and transformations at scale. |
| **LLM Tasks** | **Haystack** | Handles structured tasks involving Large Language Models, such as data classification and semantic analysis. |
| **Ingestion** | **Extensible Handlers** | Supports various data sources (e.g., CSV files, web pages via **Trustfall**) with handlers based on file type or URL structure. |
| **Storage** | **Schema.org/RDF & RocksDB** | Data is converted to **Schema.org RDF triples** for semantic representation and stored (mocked) in a **RocksDB** key-value store. |
| **API Layer** | **FastAPI** | Dynamically generates RESTful API endpoints based on the pipeline definitions in `config.toml`. |

---

### 🚀 Setup and Run

This guide assumes you have **Python 3.9+** installed. We highly recommend using `uv` for lightning-fast dependency management.

### 1. Install `uv`

```bash
pip install uv
```

###  2. Initialize and Install Dependencies

Set up the project structure and install all necessary dependencies using the `pyproject.toml` file.

#### Clone the repository
```git clone git@github.com:dshahane/du-recall.git```


#### Sync dependencies defined in pyproject.toml
```uv sync```

### 3. Run the FastAPI Service

Start the API server to expose the dynamically generated pipeline endpoints. Ensure you are in the project root directory

```
uvicorn app:app --reload
```

The service will be available at ```http://127.0.0.1:8000```

🌐 API Access and Testing


#### Dynamically Generated Endpoints

Open your browser to the Swagger UI at ```http://127.0.0.1:8000/docs```. You will see the API endpoints generated directly from your configuration, for example:
```
POST /csv_file_ingest
POST /web_report_ingest
POST /excel_monthly_data
```
Your users can trigger complex, configuration-driven data pipelines using these simple, descriptive API calls!

#### Direct Pipeline Testing

To test the core pipeline logic without the API layer, run the dedicated test script:

```Bash
python test_run.py
```
