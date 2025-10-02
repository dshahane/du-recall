import time
from typing import Dict, Any, List, Tuple
import pandas as pd
from prefect import flow, task
import numpy as np

# Import external modules
from handlers.file_handlers import get_handler, ProcessedData


# --- MOCK Haystack Components for Demo ---
class MockHaystackDocument:
    """Mock for Haystack Document structure, used for passing data into the pipeline."""

    def __init__(self, content: str, meta: Dict[str, Any]):
        self.content = content
        self.meta = meta


class MockHaystackClassifier:
    """Mock for a Haystack node (e.g., a custom classification node)."""

    def run(self, documents: List[MockHaystackDocument], **kwargs) -> Tuple[Dict[str, Any], str]:
        results = []
        for doc in documents:
            # Classification Logic Mock:
            content_len = len(doc.content)
            classification_label = "ProductReview"

            if content_len > 100:
                classification_label = "LongReport"
            elif content_len > 40:
                classification_label = "DetailedSpec"

            # The classification result is added to the metadata
            new_meta = {**doc.meta, "llm_classification": classification_label}

            results.append({
                "content": doc.content,
                "meta": new_meta
            })

        # Haystack nodes return a dictionary structure and a component name
        return {"documents": results}, "MockClassifier"


class MockHaystackPipeline:
    """Mock for Haystack Pipeline to simulate execution."""

    def __init__(self, nodes: List[MockHaystackClassifier]):
        self.nodes = nodes  # For demonstration

    def run(self, documents: List[MockHaystackDocument], **kwargs) -> Dict[str, Any]:
        """Simulates running the Haystack pipeline."""
        print("--- [Haystack] Mock Pipeline running for classification...")
        time.sleep(0.5)  # Simulate pipeline latency

        # In a real scenario, this would chain multiple nodes. Here we run the mock classifier directly.
        output, _ = self.nodes[0].run(documents=documents)
        return output


# --- END MOCK ---


# --- Prefect Tasks ---

@task(retries=3, retry_delay_seconds=[5, 10, 30])
def ingest_and_validate(source_identifier: str) -> ProcessedData:
    """
    Ingests data using the appropriate handler and performs initial validation.
    """
    print(f"\n[TASK] Starting ingestion for: {source_identifier}")
    handler = get_handler(source_identifier)
    data = handler.parse()

    if not data or not handler.validate(data):
        raise ValueError("Ingestion failed: Data is empty or invalid after parsing.")

    print(f"[TASK] Ingestion complete. Found {len(data)} records.")
    print(f"       Metadata: {handler.get_metadata()}")
    return data


@task(log_prints=True)
def run_llm_classification(raw_data: ProcessedData) -> ProcessedData:
    """
    Runs a structured Haystack pipeline (e.g., classification or QA) on the raw text data.
    """

    # 1. Prepare data for Haystack (conversion to Document format)
    haystack_docs = []
    for record in raw_data:
        # Assuming 'raw_text' contains the content for the LLM
        content = record.get("raw_text", "")
        # Use existing record data as metadata
        meta = {k: v for k, v in record.items() if k not in ['raw_text']}
        haystack_docs.append(MockHaystackDocument(content=content, meta=meta))

    if not haystack_docs:
        print("[LLM] No documents to process. Skipping classification.")
        return raw_data

    # 2. Initialize the Haystack Pipeline
    classifier_node = MockHaystackClassifier()
    classification_pipeline = MockHaystackPipeline(nodes=[classifier_node])

    # 3. Execute the Haystack Pipeline
    pipeline_results = classification_pipeline.run(documents=haystack_docs)

    # 4. Process results and merge back into ProcessedData format
    classified_data: ProcessedData = []
    for doc_result in pipeline_results['documents']:
        # The classification result is stored in the metadata of the Haystack Document
        merged_record = {
            "raw_text": doc_result["content"],  # The original text
            **doc_result["meta"]  # Merged metadata, including the new 'llm_classification' field
        }
        classified_data.append(merged_record)

    print(f"[LLM] Classification complete. Updated {len(classified_data)} records with LLM insights.")
    return classified_data


@task(log_prints=True)
def run_dask_analysis(data: ProcessedData) -> ProcessedData:
    """
    Performs complex, parallelizable analysis using Dask (mocked here).

    This analysis now calculates Inventory Velocity and a Priority Score,
    leveraging the 'llm_classification' field.
    """
    print(f"[DASK] Starting complex analysis on {len(data)} records...")

    # Convert list of dicts to Dask DataFrame via Pandas for parallel operations
    df = pd.DataFrame(data)

    # --- E-commerce Analysis 1: Calculate Inventory Velocity ---
    # Mock calculation: velocity is high if in stock and long report (high interest/demand)
    df['inventory_velocity'] = df.apply(
        lambda row:
        (np.random.rand() * 4.0) + 1.0
        if row.get('in_stock', False) and row.get('llm_classification') == 'DetailedSpec'
        else (np.random.rand() * 1.5),
        axis=1
    )

    # --- E-commerce Analysis 2: Determine Operational Priority Score (0-5) ---
    def calculate_priority(row):
        score = 0
        classification = row.get('llm_classification', '')
        in_stock = row.get('in_stock', False)

        # High priority for detailed specs (potential launch/critical data)
        if classification == 'DetailedSpec':
            score += 4
        # Medium priority for reviews (customer sentiment)
        elif classification == 'ProductReview':
            score += 2

        # Adjust based on inventory status
        if in_stock and classification == 'ProductReview':  # In stock product with review -> check needed
            score += 1
        elif not in_stock and classification == 'DetailedSpec':  # Critical data, but out of stock -> high attention
            score += 1

        # Cap score at 5
        return min(score / 5.0 * 5.0, 5.0)

    df['priority_score'] = df.apply(calculate_priority, axis=1)

    time.sleep(0.3)  # Simulate compute time

    # Convert back to list of dicts
    final_data = df.to_dict('records')

    print(f"[DASK] Analysis complete. Example record keys: {list(final_data[0].keys())}")
    return final_data


@task(log_prints=True)
def store_results_to_rocksdb(final_data: ProcessedData) -> str:
    """
    Converts final structured data to RDF triples (Schema.org) and stores them in RocksDB.
    """
    print("[STORAGE] Converting to RDF and storing in RocksDB...")

    # 1. Convert to RDF (using rdflib in a real scenario)
    triples = convert_to_rdf(final_data)

    # 2. Store using the RocksDB mock
    store_result = store_rdf_triples(triples)

    print(f"[STORAGE] Storage complete. Status: {store_result}")
    return store_result


# --- Prefect Flow Definition ---

class DaskTaskRunner:
    pass


@flow(name="data-pipeline-flow", task_runner=DaskTaskRunner())
def data_pipeline_flow(source_identifier: str):
    """
    The main orchestration flow for data ingestion, processing, and storage.
    Uses Dask for parallelizable tasks.
    """
    print(f"FLOW START: Running pipeline for source: {source_identifier}")

    # 1. Ingestion and Validation
    raw_data = ingest_and_validate(source_identifier=source_identifier)

    # 2. LLM Pipeline Execution (Haystack)
    classified_data = run_llm_classification(raw_data=raw_data)

    # 3. Distributed Analysis (Dask)
    analyzed_data = run_dask_analysis(data=classified_data)

    # 4. Storage
    store_results_to_rocksdb(final_data=analyzed_data)

    print(f"FLOW COMPLETE: Successfully processed and stored data from {source_identifier}")
