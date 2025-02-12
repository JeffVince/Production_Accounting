#!/usr/bin/env python3
"""
vector_ingestion.py

This script ingests key files (e.g., database/models.py and scratch_files/tree.txt)
into a FAISS vector store using the DocumentIngestor class. The resulting index and
metadata are saved to disk for later retrieval by the main agent.
"""

import os
from document_ingestor import DocumentIngestor
from dotenv import load_dotenv
load_dotenv("../.env")
def main():
    load_dotenv()  # Load environment variables (e.g., OPENAI_API_KEY)
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("Error: OPENAI_API_KEY is not set in the environment.")
        return

    # Initialize the ingestor with a desired chunk size.
    ingestor = DocumentIngestor(openai_api_key=openai_api_key, chunk_size=500)

    # List the files to ingest.
    files_to_ingest = [
        "../database/models.py",
        "../scratch files/tree.txt"
    ]

    for file_path in files_to_ingest:
        print(f"Ingesting {file_path}...")
        try:
            ingestor.ingest_file(file_path)
            print(f"Successfully ingested {file_path}.")
        except Exception as e:
            print(f"Error ingesting {file_path}: {e}")

    # Save the FAISS index and metadata for later retrieval.
    ingestor.save_index(index_file="faiss_index.bin", meta_file="metadata.pkl")
    print("Vector store saved: 'faiss_index.bin' and 'metadata.pkl'.")

if __name__ == "__main__":
    main()