#!/usr/bin/env python3
"""
document_ingestor.py

Reads large text files (e.g., database/models.py, scratch_files/tree.txt),
splits them into chunks, creates embeddings, and stores them in a FAISS index.
"""

import os
import pickle
import faiss
import numpy as np

from openai import OpenAI

class DocumentIngestor:
    """
    1) Reads file content
    2) Splits into chunks
    3) Calls OpenAI embeddings
    4) Stores in a FAISS index
    """
    def __init__(self, openai_api_key: str, chunk_size=500):
        self.client = OpenAI(api_key=openai_api_key)
        self.chunk_size = chunk_size
        self.index = None
        self.metadata = []  # list of {"source_file":..., "chunk_idx":..., "text":...}

    def embed_text(self, text: str):
        """Call OpenAI's embeddings endpoint using, e.g., 'text-embedding-ada-002'."""
        response = self.client.embeddings.create(
            model="text-embedding-ada-002",
            input=text
        )
        # The library returns: response.data[0].embedding
        return response.data[0].embedding

    def ingest_file(self, file_path: str):
        """Split file content into chunks, embed each, store in memory."""
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        chunks = []
        # naive chunking approach
        for i in range(0, len(content), self.chunk_size):
            chunk_text = content[i:i+self.chunk_size]
            chunks.append(chunk_text)

        if self.index is None:
            # initialize a new FAISS index
            # first get a sample embedding to figure out dimension
            sample_vec = self.embed_text("hello")
            dimension = len(sample_vec)
            # We'll use an IndexFlatIP for simplicity
            self.index = faiss.IndexFlatIP(dimension)

        vectors = []
        for idx, chunk_text in enumerate(chunks):
            vec = self.embed_text(chunk_text)
            vectors.append(vec)
            self.metadata.append({
                "source_file": file_path,
                "chunk_idx": idx,
                "text": chunk_text
            })

        arr = np.array(vectors, dtype="float32")
        self.index.add(arr)

    def save_index(self, index_file="faiss_index.bin", meta_file="metadata.pkl"):
        """Persist FAISS index and metadata to disk."""
        if self.index is not None:
            faiss.write_index(self.index, index_file)
        with open(meta_file, "wb") as f:
            pickle.dump(self.metadata, f)

    def load_index(self, index_file="faiss_index.bin", meta_file="metadata.pkl"):
        """Load FAISS index and metadata from disk."""
        self.index = faiss.read_index(index_file)
        with open(meta_file, "rb") as f:
            self.metadata = pickle.load(f)