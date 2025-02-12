#!/usr/bin/env python3
"""
retriever.py

Implements a simple FAISS-based retrieval class that,
given a query, returns top matching chunks from the stored index.
"""

import pickle
import faiss
import numpy as np
from openai import OpenAI

class Retriever:
    """
    Loads an existing FAISS index + metadata,
    then on .retrieve(query),
    calls OpenAI embeddings for the query
    and finds the top k similar chunks.
    """
    def __init__(self, openai_api_key: str, index_file="faiss_index.bin", meta_file="metadata.pkl"):
        self.client = OpenAI(api_key=openai_api_key)
        self.index = faiss.read_index(index_file)
        with open(meta_file, "rb") as f:
            self.metadata = pickle.load(f)

    def embed_text(self, text: str):
        response = self.client.embeddings.create(
            model="text-embedding-ada-002",
            input=text
        )
        return response.data[0].embedding

    def retrieve(self, query: str, top_k=3):
        """Return the top k chunks as a list of texts."""
        vec = self.embed_text(query)
        arr = np.array([vec], dtype="float32")
        # IP = inner product. We might want to do a L2 norm if we want cosine similarity
        distances, indices = self.index.search(arr, top_k)

        docs = []
        for i in indices[0]:
            chunk_info = self.metadata[i]
            docs.append(chunk_info["text"])
        return docs