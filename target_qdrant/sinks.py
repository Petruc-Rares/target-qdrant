"""Qdrant target sink class, which handles writing streams."""

from __future__ import annotations

import typing as t
import openai
import concurrent
import threading

from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import PointStruct

openai.api_key = 'XYZ'
openai.base_url = "https://devai.4psa.me/llm/v1/"

MAX_PARALLEL_API_CALLS = 20
EMBEDDING_MODEL ="Salesforce/SFR-Embedding-Mistral"
SUMMARY_MODEL = "Llama3 70B" 

class QdrantSink(BatchSink):
    """Qdrant target sink class."""

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None,
    ) -> None:

        super().__init__(target, stream_name, schema, key_properties)
        
        self.batch_idx = 0
        self.collection = self.config["collection"]

        endpoint = self.config["endpoint"]
        port = self.config["port"]
        self.qdrant_client = QdrantClient(url=endpoint, port=port)

        try:
            self.qdrant_client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(size=4096, distance=Distance.EUCLID),
            )

            self.logger.info(f"Created collection {self.collection} successfully!")
        except UnexpectedResponse as e:
            if e.status_code == 409:
                self.logger.info(f"Collection {self.collection} already exists. Did NOT overwrite it!")
                # handle the error
            else:
                raise  # re-raise the error if it's not a 409


        # threads related definitions
        self.can_start_summarization = threading.Semaphore()
        self.can_start_embedding = threading.Semaphore()

        self.summarization_over = threading.Semaphore()
        self.embedding_stage_copy_done = threading.Semaphore()

        self.summarizer_thread = threading.Thread(target=self.summarize)
        self.embedder_thread = threading.Thread(target=self.embed)
        self.threads = [self.summarizer_thread, self.embedder_thread]
        
        for thread in self.threads():
            thread.start()

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """

        if self.batch_idx > 0:
            self.summarization_over.acquire()

        self.issues = []
        self.batch_idx += 1


    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """

        issue_info = {
            'embedding_input': record['embedding_input'],
            'summarizer_input': record['summarizer_input'],
            'issue_id': record['issue_id']
        }

        del record['embedding_input']
        del record['summarizer_input']
        del record['issue_id']

        issue_info['record'] = record

        self.issues.append(issue_info)

        #force flush the batch when number of parallel API calls reached:
        if len(self.issues) >= MAX_PARALLEL_API_CALLS:
            self.process_batch(context=dict())
            self.start_batch(context=dict())


    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.can_start_summarization.release()

    def summarize(self):
        while True:
            self.can_start_summarization.acquire()

            summarizer_inputs = [{"role": "user", "content": issue_info['summarizer_input']} for issue_info in self.issues]

            # parallel API calls for summarization
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_API_CALLS) as executor:
                futures = []
                for summarizer_input in summarizer_inputs:
                    futures.append(executor.submit(openai.chat.completions.create, 
                                                model=SUMMARY_MODEL, 
                                                messages=[summarizer_input]))

                results = [future.result() for future in futures]
                issues_summaries = [result.choices[0].message.content for result in results]

                # TODO: add issues_summaries to self.issues

            self.can_start_embedding.release()

            self.embedding_stage_copy_done.acquire()

            self.summarization_over.release()

    def embed(self):
        while True:
            self.can_start_embedding.acquire()

            # TODO: copy self.issues in smth else (careful to copy also the content of the dictionaries)

            self.embedding_stage_copy_done.release()

            embedding_inputs = [issue_info['embedding_input'] for issue_info in self.issues]

            # API calls for embedding
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_API_CALLS) as executor:
                futures = []
                for embedding_input in embedding_inputs:
                    futures.append(executor.submit(openai.embeddings.create, 
                                                model=EMBEDDING_MODEL, 
                                                input=[embedding_input]))

                results = [future.result() for future in futures]
                issues_embeddings = [result.data[0].embedding for result in results]

            self.points = []

            for idx in range(len(self.issues)):
                issue_id = self.issues[idx]['issue_id']
                record = self.issues[idx]['record']

                summary = issues_summaries[idx]
                embedding = issues_embeddings[idx]

                vector = [float(feature) for feature in embedding]
                record['summary'] = summary

                self.points.append(PointStruct(id=issue_id, vector=vector, payload=record))

            self.qdrant_client.upsert(
                collection_name=self.collection,
                wait=True,
                points=self.points
            )