"""Qdrant target sink class, which handles writing streams."""

from __future__ import annotations

import typing as t
import json

from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import PointStruct

MAX_PARALLEL_API_CALLS = 20

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


    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        self.issues = []


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
        if len(self.issues) > MAX_PARALLEL_API_CALLS:
            self.process_batch(context=dict())
            self.start_batch(context=dict())


    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.points = []
        
        # TODO: first make the API calls on all the summarization and embeddings

        vector = [float(feature) for feature in embedding]

        self.points.append(PointStruct(id=issue_id, vector=vector, payload=record))

        self.qdrant_client.upsert(
            collection_name=self.collection,
            wait=True,
            points=self.points
        )

