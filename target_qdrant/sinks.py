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
                vectors_config=VectorParams(size=4096, distance=Distance.COSINE),
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
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"
        self.logger.info(f"Start Batch\n\n")

        self.points = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)
        self.logger.info(f"Process Record\n\n")

        vector = record['embedding']
        issue_id = record['issue_id']

        del record['embedding']
        del record['issue_id']

        # TODO: when date fields (e.g. created, updated) will be passed, serialization might be required (check: https://github.com/timeplus-io/target-timeplus/blob/main/target_timeplus/sinks.py)
        payload = json.dumps(record)

        self.points.append(PointStruct(id=issue_id, vector=vector, payload=payload))

        #force flush the batch to avoid sending too much data (over 10MB) to Qdrant
        if len(self.points) > 100:
            self.process_batch(context=dict())
            self.start_batch(context=dict())


    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy
        self.logger.info(f"Process Batch\n\n")
        
        self.qdrant_client.upsert(
            collection_name=self.collection,
            wait=True,
            points=self.points
        )
