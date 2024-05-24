"""Qdrant target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_qdrant.sinks import (
    QdrantSink,
)


class TargetQdrant(Target):
    """Sample target for Qdrant."""

    name = "target-qdrant"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "endpoint",
            th.StringType,
            description="The endpoint of the Qdrant instance",
            default="localhost",
            required=True
        ),
        th.Property(
            "colllection",
            th.StringType,
            description="Collection to insert data into",
            default="jira_issues",
            required=True
        ),
    ).to_dict()

    default_sink_class = QdrantSink


if __name__ == "__main__":
    TargetQdrant.cli()

