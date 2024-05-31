"""Qdrant target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_qdrant.sinks import (
    QdrantSink,
)

import typing as t

if t.TYPE_CHECKING:
    from pathlib import PurePath

class TargetQdrant(Target):
    """Sample target for Qdrant."""
    
    name = "target-qdrant"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "endpoint",
            th.StringType,
            description="The endpoint of the Qdrant instance",
            required=True
        ),
        th.Property(
            "collection",
            th.StringType,
            description="Collection to insert data into",
            required=True
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="Port Qdrant instance is listening to",
            required=True
        ),
    ).to_dict()

    default_sink_class = QdrantSink


    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        setup_mapper: bool = True,
    ) -> None:
        super().__init__(config=config, parse_env_config=parse_env_config, validate_config=validate_config, setup_mapper=setup_mapper)
        # overwrite MAX_RECORD_AGE to 365 days (impossible to have a tap opened in practice for this long)
        self._MAX_RECORD_AGE_IN_MINUTES = 365 * 24 * 60

if __name__ == "__main__":
    TargetQdrant.cli()

