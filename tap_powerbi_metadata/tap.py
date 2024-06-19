"""PowerBIMetadata tap class."""

from pathlib import Path
from typing import List
from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
    ObjectType
)

from tap_powerbi_metadata.streams import (
    TapPowerBIMetadataStream,
    ActivityEventsStream,
    AppsStream,
    GroupsStream,
    ReportsStream,
    RefreshablesStream,
    DatasetStream,
    # Commenting out DataSources stream because it is not ready to be extracte to Snowflake
    # DataSourceStream
)

PLUGIN_NAME = "tap-powerbi-metadata"

STREAM_TYPES = [
    ActivityEventsStream,
    AppsStream,
    GroupsStream,
    ReportsStream,
    RefreshablesStream,
    DatasetStream,
    # Commenting out DataSources stream because it is not ready to be extracte to Snowflake
    # DataSourceStream,

]


class TapPowerBIMetadata(Tap):
    """PowerBIMetadata tap class."""

    name = "tap-powerbi-metadata"
    config_jsonschema = PropertiesList(
        Property("tenant_id", StringType, required=True),
        Property("client_id", StringType, required=True),
        Property("username", StringType, required=True),
        Property("password", StringType, required=True),
        Property("start_date", DateTimeType),
        Property("stream_config", ObjectType(
            Property(
                "parameters",
                StringType
            )
        )),
        Property("stream_config_string", StringType)
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = TapPowerBIMetadata.cli
