"""REST client handling, including TallyStream base class."""

from __future__ import annotations

from typing import override

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream

from tap_tally import schemas

SCHEMAS_DIR = SchemaDirectory(schemas)


class TallyStream(RESTStream):
    """Tally stream class."""

    records_jsonpath = "$[*]"
    schema = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def url_base(self) -> str:
        return "https://api.tally.so"

    @override
    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        return BearerTokenAuthenticator(token=self.config["api_key"])
