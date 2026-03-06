"""Tally tap class."""

from __future__ import annotations

from typing import TYPE_CHECKING, override

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_tally import streams

if TYPE_CHECKING:
    from tap_tally.client import TallyStream


class TapTally(Tap):
    """Singer tap for Tally."""

    name = "tap-tally"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="API Key",
            description="Your Tally API key",
        ),
        th.Property(
            "organization_ids",
            th.ArrayType(th.StringType(nullable=False)),
            required=True,
            title="Organization IDs",
            description="Your Tally organization IDs",
            default=[],
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[TallyStream]:
        return [
            streams.UsersStream(self),
            streams.InvitesStream(self),
            streams.FormsStream(self),
            streams.QuestionsStream(self),
            streams.SubmissionsStream(self),
            streams.WorkspacesStream(self),
            # TODO(edgarrmondragon): Enable webhooks streams  # noqa: FIX002
            # https://github.com/reservoir-data/tap-tally/issues/1
            # streams.WebhooksStream(self),  # noqa: ERA001
        ]


if __name__ == "__main__":
    TapTally.cli()
