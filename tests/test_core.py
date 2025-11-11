"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_tally.tap import TapTally

# Run standard built-in tap tests from the SDK:
TestTapTally = get_tap_test_class(
    tap_class=TapTally,
    suite_config=SuiteConfig(
        ignore_no_records_for_streams=[
            "invites",
            "forms",
            "questions",
            "submissions",
            "workspaces",
        ],
    ),
)
