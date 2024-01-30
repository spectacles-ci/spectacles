from unittest.mock import MagicMock, patch

from spectacles import tracking


def test_anonymise_url() -> None:
    hashed_url = tracking.anonymise("https://organisation.looker.com")
    assert hashed_url == "67d1b18410d23b5765caa3320b703154"


def test_anonymise_project() -> None:
    hashed_url = tracking.anonymise("test_project")
    assert hashed_url == "6e72a69d5c5cca8f0400338441c022e4"


@patch("analytics.track")
def test_track_invocation_start_sql(mock_track: MagicMock) -> None:
    invocation_id = tracking.track_invocation_start(
        "https://organisation.looker.com", "sql", "123456", "test_project"
    )
    mock_track.assert_called_once_with(
        user_id="67d1b18410d23b5765caa3320b703154",
        event="invocation",
        properties={
            "label": "start",
            "command": "sql",
            "project": "6e72a69d5c5cca8f0400338441c022e4",
            "invocation_id": "123456",
        },
    )
    assert invocation_id == "123456"


@patch("analytics.track")
def test_track_invocation_start_assert(mock_track: MagicMock) -> None:
    invocation_id = tracking.track_invocation_start(
        "https://organisation.looker.com", "assert", "123456", "test_project"
    )
    mock_track.assert_called_once_with(
        user_id="67d1b18410d23b5765caa3320b703154",
        event="invocation",
        properties={
            "label": "start",
            "command": "assert",
            "project": "6e72a69d5c5cca8f0400338441c022e4",
            "invocation_id": "123456",
        },
    )
    assert invocation_id == "123456"


@patch("analytics.track")
def test_track_invocation_start_connect(mock_track: MagicMock) -> None:
    invocation_id = tracking.track_invocation_start(
        "https://organisation.looker.com", "assert", "123456"
    )
    mock_track.assert_called_once_with(
        user_id="67d1b18410d23b5765caa3320b703154",
        event="invocation",
        properties={
            "label": "start",
            "command": "assert",
            "project": None,
            "invocation_id": "123456",
        },
    )
    assert invocation_id == "123456"


@patch("analytics.track")
def test_track_invocation_end_sql(mock_track: MagicMock) -> None:
    tracking.track_invocation_end(
        "https://organisation.looker.com", "sql", "123456", "test_project"
    )
    mock_track.assert_called_once_with(
        user_id="67d1b18410d23b5765caa3320b703154",
        event="invocation",
        properties={
            "label": "end",
            "command": "sql",
            "project": "6e72a69d5c5cca8f0400338441c022e4",
            "invocation_id": "123456",
        },
    )


@patch("analytics.track")
def test_track_invocation_end_assert(mock_track: MagicMock) -> None:
    tracking.track_invocation_end(
        "https://organisation.looker.com", "assert", "123456", "test_project"
    )
    mock_track.assert_called_once_with(
        user_id="67d1b18410d23b5765caa3320b703154",
        event="invocation",
        properties={
            "label": "end",
            "command": "assert",
            "project": "6e72a69d5c5cca8f0400338441c022e4",
            "invocation_id": "123456",
        },
    )


@patch("analytics.track")
def test_track_invocation_end_connect(mock_track: MagicMock) -> None:
    tracking.track_invocation_end("https://organisation.looker.com", "assert", "123456")
    mock_track.assert_called_once_with(
        user_id="67d1b18410d23b5765caa3320b703154",
        event="invocation",
        properties={
            "label": "end",
            "command": "assert",
            "project": None,
            "invocation_id": "123456",
        },
    )
