import json
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from plugins.version_endpoint import (
    get_airflow_version,
    get_dockerflow_version,
    get_project_root,
    parse_airflow_version,
)


def test_get_project_root():
    # CircleCI renames the project directory to `project`
    assert get_project_root().name in ("telemetry-airflow", "project")
    assert get_project_root().is_dir()


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (
            (
                "# example comment on first line\n"
                "FROM apache/airflow:slim-2.8.2-python3.11\n"
                "# Rest of Dockerfile"
            ),
            "slim-2.8.2-python3.11",
        ),
        ("FROM apache/airflow:2.9.1", "2.9.1"),
        ("FROM apache/airflow:slim-2.7.3", "slim-2.7.3"),
    ],
)
def test_parse_airflow_version(test_input, expected):
    assert parse_airflow_version(test_input) == expected


def test_get_airflow_version_exists():
    mock_project_root = patch(
        "plugins.version_endpoint.get_project_root", return_value=Path("/mock/path")
    )
    mock_parse_airflow_version = patch(
        "plugins.version_endpoint.parse_airflow_version", return_value="2.8.2"
    )
    mock_open_file = patch("builtins.open", mock_open(read_data="Mock Data!"))
    mock_is_file = patch("pathlib.Path.is_file", return_value=True)
    mock_exists = patch("pathlib.Path.exists", return_value=True)

    with (
        mock_project_root,
        mock_parse_airflow_version,
        mock_open_file,
        mock_is_file,
        mock_exists,
    ):
        result = get_airflow_version()
        assert result == {"version": "2.8.2"}


def test_get_airflow_version_not_exists():
    mock_project_root = patch(
        "plugins.version_endpoint.get_project_root", return_value=Path("/mock/path")
    )

    with mock_project_root:
        result = get_airflow_version()
        assert result == {"version": None}


def test_get_dockerflow_version_exists():
    mock_project_root = patch(
        "plugins.version_endpoint.get_project_root", return_value=Path("/mock/path")
    )
    mock_open_file = patch(
        "builtins.open",
        mock_open(
            read_data=json.dumps(
                {
                    "build": "12345",
                    "commit": "abcdef",
                    "source": "https://github.com/mozilla/telemetry-airflow",
                }
            )
        ),
    )
    mock_is_file = patch("pathlib.Path.is_file", return_value=True)
    mock_exists = patch("pathlib.Path.exists", return_value=True)

    with mock_project_root, mock_open_file, mock_is_file, mock_exists:
        result = get_dockerflow_version()
        assert result == {
            "build": "12345",
            "commit": "abcdef",
            "source": "https://github.com/mozilla/telemetry-airflow",
        }


def test_get_dockerflow_version_not_exists():
    mock_project_root = patch(
        "plugins.version_endpoint.get_project_root", return_value=Path("/mock/path")
    )

    with mock_project_root:
        result = get_dockerflow_version()
        assert result == {"build": None, "commit": None, "source": None}
