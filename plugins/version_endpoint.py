import json
import re
from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, jsonify

version_endpoint_bp = Blueprint("version_endpoint", __name__)

# from https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
SEM_VER_REGEX = (
    r"(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\."
    r"(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>"
    r"(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


def get_project_root() -> Path:
    """Reliably give the project root as a Path object."""
    return Path(__file__).parent.parent


def get_airflow_version() -> dict[str, str | None]:
    """Parse Airflow version from Dockerfile and return it as a dict."""
    project_root = get_project_root()
    version_pattern = rf"^FROM apache\/airflow:((slim-){SEM_VER_REGEX})$"
    version_regex = re.compile(pattern=version_pattern, flags=re.MULTILINE | re.DOTALL)
    dockerfile = project_root / "Dockerfile"
    if dockerfile.is_file() and dockerfile.exists():
        with open(dockerfile) as file:
            content = file.read()
        version = version_regex.search(content).group(1)
    else:
        version = None
    return {"version": version}


def get_dockerflow_version() -> dict[str, str | None]:
    """
    Parse Dockerflow style version.json file and return it as a dict.

    version.json is baked in the Docker image at build time in CI.

    """
    project_root = get_project_root()
    version_file = project_root / "version.json"
    if version_file.is_file() and version_file.exists():
        with open(project_root / "version.json") as file:
            version = json.load(file)
    else:
        version = {"build": None, "commit": None, "source": None}
    return version


@version_endpoint_bp.route("/__version__", methods=["GET"])
def version_endpoint():
    airflow_version = get_airflow_version()
    dockerflow_version = get_dockerflow_version()
    return jsonify(dockerflow_version | airflow_version), 200


class CustomPlugin(AirflowPlugin):
    name = "version_endpoint"
    flask_blueprints = (version_endpoint_bp,)
