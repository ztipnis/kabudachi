"""The wheel //runtime:kabudachi_wheel builds, checked as a user gets it: its
metadata says what pyproject.toml says, and the version is the one the package
and the Bazel module name."""

import ast
import os
import re
import tomllib
import zipfile
from email.parser import HeaderParser
from pathlib import Path

from packaging.requirements import Requirement

WHEEL = Path(os.environ["TEST_SRCDIR"], os.environ["WHEEL_FILE"])
PYPROJECT = Path(os.environ["TEST_SRCDIR"], os.environ["PYPROJECT_FILE"])
MODULE = Path(os.environ["TEST_SRCDIR"], os.environ["MODULE_FILE"])


def project() -> dict:
    return tomllib.loads(PYPROJECT.read_text())["project"]


def module_version() -> str:
    """The version in MODULE.bazel's `module(...)` call."""
    call = re.search(r"\bmodule\((.*?)\)", MODULE.read_text(), re.DOTALL)
    return re.search(r'\bversion\s*=\s*"([^"]+)"', call[1])[1]


def package_version(source: str) -> str:
    """The string assigned to `__version__` in a module's source."""
    for statement in ast.parse(source).body:
        if isinstance(statement, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in statement.targets
        ):
            return ast.literal_eval(statement.value)
    raise AssertionError("no __version__ assignment")


def test_the_wheel_metadata_is_what_pyproject_toml_declares():
    declared = project()
    with zipfile.ZipFile(WHEEL) as wheel:
        names = wheel.namelist()
        info = f"kabudachi-{declared['version']}.dist-info/METADATA"
        fields = HeaderParser().parsestr(wheel.read(info).decode())
        init = wheel.read("kabudachi/__init__.py").decode()
    extras = declared.get("optional-dependencies", {})
    author = declared["authors"][0]

    # One version everywhere: pyproject.toml (uv_build), the wheel (Bazel's
    # VERSION), the package's __version__, and the Bazel module.
    assert (fields["Version"], package_version(init), module_version()) == (declared["version"],) * 3

    assert (fields["Name"], fields["Summary"], fields["Requires-Python"]) == (
        declared["name"], declared["description"], declared["requires-python"],
    )
    assert (fields["Author"], fields["Author-email"]) == (author["name"], author["email"])
    assert sorted(fields.get_all("Provides-Extra", [])) == sorted(extras)
    # Compared as parsed requirements, so spacing and quoting do not matter.
    wanted = [
        *declared["dependencies"],
        *(f'{requirement}; extra == "{extra}"' for extra, requirements in extras.items()
          for requirement in requirements),
    ]
    assert sorted(str(Requirement(line)) for line in fields.get_all("Requires-Dist", [])) == sorted(
        str(Requirement(line)) for line in wanted
    )
    # Type checkers read the package's own annotations only with this marker.
    assert "kabudachi/py.typed" in names
