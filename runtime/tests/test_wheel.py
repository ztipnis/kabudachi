"""The wheel //runtime:kabudachi_wheel builds, checked as a user gets it: its
metadata says what pyproject.toml says, the version is the one the package and
the Bazel module name, it installs with pip into a fresh virtual environment,
and from there it runs a flow on a worker of its own and across a networked
shard of two. On Linux its platform tag is the one its extension's symbols
allow."""

import ast
import json
import os
import re
import socket
import subprocess
import sys
import threading
import tomllib
import venv
import zipfile
from email.parser import HeaderParser
from importlib import metadata
from pathlib import Path

import pytest
from packaging.requirements import Requirement

WHEEL = Path(os.environ["TEST_SRCDIR"], os.environ["WHEEL_FILE"])
PYPROJECT = Path(os.environ["TEST_SRCDIR"], os.environ["PYPROJECT_FILE"])
MODULE = Path(os.environ["TEST_SRCDIR"], os.environ["MODULE_FILE"])
PROGRAMS = [Path(__file__).with_name(name) for name in ("wheel_smoke.py", "wheel_smoke_tasks.py")]
# Distributions the installed package imports. The test has no network, so
# they come from the test's own Bazel-installed copies, each in a directory
# of its own.
DEPENDENCIES = ("asgiref", "wrapt", "protobuf")
# Short enough for a test, in the ratios a deployment keeps: three heartbeats
# to a suspicion, and a reconnect timeout past it. A worker leaving waits two
# heartbeat timeouts at most for its voters, and a sole worker that long for
# another.
SETTINGS = {
    "KABUDACHI_PROCESSES": "1",
    "KABUDACHI_HEARTBEAT_INTERVAL": "0.1",
    "KABUDACHI_HEARTBEAT_TIMEOUT": "1",
    "KABUDACHI_RECONNECT_TIMEOUT": "2",
    "KABUDACHI_CANCEL_GRACE": "0.5",
    "KABUDACHI_DRAIN_WAIT_LIMIT": "2",
}
RUN_SECONDS = 60


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


@pytest.fixture(scope="module")
def installed(tmp_path_factory) -> dict:
    """A fresh virtual environment with the wheel installed by pip, its
    dependencies linked in, and the smoke programs in a directory beside it:
    the interpreter, that directory, the environment's site-packages, and
    the environment variables to run a program with. Nothing from the
    source tree is on its path."""
    root = tmp_path_factory.mktemp("installed")
    programs = root / "programs"
    programs.mkdir()
    for program in PROGRAMS:
        (programs / program.name).write_text(program.read_text())
    environment = {
        "PATH": os.environ["PATH"],
        "HOME": str(root),
        "TMPDIR": os.environ.get("TEST_TMPDIR", str(root)),
        # Task processes import the tasks module from here.
        "PYTHONPATH": str(programs),
        "PYTHONNOUSERSITE": "1",
        **SETTINGS,
    }

    def run(*command) -> str:
        done = subprocess.run(command, env=environment, capture_output=True, text=True)
        assert done.returncode == 0, f"{command} exited with {done.returncode}: {done.stderr}"
        return done.stdout

    # Symlinked: a copy of the interpreter cannot find the shared library it
    # loads from beside the original.
    venv.EnvBuilder(symlinks=True, with_pip=False).create(root / "venv")
    python = root / "venv" / "bin" / "python"
    run(python, "-m", "ensurepip", "--default-pip")
    site = Path(run(python, "-c", "import sysconfig; print(sysconfig.get_path('purelib'))").strip())
    (site / "wheel_test_dependencies.pth").write_text(
        "".join(f"{metadata.distribution(name).locate_file('')}\n" for name in DEPENDENCIES)
    )
    pip = [python, "-m", "pip", "--disable-pip-version-check", "--no-input"]
    run(*pip, "install", "--no-index", "--no-deps", WHEEL)
    # Every Requires-Dist of the wheel is met by what the environment has.
    run(*pip, "check")
    return {"python": python, "programs": programs, "site": site.resolve(), "environment": environment}


def start(installed: dict, mode: str, stderr=subprocess.PIPE, **settings: str) -> subprocess.Popen:
    return subprocess.Popen(
        [installed["python"], "wheel_smoke.py", mode],
        cwd=installed["programs"],
        env={**installed["environment"], **settings},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr,
        text=True,
    )


def flow(installed: dict, **settings: str) -> dict:
    """What `wheel_smoke.py flow` printed, once it has exited cleanly."""
    program = start(installed, "flow", **settings)
    try:
        out, err = program.communicate(timeout=RUN_SECONDS)
    finally:
        program.kill()
    assert program.returncode == 0, err
    return json.loads([line for line in out.splitlines() if line.startswith("{")][-1])


def test_the_installed_wheel_runs_a_flow_on_a_worker_of_its_own(installed):
    shown = flow(installed)

    assert shown["result"] == 41
    # Imported from the installed wheel, extension included.
    assert Path(shown["package"]).resolve().is_relative_to(installed["site"])
    assert Path(shown["native"]).resolve().is_relative_to(installed["site"])


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="manylinux tags describe Linux wheels")
def test_the_linux_wheel_claims_the_platform_its_symbols_allow():
    shown = subprocess.run(
        [sys.executable, "-c", "import sys; from auditwheel.main import main; sys.exit(main())",
         "show", "--json", WHEEL],
        capture_output=True, text=True,
    )
    assert shown.returncode == 0, shown.stderr
    claimed = WHEEL.name.removesuffix(".whl").rsplit("-", 1)[1]
    # The most compatible tag the extension's glibc symbols and linked
    # libraries allow: claiming an older glibc breaks the import there,
    # claiming a newer one refuses systems it runs on.
    assert json.loads(shown.stdout)["overall_tag"] == claimed


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def found_shard(installed: dict, stderr_file: Path) -> tuple[subprocess.Popen, str]:
    """A worker serving a shard it founded alone, and the address it listens
    on. Its stderr goes to `stderr_file`, so an unread pipe never blocks it. A
    port another process takes before the worker binds it makes the worker
    exit before it serves; then another port is tried. A worker that neither
    serves nor exits within RUN_SECONDS is killed and fails the test."""
    for _ in range(3):
        address = f"/ip4/127.0.0.1/tcp/{free_port()}"
        with stderr_file.open("w") as stderr:
            founder = start(installed, "serve", stderr=stderr, KABUDACHI_LISTEN=address)
        serving = []
        reader = threading.Thread(target=lambda: serving.append(founder.stdout.readline()), daemon=True)
        reader.start()
        reader.join(RUN_SECONDS)
        if reader.is_alive():
            founder.kill()
            founder.wait()
            raise AssertionError(
                f"the founding worker did not serve within {RUN_SECONDS}s: {stderr_file.read_text()}"
            )
        if serving[0]:
            return founder, address
        founder.wait()
    raise AssertionError(f"the founding worker never served: {stderr_file.read_text()}")


def test_the_installed_wheel_runs_a_flow_across_a_networked_shard_of_two(installed, tmp_path):
    stderr_file = tmp_path / "founder.stderr"
    founder, address = found_shard(installed, stderr_file)
    try:
        # The second worker starts its program only once it has joined the
        # shard the first one leads; the flow's result comes back to it.
        shown = flow(installed, KABUDACHI_LISTEN="/ip4/127.0.0.1/tcp/0", KABUDACHI_SEEDS=address)
        assert shown["result"] == 41

        founder.stdin.close()
        assert founder.wait(timeout=RUN_SECONDS) == 0, stderr_file.read_text()
    finally:
        founder.kill()
        founder.wait()
