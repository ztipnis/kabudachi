"""`import kabudachi` must not need the optional protobuf package: a missing
serializer dependency is reported when the serializer is used, not on import."""

import json
import subprocess
import sys
import textwrap

PROGRAM = textwrap.dedent(
    """
    import importlib.abc
    import json
    import sys

    # The child imports kabudachi from wherever its parent does.
    sys.path[:0] = json.loads(sys.argv[1])

    class BlockProtobuf(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name == "google.protobuf" or name.startswith("google.protobuf."):
                raise ImportError("protobuf is not installed (blocked by the test)")

    sys.meta_path.insert(0, BlockProtobuf())

    import kabudachi
    from kabudachi.errors import SerializerUnavailableError
    from kabudachi.protobuf_serializer import ProtobufSerializer
    from kabudachi.serializers import SerializerRegistry

    assert ProtobufSerializer().available() is False

    try:
        SerializerRegistry.with_defaults().get("protobuf")
    except SerializerUnavailableError:
        pass
    else:
        raise SystemExit("asking for the protobuf serializer should have failed")

    try:
        ProtobufSerializer().encode(None, type(None))
    except SerializerUnavailableError:
        pass
    else:
        raise SystemExit("encoding without protobuf should have failed")

    print("ok")
    """
)


def test_the_package_imports_and_reports_a_missing_protobuf_on_use():
    result = subprocess.run(
        [sys.executable, "-c", PROGRAM, json.dumps(sys.path)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "ok"
