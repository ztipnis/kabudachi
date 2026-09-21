"""What a serializer backend is, and how backends are found by name."""

from typing import Any, Protocol

from kabudachi.errors import (
    DuplicateSerializerError,
    SerializerUnavailableError,
    UnknownSerializerError,
)
from kabudachi.protobuf_serializer import ProtobufSerializer

DEFAULT_SERIALIZER = "protobuf"


class Serializer(Protocol):
    """Turns a task's input and output into bytes and back.

    Any object with these members is a backend: register it with a
    `SerializerRegistry` under its `name`.
    """

    name: str

    def available(self) -> bool:
        """Whether this backend can run here, for example whether its library
        is installed."""
        ...

    def supports(self, value_type: Any) -> bool:
        """Whether this backend can encode and decode values of `value_type`,
        so a task that cannot work is refused where it is declared."""
        ...

    def encode(self, value: Any, value_type: Any) -> bytes:
        """`value`, which is a `value_type`, as bytes."""
        ...

    def decode(self, payload: bytes, target_type: Any) -> Any:
        """The `target_type` that `payload` encodes."""
        ...


class SerializerRegistry:
    """Serializer backends by name."""

    def __init__(self) -> None:
        self._serializers: dict[str, Serializer] = {}

    @classmethod
    def with_defaults(cls) -> "SerializerRegistry":
        """A registry holding the backends that ship with kabudachi."""
        registry = cls()
        registry.register(ProtobufSerializer())
        return registry

    def register(self, serializer: Serializer) -> None:
        """Adds a backend, even one that is not available here: that is only
        reported when it is asked for. Raises `TypeError` if it lacks any
        member of `Serializer` or one is of the wrong kind, and
        `DuplicateSerializerError` if the name is taken."""
        missing = [
            member
            for member in ("name", "available", "supports", "encode", "decode")
            if not hasattr(serializer, member)
        ]
        if missing:
            raise TypeError(
                f"{serializer!r} is not a serializer: it has no {', '.join(missing)}"
            )
        if not isinstance(serializer.name, str) or not serializer.name.strip():
            raise TypeError(
                f"{serializer!r} is not a serializer: its name must be a non-empty string"
            )
        not_callable = [
            member
            for member in ("available", "supports", "encode", "decode")
            if not callable(getattr(serializer, member))
        ]
        if not_callable:
            raise TypeError(
                f"{serializer!r} is not a serializer: {', '.join(not_callable)} must be callable"
            )
        if serializer.name in self._serializers:
            raise DuplicateSerializerError(
                f"a serializer named {serializer.name!r} is already registered"
            )
        self._serializers[serializer.name] = serializer

    def get(self, name: str) -> Serializer:
        """The backend registered as `name`. Raises `UnknownSerializerError`
        if there is none and `SerializerUnavailableError` if it cannot run
        here."""
        serializer = self._serializers.get(name)
        if serializer is None:
            known = ", ".join(self.names()) or "none"
            raise UnknownSerializerError(f"no serializer named {name!r}; registered: {known}")
        if not serializer.available():
            raise SerializerUnavailableError(
                f"the serializer {name!r} is registered but cannot run here"
            )
        return serializer

    def find(self, name: str) -> Serializer | None:
        """The backend registered as `name`, whether or not it can run here,
        or `None` if there is none. Unlike `get` it never raises."""
        return self._serializers.get(name)

    def names(self) -> list[str]:
        """The names of the registered serializers, sorted."""
        return sorted(self._serializers)


_process_serializers = SerializerRegistry.with_defaults()


def process_serializers() -> SerializerRegistry:
    """The serializer registry of this process."""
    return _process_serializers


def register_serializer(serializer: Serializer) -> None:
    """Adds a serializer backend to this process, for tasks to select with
    `serializer=<its name>`.

    Every worker of a shard must register the same backend under the same
    name, because a task's data is encoded by one worker and decoded by
    another. Raises `DuplicateSerializerError` if the name is taken.
    """
    _process_serializers.register(serializer)
