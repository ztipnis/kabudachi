"""Protobuf serialization: a message, a list of messages, or nothing.

A message is its protobuf bytes. A list is each message's bytes, in order,
each preceded by its length as a protobuf varint. Nothing (`None`) is no
bytes at all.
"""

import enum
import typing
from typing import Any

from kabudachi.errors import SerializationError, SerializerUnavailableError

try:
    from google.protobuf import message as _protobuf_message
except ImportError:
    _protobuf_message = None

_NONE_TYPE = type(None)
_MAX_VARINT_BYTES = 10


class _Shape(enum.Enum):
    NOTHING = "nothing"
    MESSAGE = "message"
    LIST = "list"


def _is_message_type(candidate: Any) -> bool:
    return isinstance(candidate, type) and issubclass(candidate, _protobuf_message.Message)


def _classify(value_type: Any) -> tuple[_Shape, Any]:
    """What `value_type` is and the message type inside it, or raises."""
    if _protobuf_message is None:
        raise SerializerUnavailableError("the protobuf serializer needs the 'protobuf' package")
    if value_type is _NONE_TYPE:
        return _Shape.NOTHING, None
    if _is_message_type(value_type):
        return _Shape.MESSAGE, value_type
    if typing.get_origin(value_type) is list:
        (item_type,) = typing.get_args(value_type) or (None,)
        if item_type is not None and _is_message_type(item_type):
            return _Shape.LIST, item_type
    raise SerializationError(
        f"the protobuf serializer supports Message, list[Message] and None, not {value_type!r}"
    )


def _require_instance(value: Any, message_type: Any) -> None:
    if not isinstance(value, message_type):
        raise SerializationError(
            f"expected a {message_type.__name__}, got {type(value).__name__}"
        )


def _encode_varint(number: int) -> bytes:
    out = bytearray()
    while True:
        low_bits = number & 0x7F
        number >>= 7
        if number:
            out.append(low_bits | 0x80)
        else:
            out.append(low_bits)
            return bytes(out)


def _read_varint(data: bytes, position: int) -> tuple[int, int]:
    """The varint at `position` and the position after it."""
    number = 0
    for index in range(_MAX_VARINT_BYTES):
        if position + index >= len(data):
            raise SerializationError("the list ends inside a length")
        byte = data[position + index]
        number |= (byte & 0x7F) << (7 * index)
        if not byte & 0x80:
            return number, position + index + 1
    raise SerializationError("a length in the list is too long to be valid")


def _serialize(message: Any) -> bytes:
    try:
        return message.SerializeToString(deterministic=True)
    except _protobuf_message.EncodeError as error:
        raise SerializationError(
            f"cannot serialize this {type(message).__name__}: {error}"
        ) from error


def _parse(message_type: Any, payload: bytes) -> Any:
    try:
        return message_type.FromString(payload)
    except _protobuf_message.DecodeError as error:
        raise SerializationError(f"not a valid {message_type.__name__}: {error}") from error


class ProtobufSerializer:
    """Serializes protobuf messages. Needs the `protobuf` package, which is an
    optional extra: without it `available()` is false."""

    name = "protobuf"

    def available(self) -> bool:
        return _protobuf_message is not None

    def supports(self, value_type: Any) -> bool:
        """Whether `value_type` is a protobuf message class or a `list` of one."""
        try:
            _classify(value_type)
        except SerializationError:
            return False
        return True

    def encode(self, value: Any, value_type: Any) -> bytes:
        """The bytes of `value`, a message or a list of messages of `value_type`.

        Raises `SerializationError` if it is not one, or cannot be encoded (for example a
        required field is unset).
        """
        kind, message_type = _classify(value_type)
        if kind is _Shape.NOTHING:
            if value is not None:
                raise SerializationError(f"expected None, got {type(value).__name__}")
            return b""
        if kind is _Shape.MESSAGE:
            _require_instance(value, message_type)
            return _serialize(value)
        if not isinstance(value, list):
            raise SerializationError(f"expected a list, got {type(value).__name__}")
        framed = bytearray()
        for item in value:
            _require_instance(item, message_type)
            encoded = _serialize(item)
            framed += _encode_varint(len(encoded))
            framed += encoded
        return bytes(framed)

    def decode(self, payload: bytes, target_type: Any) -> Any:
        """The message, or list of messages, of `target_type` that `payload` holds.

        Raises `SerializationError` if the bytes are not a valid one.
        """
        kind, message_type = _classify(target_type)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise SerializationError(f"the payload must be bytes, not {type(payload).__name__}")
        payload = bytes(payload)
        if kind is _Shape.NOTHING:
            if payload:
                raise SerializationError("expected no bytes for None")
            return None
        if kind is _Shape.MESSAGE:
            return _parse(message_type, payload)
        items = []
        position = 0
        while position < len(payload):
            length, position = _read_varint(payload, position)
            end = position + length
            if end > len(payload):
                raise SerializationError("an item in the list runs past the end of the bytes")
            items.append(_parse(message_type, payload[position:end]))
            position = end
        return items
