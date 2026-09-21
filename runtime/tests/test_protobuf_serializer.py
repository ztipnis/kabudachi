"""The protobuf backend: a message, a list of messages, or nothing, to bytes
and back, refusing anything else and any bytes that are not what was asked for."""

import pytest

from kabudachi.errors import SerializationError
from kabudachi.protobuf_serializer import ProtobufSerializer
from proto_messages import Greeting, Receipt, Strict

serializer = ProtobufSerializer()
NoneType = type(None)


def greeting(text="hi", times=1):
    return Greeting(text=text, times=times)


def test_it_is_named_protobuf_and_available_here():
    assert serializer.name == "protobuf"
    assert serializer.available()


def test_a_message_round_trips():
    original = greeting("hello", 3)

    decoded = serializer.decode(serializer.encode(original, Greeting), Greeting)

    assert decoded == original
    assert isinstance(decoded, Greeting)


def test_a_message_is_encoded_as_its_protobuf_bytes():
    original = greeting("hello", 3)

    assert serializer.encode(original, Greeting) == original.SerializeToString()


def test_an_empty_message_round_trips():
    empty = Greeting()

    assert serializer.decode(serializer.encode(empty, Greeting), Greeting) == empty


def test_a_list_of_messages_round_trips_in_order():
    original = [greeting("a", 1), greeting("b", 2), greeting("c", 3)]

    decoded = serializer.decode(serializer.encode(original, list[Greeting]), list[Greeting])

    assert decoded == original


def test_an_empty_list_round_trips():
    assert serializer.decode(serializer.encode([], list[Greeting]), list[Greeting]) == []


def test_a_list_with_empty_messages_keeps_its_length():
    original = [Greeting(), Greeting(), Greeting()]

    decoded = serializer.decode(serializer.encode(original, list[Greeting]), list[Greeting])

    assert len(decoded) == 3


def test_a_long_message_in_a_list_round_trips():
    original = [greeting("x" * 100_000, 1), greeting("y", 2)]

    decoded = serializer.decode(serializer.encode(original, list[Greeting]), list[Greeting])

    assert decoded == original


def test_nothing_round_trips_as_no_bytes():
    assert serializer.encode(None, NoneType) == b""
    assert serializer.decode(b"", NoneType) is None


def test_a_value_of_the_wrong_message_type_is_refused():
    with pytest.raises(SerializationError, match="Greeting"):
        serializer.encode(Receipt(ok=True), Greeting)


def test_a_non_message_value_is_refused():
    with pytest.raises(SerializationError):
        serializer.encode("hello", Greeting)


def test_a_list_holding_the_wrong_type_is_refused():
    with pytest.raises(SerializationError):
        serializer.encode([greeting(), Receipt()], list[Greeting])


def test_a_non_list_for_a_list_type_is_refused():
    with pytest.raises(SerializationError):
        serializer.encode(greeting(), list[Greeting])


def test_a_value_for_a_none_type_must_be_none():
    with pytest.raises(SerializationError):
        serializer.encode(greeting(), NoneType)


@pytest.mark.parametrize("unsupported", [int, str, dict, list[int], list, list[list[Greeting]]])
def test_types_that_are_not_messages_are_refused(unsupported):
    with pytest.raises(SerializationError, match="supports"):
        serializer.encode(1, unsupported)
    with pytest.raises(SerializationError, match="supports"):
        serializer.decode(b"", unsupported)


def test_bytes_that_are_not_the_message_are_refused():
    with pytest.raises(SerializationError):
        serializer.decode(b"\xff\xff\xff\xff\xff", Greeting)


def test_bytes_for_a_none_type_must_be_empty():
    with pytest.raises(SerializationError):
        serializer.decode(b"x", NoneType)


def test_a_truncated_list_is_refused():
    encoded = serializer.encode([greeting("a", 1), greeting("b", 2)], list[Greeting])

    with pytest.raises(SerializationError):
        serializer.decode(encoded[:-1], list[Greeting])


def test_a_list_whose_length_prefix_is_broken_is_refused():
    with pytest.raises(SerializationError):
        serializer.decode(b"\x80", list[Greeting])


def test_a_list_whose_declared_length_runs_past_the_end_is_refused():
    with pytest.raises(SerializationError):
        serializer.decode(b"\x05ab", list[Greeting])


def test_an_item_that_is_cut_short_is_refused_even_if_what_is_left_is_a_valid_message():
    complete = greeting("hi", 0).SerializeToString()

    with pytest.raises(SerializationError, match="past the end"):
        serializer.decode(bytes([len(complete) + 5]) + complete, list[Greeting])


def test_a_single_messages_bytes_are_refused_as_a_list_when_the_framing_does_not_fit():
    encoded = serializer.encode(greeting("hello", 3), Greeting)

    with pytest.raises(SerializationError):
        serializer.decode(encoded, list[Receipt])


def test_a_message_that_cannot_be_serialized_is_a_serialization_error():
    with pytest.raises(SerializationError, match="Strict"):
        serializer.encode(Strict(), Strict)


def test_a_list_item_that_cannot_be_serialized_is_a_serialization_error():
    with pytest.raises(SerializationError, match="Strict"):
        serializer.encode([Strict(must_be_set="ok"), Strict()], list[Strict])


def test_a_complete_message_with_required_fields_round_trips():
    original = Strict(must_be_set="ok")

    assert serializer.decode(serializer.encode(original, Strict), Strict) == original


def test_it_says_which_types_it_supports():
    assert serializer.supports(Greeting)
    assert serializer.supports(list[Greeting])
    assert serializer.supports(NoneType)
    assert not serializer.supports(int)
    assert not serializer.supports(list[int])
    assert not serializer.supports(list)


def test_a_length_that_is_too_long_to_be_valid_is_refused():
    endless_length = b"\xff" * 10 + b"\x01"

    with pytest.raises(SerializationError, match="too long"):
        serializer.decode(endless_length, list[Greeting])


@pytest.mark.parametrize("wrap", [bytearray, memoryview])
def test_bytes_like_payloads_are_accepted(wrap):
    original = greeting("hello", 3)
    encoded = serializer.encode(original, Greeting)

    assert serializer.decode(wrap(encoded), Greeting) == original


@pytest.mark.parametrize("not_bytes", [None, 5, "text"])
def test_a_payload_that_is_not_bytes_is_a_serialization_error(not_bytes):
    with pytest.raises(SerializationError):
        serializer.decode(not_bytes, Greeting)
