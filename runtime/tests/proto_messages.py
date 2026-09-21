"""Protobuf message classes built at runtime, so the tests need no generated code."""

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

_POOL = descriptor_pool.DescriptorPool()


def make_message_class(name, fields, required=False):
    """A message class called `name` with `fields`, given as (field name, type name).

    With `required`, it is a proto2 message whose fields must all be set.
    """
    types = {
        "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "int64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "bool": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    }
    file_proto = descriptor_pb2.FileDescriptorProto(
        name=f"{name.lower()}.proto",
        package="kabudachi.tests",
        syntax="proto2" if required else "proto3",
    )
    label = (
        descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED
        if required
        else descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    )
    message = file_proto.message_type.add(name=name)
    for number, (field_name, type_name) in enumerate(fields, start=1):
        message.field.add(
            name=field_name,
            number=number,
            type=types[type_name],
            label=label,
        )
    _POOL.Add(file_proto)
    descriptor = _POOL.FindMessageTypeByName(f"kabudachi.tests.{name}")
    return message_factory.GetMessageClass(descriptor)


Greeting = make_message_class("Greeting", [("text", "string"), ("times", "int64")])
Receipt = make_message_class("Receipt", [("ok", "bool")])
Strict = make_message_class("Strict", [("must_be_set", "string")], required=True)
