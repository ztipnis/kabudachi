"""Tasks for wheel_smoke.py, in a module of their own so the task processes
a worker starts can import them."""

from google.protobuf.wrappers_pb2 import Int64Value

import kabudachi


@kabudachi.task(name="smoke.double")
def double(number: Int64Value) -> Int64Value:
    return Int64Value(value=number.value * 2)


@kabudachi.task(name="smoke.add_one")
def add_one(number: Int64Value) -> Int64Value:
    return Int64Value(value=number.value + 1)


double_then_add_one = kabudachi.flow(double, add_one)
