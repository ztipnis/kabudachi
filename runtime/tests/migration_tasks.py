"""Tasks for the migration test. The submitter and the worker run
different builds of the same tasks: `KABUDACHI_TEST_SCHEMA` says which.

- "v1": the submitter. Its orders and invoices are at version 1.
- "v3": runs orders at version 3 through an explicit 1->2 and the default
  2->3, which drops `note` (gone in version 3) and leaves `rush` (new in
  version 3) at its default. Its invoices are at version 2, a bump declared
  incompatible.
"""

import os

import pydantic

import kabudachi

BUILD = os.environ["KABUDACHI_TEST_SCHEMA"]


if BUILD == "v1":

    class Order(pydantic.BaseModel):
        text: str
        note: str = ""

    class Invoice(pydantic.BaseModel):
        total: int

    kabudachi.schema(Order, version=1, family="tests.Order", version_path="$.version")
    kabudachi.schema(Invoice, version=1, family="tests.Invoice", version_path="$.version")

else:

    class Order(pydantic.BaseModel):
        text: str
        rush: bool = False

    class Invoice(pydantic.BaseModel):
        cents: int

    kabudachi.schema(Order, version=3, family="tests.Order", version_path="$.version")
    kabudachi.schema(Invoice, version=2, family="tests.Invoice", version_path="$.version", migration=None)

    @kabudachi.migration(Order, from_version=1)
    def mark_upgraded(order: dict) -> dict:
        return {**order, "text": order["text"] + ">2"}


@kabudachi.task(name="migrations.ship", queue="orders", serializer="pydantic")
def ship(order: Order) -> Order:
    return Order(text=f"{order.text} shipped, rush={getattr(order, 'rush', None)}")


@kabudachi.task(name="migrations.bill", queue="bills", serializer="pydantic")
def bill(invoice: Invoice) -> Invoice:
    return invoice


@kabudachi.task(name="migrations.bill_later", queue="bills", serializer="pydantic", raise_on_failed_migration=True)
def bill_later(invoice: Invoice) -> Invoice:
    return invoice


TASKS = {"ship": ship, "bill": bill, "bill_later": bill_later}
