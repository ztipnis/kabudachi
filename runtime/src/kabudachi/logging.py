"""`ContextFilter`, which puts the current task run on log records."""

import dataclasses
import logging

from kabudachi.run_info import RunInfo, current_context


class ContextFilter(logging.Filter):
    """Adds the running task's `task_id`, `run_id`, `task_name`,
    `attempt`, `shard`, `worker_id`, `trace_id` and `span_id` to each record
    as attributes of those names, for a formatter to use (such as
    `%(task_id)s`). Outside a task body each is an empty string, and
    `attempt` is 0. Add it to a handler, so records from every logger
    are covered."""

    FIELDS = tuple(field.name for field in dataclasses.fields(RunInfo))

    def filter(self, record: logging.LogRecord) -> bool:
        info = current_context()
        for name in self.FIELDS:
            default = 0 if name == "attempt" else ""
            setattr(record, name, default if info is None else getattr(info, name))
        return True
