"""`kabudachi.resume`: get a result back by its id."""

from typing import Any

from kabudachi.client import current_session


async def resume(result_id: str) -> Any:
    """The result of a task or composite submitted earlier, perhaps by a
    process that has since died: its state from the shard's record, its value
    from the result backend.

    Waits while it is still running, or while its value is being mirrored;
    bound the wait with `asyncio.wait_for`. A task that did not succeed raises
    an error of the kind a task run in another process would raise, built
    from its record: a failure has the leader's message, not the exception
    its own process raised.

    Raises:
        ResultUnavailableError: The shard does not know `result_id`, no result
            backend is configured, the backend holds no entry for it (it kept
            nothing, or its worker died before mirroring), or the entry does
            not match the record.
        UnknownTaskError: This process does not declare the task, so it cannot
            decode the value.
        RuntimeNotStartedError: No worker is active and no caller seeds or
            authority are configured. Caller connections initialize lazily.
    """
    return await current_session().resume(result_id)
