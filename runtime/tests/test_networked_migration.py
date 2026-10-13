"""Workers at different schema versions share a shard: an older submitter's
task is migrated by a newer worker through explicit and default steps; a
task across a bump declared incompatible is dropped with a warning; the same
task, declared to keep its tasks, waits for another worker and fails once
the block TTL passes. Every failure names the versions."""

from cluster import TIMINGS, Cluster, Worker, eventually

BLOCK_TTL_SECONDS = 3


def test_tasks_are_migrated_dropped_when_incompatible_and_held_until_the_ttl_when_asked(tmp_path):
    environment = {
        **TIMINGS,
        "KABUDACHI_LISTEN": "/ip4/127.0.0.1/tcp/0",
        "KABUDACHI_PROCESSES": "1",
        "KABUDACHI_CONCURRENCY": "1",
        "KABUDACHI_VERSION_BLOCK_TTL": str(BLOCK_TTL_SECONDS),
        "KABUDACHI_TEST_TASKS": "migration_tasks",
        "KABUDACHI_TEST_MARKERS": str(tmp_path),
    }
    workers = []
    try:
        # The submitter claims nothing: it serves a queue no task uses.
        submitter = Worker({**environment, "KABUDACHI_TEST_SCHEMA": "v1", "KABUDACHI_QUEUES": "unused"})
        workers.append(submitter)
        worker = Worker({
            **environment,
            "KABUDACHI_SEEDS": submitter.address,
            "KABUDACHI_TEST_SCHEMA": "v3",
            "KABUDACHI_QUEUES": "orders,bills",
        })
        workers.append(worker)
        eventually("one leader of two voters", lambda: Cluster(workers).led())

        order = submitter.ask(submit="ship", input={"text": "o1", "note": "fragile"})["task_id"]
        dropped = submitter.ask(submit="bill", input={"total": 5})["task_id"]
        held = submitter.ask(submit="bill_later", input={"total": 7})["task_id"]

        # Version 1 -> 2 explicitly, 2 -> 3 by default: `note` dropped, `rush` at its default.
        shipped = submitter.wait_for(lambda line: line.get("settled") == order)
        assert shipped["error"] is None
        assert shipped["value"]["text"] == "o1>2 shipped, rush=False"
        [run] = submitter.record(order)["runs"]
        assert (run["state"], run["worker"]) == ("SUCCEEDED", worker.id)
        assert (run["source_version"], run["execution_version"]) == (1, 3)

        # Across the incompatible bump: dropped at once, with a warning naming the versions.
        gone = submitter.wait_for(lambda line: line.get("settled") == dropped, timeout=BLOCK_TTL_SECONDS)
        assert gone["error"] == "MigrationError"
        record = submitter.record(dropped)
        assert [r["failure_kind"] for r in record["runs"]] == ["MigrationError"]
        assert "version 1 of tests.Invoice cannot be migrated to version 2" in record["blocked_reason"]
        # The worker's last-resort log handler prints a warning as its bare message.
        assert any(
            line.startswith(f"dropping task {dropped}") and "tests.Invoice" in line and "version 1 of tests.Invoice cannot be migrated to version 2" in line
            for line in worker.error_lines()
        ), worker.error_lines()

        # Declared to keep its tasks: left queued for another worker, then failed by the TTL.
        waiting = eventually(
            "the held task to be blocked",
            lambda: (r := submitter.record(held)) and r["blocked_reason"] and not r["finished"] and r,
        )
        assert [r["state"] for r in waiting["runs"]] == ["QUEUED"]
        assert not any(held in line for line in worker.error_lines()), "a held task is not dropped"
        failed = submitter.wait_for(lambda line: line.get("settled") == held, timeout=3 * BLOCK_TTL_SECONDS)
        assert failed["error"] == "MigrationError"
        assert [r["failure_kind"] for r in submitter.record(held)["runs"]] == ["MigrationError"]
    finally:
        for each in workers:
            each.close()
