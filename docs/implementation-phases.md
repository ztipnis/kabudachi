# Implementation phases and status

This file preserves the phase plan and completion notes from section 27 of the former README, at commit `e1276a3c7fd5dec9f1d20514759e1937048490f7`. It was extracted after the new user-facing README was committed as `71b5d766`.

Status corrected on October 14, 2026: **Phase 6 is incomplete.** Its exit gate is functional completeness, just short of production readiness. The earlier completion report is superseded by the identified functional gaps. The original README explicitly marks Phases 4 and 5 implemented, describes earlier milestones, and lists Phase 6 additions and open work. Historical completion notes below are not a fresh acceptance audit.

## Status overview

| Phase | Milestone | Recorded status |
| --- | --- | --- |
| 0 | Protocol model and simulator | Earlier milestone; original scope retained below |
| 1 | Single-process Python/native runtime | Earlier milestone; later phase notes explicitly credit its local API and composition support |
| 2 | Multi-worker ordinary election | Earlier milestone; original notes describe the completed transition to real peer communication |
| 3 | DHT task dissemination | Earlier milestone; original notes describe task records, replication, reconciliation, and compaction |
| 4 | Redis CoordinationAuthority | Explicitly marked implemented in the original README; two historical test flakes remain recorded |
| 5 | Python subprocess execution | Explicitly marked implemented in the original README; remote-result delivery remained open |
| 6 | Flow/group/map/reduce | **Incomplete: functional-completeness gate not met** |
| 7 | Observability | Planned; no completion recorded in the original phase section |
| 8 | Sharding and convergence | Planned; no completion recorded in the original phase section |

## Phase 0: protocol model and simulator

Original scope, before production networking:

- Define shared messages and task/attempt states.
- Implement the worker election state machine.
- Supply fake time, network, and coordination services.
- Check the core invariants under simulated partitions.

## Phase 1: single-process Python/native runtime

Original scope:

- Task decorators, registry, protobuf serialization, handles, and `.local()`.
- Native runtime startup, local execution, and result certification in a one-node shard.
- Callbacks, automatic retries, cancellation, and ephemeral tasks.
- Coalescing supersession and ordered payload folding.
- Delayed submission, timeouts, and expiry; in-process cancellation is cooperative.
- Binding, flows, groups with failure policies, implicit flows, and maps.
- Memory backpressure: bulk submission pauses at the soft limit; the hard limit refuses submissions unless dropping retained coalescing payloads is explicitly enabled.

The original notes describe a one-worker cluster using the election state machine with quorum one.

Originally deferred: coalescing-chain compaction to Phase 3, observer hooks to Phase 7, reduction and type compatibility checks to Phase 6, migrations, and queue-subscription enforcement to Phase 2. These are historical assignments, not independent confirmation that every deferred item later closed.

## Phase 2: multi-worker ordinary election

Original scope:

- Peer identity and direct leader heartbeats.
- Roll call and voting.
- Worker-pull claim arbitration.
- Keep multi-shard scaling outside this milestone.

The original completion notes describe real peer messaging, multiple coordination adapters, worker bootstrap through seeds and registered peers, admission of joining workers, and tests of the startup window before leadership. Detailed implementation history remains in the original commit rather than this plan.

## Phase 3: DHT task dissemination

Original scope:

- Leader reconciliation, moved from Phase 2 because it rebuilds from shared task records.
- Versioned records containing each task and its execution attempts.
- Content hashes for inputs and certified results.
- Record placement, replication, graceful handoff, and repair as membership changes.
- Worker discovery of pending work through held records, peers, and the leader.
- Compaction of retained coalescing payload chains.

The original notes place compaction execution on workers. Networked execution of that compaction depended on the executor work in Phase 5.

## Phase 4: Redis CoordinationAuthority

Explicitly recorded as implemented:

- Redis and Valkey coordination support, including standalone and cluster deployments, configurable namespaces and databases, and bounded calls.
- Coordination contract tests against standalone Valkey with restricted credentials and a single-node cluster.
- Detection and handling of a flush, restart, failover, and observed outage.
- Networked worker startup using the authority and a reachable external address.
- End-to-end tests for cold bootstrap, flush with a live leader, restart, and quorum loss combined with a flush.
- Leader discovery hints and republishing after lost authority data.
- Shard incarnation identity, ordered recovery epochs, recovery fencing, and shared-service constraints.

The original notes assign the Python-hosted networked worker to Phase 5, and task-to-shard caching, the client shard map, and `ShardLostError` to Phase 8.

Historical open test issues: one Redis integration run hung after its tests passed, and one cluster test's initial call returned `Unavailable`. Neither was reproduced or explained. Port-ownership fixes may have addressed them; if either recurs, investigate the cause rather than treating a rerun as resolution.

## Phase 5: Python subprocess execution

Explicitly recorded as implemented:

- Configurable task process counts and bounded concurrency for synchronous and asynchronous bodies.
- Task and hook discovery in spawned processes, version checks, and startup readiness deadlines.
- Cooperative cancellation followed by escalation, containment of process-exit exceptions, and protection of neighboring runs.
- Reporting lost subprocess runs, applying task delivery semantics, and replacing failed processes with backoff.
- Nested task and composition calls through the worker, and payload compaction in a task process.
- Process initialization, before/after-run hooks, and process recycling.
- Worker drain on the first termination signal and forced shutdown on a second signal.
- A networked execution interface, run abort deadlines, remote cancellation, and task/queue/shard reconnect policies.
- Python-hosted networked workers configured through `listen`, seeds, external address, authority, and shard settings.
- Multi-process tests covering killed task processes, workers, and leaders; CPU-bound tasks; compaction; cancellation; partitions; and cold bootstrap through Valkey.

Historical open item: networked handles raise `RemoteResultUnavailableError`, including inside task bodies. The original roadmap assigned remote client result delivery to Phase 8.

The original notes also describe submission through workers. That behavior conflicts with the clarified core model: callers and workers are separate roles. Preserve it as historical implementation status, not as intended production architecture.

## Phase 6: flow/group/map/reduce

Project status: **incomplete**. Phase 6 requires functional completeness of the task API and distributed execution model; production hardening is a separate gate. The original scope adds these capabilities beyond the local flows, groups, implicit flows, and maps credited to Phase 1:

- Typed compatibility validation.
- Distributed maps across multiple workers.
- Reduction through sequential certified steps, with tree reduction for an associative seedless reducer.
- Ordered group result collection across workers and leader changes.
- Durable flow continuations, including their completion across the network and survival of leader changes.

Historical open item: a networked task returning a flow, group, or bound task fails with `RemoteResultUnavailableError`. The original notes say this requires distributed result collection and durable continuations.

### Functional blockers

The documentation review identified these blockers to Phase 6 completion:

- Remote task results are not delivered to Python handles.
- Caller submission is coupled to worker startup instead of providing separate roles.
- `kabudachi.run(main)` exposes test-only function execution at the production worker boundary; the production entry point should be no-argument `kabudachi.run()`.
- **Missing `.reduce` is a Phase 6 defect.** The public Python task API has no `.reduce`; reduction is required for functional completeness and blocks Phase 6 completion.

Remote-result delivery was assigned to Phase 8 in the old plan. It is now a Phase 6 completion requirement because distributed task execution and composition are functionally incomplete without it. Caller/worker separation and the worker entry-point correction are also Phase 6 requirements, not production-hardening work.

### Functional-completeness exit gate

Phase 6 remains open until end-to-end checks demonstrate all of the following:

- [ ] An independent caller submits tasks without starting a worker and receives remote results and errors.
- [ ] Workers execute tasks as a separate role, with no-argument `kabudachi.run()` as their production entry point.
- [ ] Function-accepting `run(main)` behavior is confined to test-only code and removed from the production API.
- [ ] The public reduction API works with its declared sequential and associative-reducer semantics.
- [ ] Flows, groups, maps, reductions, and implicit continuations complete across workers with the required type validation and result ordering.
- [ ] Distributed composition preserves its required behavior through worker loss and leader changes, including continuation completion and recovery.

These are pending acceptance checks, not claims that every unchecked capability is wholly absent. Passing them establishes functional completeness; it does not establish production readiness.

## Phase 7: observability

Planned hardening scope; metrics and tracing were expected to exist earlier for development:

- Documented metrics and OpenTelemetry propagation.
- Reference dashboards.
- Lifecycle event API and plugin interface, including task-submitting plugins.
- First-party cron scheduler plugin.
- Shared custom metric registry.
- Autoscaling examples.
- Metrics during worker bootstrap.
- Backpressure error attributes describing the hard limit, current usage, and requested space.

Task-submitting plugins belong to the caller role under the clarified system model.

## Phase 8: sharding and convergence

Planned after a trustworthy single shard:

- Load testing at approximately 1,000 workers per shard.
- Client shard map and task shard selection.
- Replacement shards and automatic convergence.
- `ShardLostError` and explicit resubmission strategies.
- Remote client liveness and the resulting orphaned-flow state.
- Task-to-shard cache.
- Disaster recovery storage for unfinished tasks.

The old plan sequences sharding after proving single-shard elections. The scale figure is a test target, not a measured capacity claim. Networked client result delivery was originally listed here; it now belongs to the Phase 6 functional-completeness gate.

## Acceptance criteria retained from the original plan

The old README names criteria for Phases 0–5; it does not provide a completed acceptance checklist for Phases 6–8.

| Phase | Acceptance scope |
| --- | --- |
| 0 | Abort before replacement; one authoritative running coalescing generation; supersede only pending work; rebuild key occupancy after leader change |
| 1 | Preserve retained payloads and fold order; commit continuations with their returning attempt; run the stage after a group once; recover the newest coalescing generation; enforce local soft/hard backpressure |
| 2 | Recover the newest coalescing generation after worker loss; abort before replacement during a partition; retain a test bounding replacement delay |
| 3 | Recover coalescing state after leader loss; preserve and compact retained payloads across replicated records |
| 4 | Cold bootstrap, live-leader flush, restart, and quorum loss plus flush against a real Redis-compatible service; preserve shard incarnation identity |
| 5 | Hard-timeout subprocess termination; heartbeats unaffected by CPU-bound bodies; recovery after killing a subprocess, worker, or leader; partitioned workers stop runs by their deadlines |

These are the original acceptance requirements. Listing them does not assert that this documentation change reran or certified them.

## Production-readiness gates

The original README records these as required before adopting real workloads. No closure evidence was supplied by the phase section:

- Peer and client authentication, encrypted transport, authorized task-record writes, and resource bounds for unfinished records.
- A minimal deployment requirements document covering discovery, changing addresses, peer reachability, graceful termination signals, and health endpoints, without requiring Kubernetes.
- Measured submission-to-start latency and time from worker loss to a replacement claim.
- Mixed-version clusters and rolling upgrades with two worker versions in one shard.

The functional gaps identified in the [README](../README.md#gaps-against-the-system-model) block Phase 6 before these separate production-readiness gates can establish readiness for real workloads.
