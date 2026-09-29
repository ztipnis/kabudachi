# Kabudachi

A distributed task runtime whose workers elect one leader per shard; the leader's scheduler alone decides claims. This glossary names the election's driving concepts; the election's own domain terms follow ADR-0001 (`docs/superpowers/adr/`) and README §10, and architecture terms (module, interface, depth, seam, adapter, leverage, locality) follow the codebase-design vocabulary.

## Driving a node

**Step**:
What one call into a worker's election node produced: the outputs it asks its driver to carry out, and the node's next deadline. A driver carries every step out whole; a test may withhold one.
_Avoid_: result, tick output

**Carry out**:
To apply a step in the one fixed order: its grant to the scheduler first, then its messages through a message sink, then its authority calls to an authority performer, feeding each reply given at once back to the node until a step asks for nothing more.
_Avoid_: apply, execute, handle outputs

**Message sink**:
Where a step's messages go: the real transport, the simulated network, or nowhere for a node with no peers.
_Avoid_: transport (when meaning only the send side), outbox

**Authority performer**:
Who performs a step's authority calls: one that answers later (the network driver), one that answers at once (the simulator), or one with no authority that answers every call as unavailable.
_Avoid_: authority client, caller

**Driver**:
Whatever feeds a node its inputs and carries out its steps: the network driver, the single-process runtime's election loop, or the simulator harness.
_Avoid_: runner, event loop

## Starting a node

**Identity**:
Who a node is: its worker, that worker's process incarnation, its shard, and the timers it runs its election on.

**Entry**:
How a node enters its shard: founding it alone, joining the leader a pointer names, or starting inside a configuration already known. The bootstrap cascade ends by choosing one.
_Avoid_: bootstrap result, join mode

**Founding**:
Entering a shard as its only voter, at a recovery epoch of a lineage the founder drew.
_Avoid_: genesis (for the entry itself; the genesis configuration is what a founder starts with)

**Joining**:
Entering a shard as a pending member of the leader a JOIN answer points at.

## Deciding an election

**Election round**:
The roll call, candidacy and win rule of ADR-0001 decisions 3–8, deciding through verdicts that the node turns into outputs.
_Avoid_: ballot, vote round, roll call round (each names only a part)

**Verdict**:
One decision of the election round: answer, refuse, publish a roll call, stand, ask for votes, grant, certify, win, go NoQuorum, or suspect again.

**View**:
The read-only snapshot of a node that the election round decides against.

## Testing against the authority

**Faulting authority**:
The one test stand-in for the coordination authority that injects faults: unreachability, unavailability, a lost race, a flush, and held calls that model slow, hung and rival-during-call authorities without sleeping.
_Avoid_: slow authority, fake authority

**Step record**:
One node's step as a harness saw it: the input, the outputs, and the node's state, term, epoch, admission and leader afterwards. Both harnesses record one per step (the core simulator from `carry_out`'s observer, the net tests from `run_driver`'s, which hands on the same) and assert the same invariants over the records: `assert_at_most_one_leader` and `first_grant_overlap`. A grant holds from the record that reports it until the earlier of its lease end and that node's next grant report, so a node that stalls or stops holds it no longer than its scheduler would; records must share one timeline, lease ends included.
_Avoid_: batch (for the record), timeline entry

## Holding office

**Lease**:
The leader's grant and every worker's abort deadline, worked out in one module from the acks confirmed, the leader contact a worker's acks prove, and when it fenced itself; it reports each change once as a lease change. Its quorum-contact bookkeeping is private to it.
_Avoid_: grant tracker, lease state

**Office**:
What the lease reads of a node that leads: its id, roster, term, recovery epoch and the end of its recovery fence. A leader that needs a fence and holds none has no office, and so no grant.

**Lease change**:
A grant or abort-deadline change the lease reports, which the node turns into its grant output or its abort-deadline output.

## The transport

**Transport**:
A worker's `Net`: it sends, publishes and subscribes, queues its node's inputs, and carries the join and claim request handles. It holds no node state, so it never decides who leads.
_Avoid_: messenger (for the whole), network (for one worker's side)

**NewPortTcp**:
The net transport wrapper (`net/src/swarm.rs`) around libp2p's TCP transport that forces `PortUse::New` on every dial, so each dial leaves from a port of its own instead of this node's listen port. Libp2p's port reuse lets two listening peers' simultaneous dials open a TCP simultaneous open, which breaks the connection; a dial with its own source port shares its 4-tuple with no other connection. It gives up port-reuse NAT hole punching, which nothing here uses.
_Avoid_: port-reuse transport

**Peers**:
What the transport's swarm task alone knows and changes about the peers around it: connections, each peer's address of record, redials, the gossip mesh and shard subscribers, and traffic counts.
_Avoid_: peer table, connection state

**Diagnostics**:
One read of the transport's peers, for tests and logs; nothing a node decides depends on it.
_Avoid_: telemetry, stats

**Caller-named leader**:
The leader a claim is sent to, named by whoever claims, as their node knows it. The transport keeps no leader of its own; a leader that has since lost office answers `NOT_LEADER`.
_Avoid_: current leader (for the transport's view)

**Join client**:
The one module that asks peers who leads and connects to the leader one points at (`join::ask_for_leader`), asks the workers the authority lists when a node rejoins (`join::find_leader`), and resolves the leader's address for the pointer a node hands a joiner (`join::pointer_for`, which the node itself assembles: `WorkerNode::join_response`), for bootstrap and rejoin alike.
_Avoid_: bootstrap join, rejoin search (each names one caller)
