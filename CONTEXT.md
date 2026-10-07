# Kabudachi

A distributed task runtime whose workers elect one leader per shard; the leader's scheduler alone decides claims. This glossary names the election's driving concepts; the election's own domain terms are defined below, and architecture terms (module, interface, depth, seam, adapter, leverage, locality) follow the codebase-design vocabulary.

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
Who performs a step's authority calls: one that answers later (net's authority client), one that answers at once (the simulator), or one with no authority that answers every call as unavailable.
_Avoid_: caller

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
The roll call, candidacy and win rule, deciding through verdicts that the node turns into outputs.
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

**Leader office**:
What a node holds only while it leads: the roster it leads, the removals it accepted and has not applied, and when it last heard from each worker it has not reported lost. It applies removals before anything reads or changes the configuration it leads. When leadership ends it hands its configuration back to the node's shard standing; a draining leader's departure hands back the announcement of its own removal too.
_Avoid_: leader state

**Lease change**:
A grant or abort-deadline change the lease reports, which the node turns into its grant output or its abort-deadline output.

## Knowing the shard and the authority

**Checked message**:
An election message that decoding has proven well formed (`protocol::checked::decode`); the only kind `WorkerNode::step` accepts, so no accessor on it can fail. It wraps the wire message rather than mirroring it.
_Avoid_: valid message, well-formed message (the type is what proves it)

**Shard standing**:
What a node knows of its shard: its recovery epoch, the highest term it has seen, and the configuration and admissions it follows. It changes only through named transitions, so no site writes one of those fields beside the others.
_Avoid_: shard state

**Epoch ordering**:
How another node's recovery epoch compares with this node's: mine, later, stale, or foreign, which is another lineage's epoch and keeps its number ordering. An epoch that names no lineage compares by number alone.

**Authority standing**:
A node's standing with the coordination authority: its registration and fence timers, forced recovery, reconnect once fenced, and the one reply it awaits. It numbers every call it asks and turns replies into authority verdicts, so the node never builds an authority call.

**Reply token**:
What matches an authority reply to the call that asked it: who issued the call (the node, or net for the calls it asks for itself, such as the bootstrap cascade's and a rejoin's), the call's kind, and a number that issuer never reuses. A node compares whole tokens, so a reply to net's call never answers the call the node awaits.

## The transport

**Transport**:
A worker's `Net`: it sends, publishes and subscribes, queues its node's inputs, and carries the join and claim request handles. It holds no node state, so it never decides who leads.
_Avoid_: messenger (for the whole), network (for one worker's side)

**NewPortTcp**:
The net transport wrapper (`net/src/swarm.rs`) around libp2p's TCP transport that forces `PortUse::New` on every dial, so each dial leaves from a port of its own instead of this node's listen port. Libp2p's port reuse lets two listening peers' simultaneous dials open a TCP simultaneous open, which breaks the connection; a dial with its own source port shares its 4-tuple with no other connection. It gives up port-reuse NAT hole punching, which nothing here uses.
_Avoid_: port-reuse transport

**Peer book**:
What the transport's swarm task alone observes about the peers around it and answers about them: connections, each peer's address of record, redials, the gossip mesh and shard subscribers, and traffic counts. The swarm task tells it what it saw happen; it reads neither the swarm nor the clock. In code it is `Peers` (`net/src/peers.rs`).
_Avoid_: peer table, connection state

**Redial schedule**:
When a lost mesh peer is dialled again: a fast series of attempts followed by a slow, steady one, and the peer is never given up on. The peer book works it out from the observations and the time it is given (`RedialPolicy`).

**Routing refresh**:
When a driven node crawls peer routing again: after its view of its shard changes and settles, and every few suspicion timeouts otherwise. A pure function of what the node shows and the time (`net/src/routing_refresh.rs`).

**Diagnostics**:
One read of the peer book, for tests and logs; nothing a node decides depends on it.
_Avoid_: telemetry, stats

**Caller-named leader**:
The leader a claim is sent to, named by whoever claims, as their node knows it. The transport keeps no leader of its own; a leader that has since lost office answers `NOT_LEADER`.
_Avoid_: current leader (for the transport's view)

**Join client**:
The one module that asks peers who leads and connects to the leader one points at (`join::ask_for_leader`), and resolves the leader's address for the pointer a node hands a joiner (`join::pointer_for`, which the node itself assembles: `WorkerNode::join_response`), for bootstrap and rejoin alike. Whom to ask, and when, is the leader search's decision.
_Avoid_: bootstrap join, rejoin search (each names one caller)

**Leader search**:
Asking addresses who leads, round by round: the seeds, then the workers the authority lists. Bootstrap and the driver run the same search (`net/src/leader_search.rs`: `SearchRounds`, and `DrivenSearch`, which runs `Rejoin` for a node back in `Bootstrapping` and for a stranded node), which decides without I/O what each round asks and what an answer means.

**Driven leader search**:
The leader search a driven node runs, held in one place (`DrivenSearch`): which search the node's state calls for (a rejoin when it is back in `Bootstrapping` or `Joining`, a stranded search when it has sat in `RollCall` or `NoQuorum` for a suspicion timeout), the replies to the calls net asked for itself, and what the node must be told (an epoch read asked, what it found, a pointer to join). It never steps the node; the driver does, at the points of its batch where the search hands something back.
_Avoid_: rejoin (for the stranded search too)

**Authority client**:
A worker's one way to call the coordination authority from net (`AuthorityClient`). The bootstrap cascade and the driver share it, so at most one call of each kind is in flight whoever asked; it performs calls on the blocking pool, answers one that panics as unavailable, and numbers the calls net asks for itself with reply tokens issued by net. It is net's authority performer.

**Claim module**:
The one module for the claim protocol on the network (`net/src/claim.rs`): asking a leader for claims (`Net::request_claim`, `Net::claim_oldest`), and a leader's answer to one. Whether to grant a claim is the scheduler's decision alone.

## The scheduler

**Scheduler door**:
The bindings' one way into the shared scheduler (`bindings/src/door.rs`): the lock around it, the wake-ups after every change to claims, timers and events, and the closed flag. Once the runtime is closed it refuses everything.

**Catch up**:
The scheduler's one time-driven call (`Scheduler::catch_up`), made when its next deadline comes: it forgets every finished task past its retention, and, only while this scheduler leads, makes due delayed tasks pending and expires pending tasks past their expiry.

**Scheduler observer**:
Whoever the scheduler hands each published task record to (the `Observer` trait). The runtime's record store in production, `NoObserver` where nobody watches, a recording spy in tests.

**Waiting room**, **Memory budget**, **Retention**:
The scheduler's private parts for pending tasks, the payload memory of unfinished tasks, and how long finished tasks are kept.

## The Python runtime

**Task table**:
The runtime's record of every task this process submitted, from submission until its handle is settled, and the transition rules for it. It settles each handle once, however the task's runs come and go.

**Loop-hosted work**:
Coroutines the runtime runs on the run's event loop on behalf of other threads (flow and group orchestrations, task callbacks). Each counts as outstanding until it ends, so finishing the run cannot miss one.

**Concurrency places**:
The per-task concurrency slots a running body holds, gives back while it waits for another task, and takes again after.
