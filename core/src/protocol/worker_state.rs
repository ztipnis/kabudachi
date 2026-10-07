//! Domain `WorkerState` and its transition table, including the
//! step-down, deadline, `NoQuorum` exit, authority-path and orphaning
//! edges. No message carries a worker's state.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerState {
    Bootstrapping,
    Joining,
    Active,
    LeaderSuspect,
    RollCall,
    Candidate,
    LeaderReconciling,
    Leader,
    NoQuorum,
    Draining,
    Fenced,
    Stopped,
}

impl WorkerState {
    /// Every variant, in definition order, for exhaustive iteration in tests.
    pub const ALL: [WorkerState; 12] = [
        WorkerState::Bootstrapping,
        WorkerState::Joining,
        WorkerState::Active,
        WorkerState::LeaderSuspect,
        WorkerState::RollCall,
        WorkerState::Candidate,
        WorkerState::LeaderReconciling,
        WorkerState::Leader,
        WorkerState::NoQuorum,
        WorkerState::Draining,
        WorkerState::Fenced,
        WorkerState::Stopped,
    ];

    /// A terminal state has no legal outgoing transition.
    pub fn is_terminal(self) -> bool {
        self == WorkerState::Stopped
    }

    /// Whether `self -> next` is a legal edge.
    ///
    /// `LeaderSuspect -> Active` lets an ack
    /// from the leader end a suspicion before any roll call starts: a
    /// pending member, which starts none, has no other way back. A node that
    /// holds or contests a term steps down once it sees a later one:
    /// from `RollCall`, `Candidate` or `Leader`, to
    /// `Active` under that term's leader or to `LeaderSuspect` to contest
    /// again. A winner holds office from its win but leads only once it has
    /// reconciled: `LeaderReconciling` does every election duty of a leader
    /// and leaves office by the same edges as `Leader` (to `Active` or
    /// `LeaderSuspect` on a later term, to `NoQuorum` when its lease runs
    /// out, to `Fenced`, to `Draining`), or moves to `Leader` once
    /// reconciled. A roll call or a vote that misses its deadline leaves
    /// `RollCall` or `Candidate` for `LeaderSuspect` too, or, short of a
    /// quorum, `RollCall`
    /// for `NoQuorum`. A `NoQuorum` node
    /// leaves by a roll call of its own (`-> RollCall`) or by an ack from a
    /// leader (`-> Active`).
    ///
    /// With a coordination authority: a
    /// `NoQuorum` node whose authority path swaps the recovery epoch stands
    /// (`-> Candidate`) while it waits out the recovery fence, and goes back
    /// (`Candidate -> NoQuorum`) if the fence is refused for good; one that
    /// finds the epoch missing stops, its shard abandoned (`-> Stopped`). A
    /// node that fails to renew its registration fences itself (`-> Fenced`)
    /// from any state that takes part in elections, and on reconnecting
    /// resumes (`Fenced -> Active`) or, if the epoch is no longer its own,
    /// rejoins (`Fenced -> Bootstrapping`); a `NoQuorum` node whose
    /// authority path finds an epoch it cannot recover from rejoins it the
    /// same way (`NoQuorum -> Bootstrapping`), and so does a `LeaderSuspect` or
    /// `NoQuorum` member whose read of the authority's epoch names another
    /// epoch than its own, rather than stand for election at it
    /// (`LeaderSuspect -> Bootstrapping`). A rejoining node that took a
    /// JOIN pointer waits in `Joining` for the authority to confirm it
    /// (`-> Active`), and goes back (`Joining -> Bootstrapping`) when the
    /// authority holds another epoch.
    pub fn can_transition_to(self, next: WorkerState) -> bool {
        matches!(
            (self, next),
            (WorkerState::Bootstrapping, WorkerState::Joining)
                | (WorkerState::Joining, WorkerState::Active)
                | (WorkerState::Joining, WorkerState::Bootstrapping)
                | (WorkerState::Active, WorkerState::LeaderSuspect)
                | (WorkerState::Active, WorkerState::Draining)
                | (WorkerState::Active, WorkerState::Fenced)
                | (WorkerState::LeaderSuspect, WorkerState::Active)
                | (WorkerState::LeaderSuspect, WorkerState::RollCall)
                | (WorkerState::LeaderSuspect, WorkerState::NoQuorum)
                | (WorkerState::LeaderSuspect, WorkerState::Bootstrapping)
                | (WorkerState::LeaderSuspect, WorkerState::Fenced)
                | (WorkerState::RollCall, WorkerState::Active)
                | (WorkerState::RollCall, WorkerState::LeaderSuspect)
                | (WorkerState::RollCall, WorkerState::Candidate)
                | (WorkerState::RollCall, WorkerState::NoQuorum)
                | (WorkerState::RollCall, WorkerState::Fenced)
                | (WorkerState::Candidate, WorkerState::Active)
                | (WorkerState::Candidate, WorkerState::LeaderSuspect)
                | (WorkerState::Candidate, WorkerState::LeaderReconciling)
                | (WorkerState::Candidate, WorkerState::NoQuorum)
                | (WorkerState::Candidate, WorkerState::Fenced)
                | (WorkerState::LeaderReconciling, WorkerState::Leader)
                | (WorkerState::LeaderReconciling, WorkerState::Active)
                | (WorkerState::LeaderReconciling, WorkerState::LeaderSuspect)
                | (WorkerState::LeaderReconciling, WorkerState::NoQuorum)
                | (WorkerState::LeaderReconciling, WorkerState::Fenced)
                | (WorkerState::LeaderReconciling, WorkerState::Draining)
                | (WorkerState::Leader, WorkerState::Active)
                | (WorkerState::Leader, WorkerState::LeaderSuspect)
                | (WorkerState::Leader, WorkerState::NoQuorum)
                | (WorkerState::Leader, WorkerState::Draining)
                | (WorkerState::Leader, WorkerState::Fenced)
                | (WorkerState::NoQuorum, WorkerState::Active)
                | (WorkerState::NoQuorum, WorkerState::RollCall)
                | (WorkerState::NoQuorum, WorkerState::Candidate)
                | (WorkerState::NoQuorum, WorkerState::Stopped)
                | (WorkerState::NoQuorum, WorkerState::Bootstrapping)
                | (WorkerState::NoQuorum, WorkerState::Fenced)
                | (WorkerState::Fenced, WorkerState::Active)
                | (WorkerState::Fenced, WorkerState::Bootstrapping)
                | (WorkerState::Draining, WorkerState::Stopped)
        )
    }
}
